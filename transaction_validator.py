import json
from datetime import datetime, timedelta
from confluent_kafka import Consumer, Producer

# Kafka setup
broker = "localhost:9092"
input_topic = "darooghe.transactions"
error_topic = "darooghe.error_logs"

consumer = Consumer({
    'bootstrap.servers': broker,
    'group.id': 'darooghe-validator',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([input_topic])

producer = Producer({'bootstrap.servers': broker})

def send_error(error_code, txn_id, original_data):
    error_record = {
        "transaction_id": txn_id,
        "error_code": error_code,
        "data": original_data
    }
    producer.produce(error_topic, value=json.dumps(error_record))
    producer.flush()

def is_valid(txn):
    try:
        # Rule 1: Amount Consistency
        if txn["total_amount"] != txn["amount"] + txn["vat_amount"] + txn["commission_amount"]:
            return "ERR_AMOUNT"
        
        # Rule 2: Time Warping
        txn_time = datetime.fromisoformat(txn["timestamp"].replace("Z", ""))
        now = datetime.utcnow()
        if txn_time > now or txn_time < now - timedelta(days=1):
            return "ERR_TIME"

        # Rule 3: Device Mismatch
        if txn["payment_method"] == "mobile":
            os = txn.get("device_info", {}).get("os", None)
            if os not in ["Android", "iOS"]:
                return "ERR_DEVICE"
        
        return None  # valid
    except Exception as e:
        return "ERR_PARSE"

print("🟢 Kafka consumer is listening...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("⚠️ Consumer error:", msg.error())
        continue
    
    txn_str = msg.value().decode('utf-8')
    txn = json.loads(txn_str)
    error = is_valid(txn)

    if error:
        send_error(error, txn.get("transaction_id", "unknown"), txn)
        print(f"❌ Invalid TXN: {txn['transaction_id']} | Reason: {error}")
    else:
        print(f"✅ Valid TXN: {txn['transaction_id']}")

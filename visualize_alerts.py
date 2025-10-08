import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import timezone

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="kafka_alerts",
    user="kafka_user",
    password="kafka_pass",
    host="localhost",
    port="5432"
)

# Read data
query = "SELECT * FROM alerts"
df = pd.read_sql(query, conn)
conn.close()

# Convert timestamp
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Set timestamp as index
df.set_index('timestamp', inplace=True)

# Ensure output directory exists
output_dir = os.path.expanduser("~/alerts_visualizations")
os.makedirs(output_dir, exist_ok=True)

# Seaborn style
sns.set(style="whitegrid")

# Filter last 24 hours (with timezone-aware timestamps)
now_utc = pd.Timestamp.now(tz=timezone.utc)
last_24h = df[df.index > now_utc - pd.Timedelta(hours=24)]

# 🟢 Plot alerts per 5 minutes (for more detail)
alerts_per_5min = last_24h.resample('5min').size()

plt.figure(figsize=(14, 6))
alerts_per_5min.plot(marker='o', linewidth=1)
plt.title('Alerts Over the Last 24 Hours (5-Minute Resolution)')
plt.xlabel('Time')
plt.ylabel('Number of Alerts')
plt.tight_layout()
plt.savefig(os.path.join(output_dir, 'alerts_last_24_hours_detailed.png'))
plt.clf()

# Risk Level Distribution
sns.countplot(x='risk_level', data=df.reset_index(), palette='Reds')
plt.title('Risk Level Distribution')
plt.xlabel('Risk Level')
plt.ylabel('Count')
plt.tight_layout()
plt.savefig(os.path.join(output_dir, 'risk_level_distribution.png'))
plt.clf()

# Payment Method Distribution
sns.countplot(x='payment_method', data=df.reset_index(), palette='Blues')
plt.title('Payment Method Distribution')
plt.xlabel('Payment Method')
plt.ylabel('Count')
plt.tight_layout()
plt.savefig(os.path.join(output_dir, 'payment_method_distribution.png'))
plt.clf()

# Device OS Distribution
sns.countplot(x='device_os', data=df.reset_index().fillna('Unknown'), palette='Greens')
plt.title('Device OS Distribution')
plt.xlabel('Device OS')
plt.ylabel('Count')
plt.tight_layout()
plt.savefig(os.path.join(output_dir, 'device_os_distribution.png'))
plt.clf()

# High Risk Alerts by Device OS
high_risk = df[df['risk_level'] == 3].reset_index()
sns.countplot(x='device_os', data=high_risk.fillna('Unknown'), palette='Purples')
plt.title('High Risk Alerts by Device OS')
plt.xlabel('Device OS')
plt.ylabel('Count')
plt.tight_layout()
plt.savefig(os.path.join(output_dir, 'high_risk_by_device_os.png'))
plt.clf()

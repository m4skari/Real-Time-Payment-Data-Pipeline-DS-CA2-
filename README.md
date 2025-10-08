# Real-Time-Payment-Data-Pipeline-DS-CA2-
A complete **real-time data engineering project** built with **Apache Kafka**, **Apache Spark (PySpark)**, and **MongoDB**, designed for streaming, analyzing, and visualizing financial transaction data for a simulated payment service provider called *Darooghe*.
# Real-Time Payment Data Pipeline (DS-CA2)

A complete **real-time data engineering project** built with **Apache Kafka**, **Apache Spark (PySpark)**, and **MongoDB**, designed for streaming, analyzing, and visualizing financial transaction data for a simulated payment service provider called *Darooghe*.

---

## 🧩 Project Overview
This project implements an end-to-end data pipeline capable of:
- Ingesting live payment transaction events from multiple sources.
- Validating and cleaning data streams through Kafka consumers.
- Running **batch analytics** with PySpark to evaluate commissions and transaction trends.
- Performing **real-time stream processing** with Spark Streaming for fraud detection.
- Storing processed data efficiently in MongoDB.
- Visualizing insights with interactive Python dashboards.

---

## ⚙️ Core Components

### **1. Data Ingestion Layer**
- Built using **Apache Kafka** (producers/consumers).
- Handles deserialization, validation, and routing of events.
- Invalid transactions are redirected to a separate Kafka topic (`darooghe.error_logs`).

### **2. Batch Processing Layer**
- Implemented with **PySpark**.
- Performs:
  - Commission efficiency reports by merchant category.
  - Customer segmentation and temporal transaction pattern analysis.
- Aggregated datasets stored in **MongoDB** for long-term access.

### **3. Real-Time Processing Layer**
- Powered by **Spark Streaming**.
- Micro-batch analysis with sliding time windows.
- Real-time metrics:
  - Commission ratios and merchant performance.
  - Fraud detection using velocity, geo-distance, and amount anomalies.
- Results written back to dedicated Kafka topics.

### **4. Storage & Visualization**
- **MongoDB** for structured storage with partitioning by date and merchant.
- **Plotly / Matplotlib** dashboards for:
  - Transaction volume trends
  - Merchant rankings
  - User activity metrics

---

## 🧠 Bonus Features
- Advanced temporal fraud detection (outside business hours).
- Dynamic commission recommendation using PySpark UDFs.
- System monitoring with **Prometheus** metrics (Kafka lag, Spark CPU/MEM, JVM GC time).

---

## 🛠️ Technologies Used
| Category | Tools |
|-----------|-------|
| Stream Processing | Apache Kafka, Spark Streaming |
| Batch Analytics | PySpark |
| Storage | MongoDB |
| Visualization | Plotly, Matplotlib |
| Monitoring | Prometheus |
| Language | Python 3.x |

---

## 🚀 How to Run
1. Install Java (OpenJDK), Kafka, and Spark.
2. Start ZooKeeper and Kafka brokers.
3. Run the transaction generator script to produce synthetic events.
4. Launch the Spark Streaming job to process and analyze streams.
5. Connect MongoDB Compass or dashboards to visualize the results.

---

## 📈 Learning Outcomes
This project demonstrates hands-on experience in:
- Distributed data streaming and fault-tolerant architectures.
- Real-time analytics and fraud detection.
- Data modeling and visualization for financial systems.

---

## 👤 Author
**Mohammad Askari**  
📧 [mohammadeaskary@gmail.com](mailto:mohammadeaskary@gmail.com)  
🌐 [GitHub – m4skari](https://github.com/m4skari)

---


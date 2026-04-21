# 🚀 Real-Time Order Intelligence Pipeline

A complete **end-to-end real-time data engineering pipeline** built using Kafka, Spark Structured Streaming, Prometheus, and Grafana.

This project simulates an e-commerce streaming system where orders are processed, validated, monitored, and visualized in real time.

---

## 📌 Project Overview

This pipeline processes streaming order data and ensures:

* ✅ Real-time ingestion using Kafka
* ✅ Data validation and deduplication
* ✅ Clean vs invalid data separation
* ✅ Metrics generation for monitoring
* ✅ Live dashboard for decision-making

---

## 🧱 Architecture

![Architecture](https://raw.githubusercontent.com/akhil442/Real-Time-Order-Intelligence-Pipeline/main/screenshots/architecture.png)

---

## ⚙️ Tech Stack

| Component       | Technology Used                         |
| --------------- | --------------------------------------- |
| Data Producer   | Python                                  |
| Streaming Layer | Apache Kafka                            |
| Processing      | Apache Spark Structured Streaming       |
| Storage         | Parquet (Clean Data), JSON (Quarantine) |
| Monitoring API  | FastAPI                                 |
| Metrics         | Prometheus                              |
| Visualization   | Grafana                                 |
| Orchestration   | Docker                                  |

---

## 🔄 Data Flow

1. **Python Producer**

   * Generates real-time order events
   * Sends data to Kafka topic

2. **Kafka**

   * Acts as streaming buffer
   * Handles high-throughput ingestion

3. **Spark Structured Streaming**

   * Reads Kafka data
   * Performs:

     * Validation
     * Deduplication
     * Aggregations

4. **Data Output**

   * ✅ Clean data → `/tmp/raw_orders_clean`
   * ⚠️ Invalid data → `/tmp/quarantine_orders`

5. **Metrics API**

   * Tracks:

     * Duplicate events
     * Late events
     * Invalid records
     * Bad data %

6. **Prometheus**

   * Scrapes metrics from the FastAPI `/metrics` endpoint

7. **Grafana**

   * Visualizes system health and pipeline metrics

---

## 📊 Grafana Dashboard

![Grafana Dashboard](https://raw.githubusercontent.com/akhil442/Real-Time-Order-Intelligence-Pipeline/main/screenshots/grafana_dashboard.png)

### 🔍 What You Can Understand From the Dashboard

* Total incoming records
* Duplicate events (data issue detection)
* Invalid payment / quantity errors
* Pipeline health status
* Real-time trends

👉 Even a beginner can quickly identify if something is wrong in the pipeline.

---

## 📡 Prometheus Metrics

![Prometheus](https://raw.githubusercontent.com/akhil442/Real-Time-Order-Intelligence-Pipeline/main/screenshots/prometheus_query.png)

Example query:

```
bad_record_count
```

---

## 📦 Data Output (Proof)

### ✅ Clean Data (Valid Records)

![Clean Data](https://raw.githubusercontent.com/akhil442/Real-Time-Order-Intelligence-Pipeline/main/screenshots/clean_data.png)

### ⚠️ Quarantine Data (Invalid Records)

![Quarantine Data](https://raw.githubusercontent.com/akhil442/Real-Time-Order-Intelligence-Pipeline/main/screenshots/quarantine_data.png)

---

## 📤 Producer Stream

![Producer Stream](https://raw.githubusercontent.com/akhil442/Real-Time-Order-Intelligence-Pipeline/main/screenshots/producer_stream.png)

Shows:

* Normal events
* Duplicate events simulation

---

## ▶️ How to Run

### Step 1: Start everything

```bash
docker-compose up --build
```

### Step 2: Run Producer

```bash
python producer/producer.py
```

### Step 3: Access Services

| Service    | URL                   |
| ---------- | --------------------- |
| Kafka      | localhost:9092        |
| Prometheus | http://localhost:9090 |
| Grafana    | http://localhost:3000 |

---

## 🎯 Key Features

* ⚡ Real-time streaming pipeline
* 🛡️ Fault-tolerant architecture
* 🔍 Data quality monitoring
* 📈 Scalable design
* 🔭 End-to-end observability

---

## 💡 Why This Project Matters

In real-world systems:

* 🚨 Bad data can break analytics
* 💸 Duplicate events cause revenue errors
* ⏰ Late data impacts decisions

👉 This pipeline detects and handles all these issues in real time.

---

## 🚀 Future Improvements

* 🔔 Alerting (Grafana Alerts)
* 📋 Kafka Schema Registry
* 🌀 Airflow Orchestration
* ☁️ Cloud Deployment (AWS / Azure)

---

## 👨‍💻 Author

**Akhil Puttabanthi**
MS Data Science — University of New Haven

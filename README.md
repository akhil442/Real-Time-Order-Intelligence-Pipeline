\# 🚀 Real-Time Order Intelligence Pipeline



A complete \*\*end-to-end real-time data engineering pipeline\*\* built using Kafka, Spark Structured Streaming, Prometheus, and Grafana.



This project simulates an e-commerce streaming system where orders are processed, validated, monitored, and visualized in real time.



\---



\## 📌 Project Overview



This pipeline processes streaming order data and ensures:



\- ✅ Real-time ingestion using Kafka

\- ✅ Data validation and deduplication

\- ✅ Clean vs invalid data separation

\- ✅ Metrics generation for monitoring

\- ✅ Live dashboard for decision-making



\---



\## 🧱 Architecture



!\[Architecture](screenshots/architecture.png)



\---



\## ⚙️ Tech Stack



| Component | Technology Used |

|---|---|

| Data Producer | Python |

| Streaming Layer | Apache Kafka |

| Processing | Apache Spark Structured Streaming |

| Storage | Parquet (Clean Data), JSON (Quarantine) |

| Monitoring API | FastAPI |

| Metrics | Prometheus |

| Visualization | Grafana |

| Orchestration | Docker |



\---



\## 🔄 Data Flow



1\. \*\*Python Producer\*\*

&#x20;  - Generates real-time order events

&#x20;  - Sends data to Kafka topic



2\. \*\*Kafka\*\*

&#x20;  - Acts as streaming buffer

&#x20;  - Handles high-throughput ingestion



3\. \*\*Spark Structured Streaming\*\*

&#x20;  - Reads Kafka data

&#x20;  - Performs:

&#x20;    - Validation

&#x20;    - Deduplication

&#x20;    - Aggregations



4\. \*\*Data Output\*\*

&#x20;  - ✅ Clean data → `/tmp/raw\_orders\_clean`

&#x20;  - ⚠️ Invalid data → `/tmp/quarantine\_orders`



5\. \*\*Metrics API\*\*

&#x20;  - Tracks:

&#x20;    - Duplicate events

&#x20;    - Late events

&#x20;    - Invalid records

&#x20;    - Bad data %



6\. \*\*Prometheus\*\*

&#x20;  - Scrapes metrics from the FastAPI `/metrics` endpoint



7\. \*\*Grafana\*\*

&#x20;  - Visualizes system health and pipeline metrics



\---



\## 📊 Grafana Dashboard



!\[Grafana Dashboard](screenshots/grafana\_dashboard.png)



\### 🔍 What You Can Understand From the Dashboard



\- Total incoming records

\- Duplicate events (data issue detection)

\- Invalid payment / quantity errors

\- Pipeline health status

\- Real-time trends



> 👉 Even a beginner can quickly identify if something is wrong in the pipeline.



\---



\## 📡 Prometheus Metrics



!\[Prometheus](screenshots/prometheus\_query.png)



Example query:



```promql

bad\_record\_count

```



\---



\## 📦 Data Output (Proof)



\### ✅ Clean Data (Valid Records)



!\[Clean Data](screenshots/clean\_data.png)



\### ⚠️ Quarantine Data (Invalid Records)



!\[Quarantine Data](screenshots/quarantine\_data.png)



\---



\## 📤 Producer Stream



!\[Producer Stream](screenshots/producer\_stream.png)



Shows:

\- Normal events

\- Duplicate events simulation



\---



\## ▶️ How to Run



\### Step 1: Start everything



```bash

docker-compose up --build

```



\### Step 2: Run Producer



```bash

python producer/producer.py

```



\### Step 3: Access Services



| Service | URL |

|---|---|

| Kafka | `localhost:9092` |

| Prometheus | http://localhost:9090 |

| Grafana | http://localhost:3000 |



\---



\## 🎯 Key Features



\- ⚡ Real-time streaming pipeline

\- 🛡️ Fault-tolerant architecture

\- 🔍 Data quality monitoring

\- 📈 Scalable design

\- 🔭 End-to-end observability



\---



\## 💡 Why This Project Matters



In real-world systems:



\- 🚨 Bad data can break analytics

\- 💸 Duplicate events cause revenue errors

\- ⏰ Late data impacts decisions



> 👉 This pipeline detects and handles all these issues in real time.



\---



\## 🚀 Future Improvements



\- 🔔 Alerting (Grafana Alerts)

\- 📋 Kafka Schema Registry

\- 🌀 Airflow Orchestration

\- ☁️ Cloud Deployment (AWS / Azure)



\---



\## 👨‍💻 Author



\*\*Akhil Puttabanthi\*\*  

MS Data Science — University of New Haven


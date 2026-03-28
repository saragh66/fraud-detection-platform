# 🛡️ Real-Time Payment Fraud Detection Platform

![Python](https://img.shields.io/badge/Python-3.10-blue)
![GCP](https://img.shields.io/badge/Cloud-GCP-orange)
![Terraform](https://img.shields.io/badge/IaC-Terraform-purple)
![Kafka](https://img.shields.io/badge/Stream-Kafka-black)
![Spark](https://img.shields.io/badge/Processing-Spark-red)
![BigQuery](https://img.shields.io/badge/DWH-BigQuery-green)
![dbt](https://img.shields.io/badge/Transform-dbt-yellow)
![Airflow](https://img.shields.io/badge/Orchestration-Airflow-teal)

> End-to-end data engineering project — **DE Zoomcamp 2025**

## 🎯 Problem Statement

Credit card fraud costs billions annually. This platform processes **1.3M+ real transactions** in real-time using a streaming architecture to detect fraudulent patterns across merchants, states, age groups and demographics.

## 🏗️ Architecture
```
CSV Data (1.3M transactions)
        │
        ▼
┌──────────────────┐
│  Apache Kafka    │  ← Real-time streaming producer
│  + Zookeeper     │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Spark Streaming │  ← Stream processing + risk scoring
└────────┬─────────┘
         │
         ▼
┌──────────────────┐     ┌─────────────────┐
│  Google Cloud    │     │   Terraform      │
│  Storage (GCS)   │◄────│   (IaC)         │
└────────┬─────────┘     └─────────────────┘
         │
         ▼
┌──────────────────┐
│    BigQuery      │  ← Partitioned by DAY + Clustered
│  Data Warehouse  │     by category & state
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│   dbt Models     │  ← 5 transformation models
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Airflow DAG     │  ← Daily orchestration pipeline
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│    Streamlit     │  ← Interactive dashboard
│    Dashboard     │
└──────────────────┘
```

## 🛠️ Technologies

| Category | Tool | Purpose |
|----------|------|---------|
| Cloud | Google Cloud Platform | Infrastructure |
| IaC | Terraform | Automated resource provisioning |
| Containerization | Docker | Kafka + Airflow deployment |
| Stream Ingestion | Apache Kafka + Zookeeper | Real-time data streaming |
| Stream Processing | Apache Spark | Data transformation + risk scoring |
| Data Lake | Google Cloud Storage | Raw data storage |
| Data Warehouse | BigQuery | Analytical queries |
| Transformations | dbt | SQL model definitions |
| Orchestration | Apache Airflow | Pipeline scheduling |
| Dashboard | Streamlit + Plotly | Data visualization |
| Language | Python 3.10 | All scripts |

## 📊 Dashboard

Two mandatory tiles as per DE Zoomcamp requirements:

- **Tile 1** — Fraud Rate by Merchant Category *(categorical distribution)*
- **Tile 2** — Daily Fraud Trends Over Time *(temporal distribution)*

Additional visualizations:
- Geographic fraud map by US State
- Top 10 most fraudulent merchants
- Fraud by age group & gender demographics

## 📈 Key Results

| Metric | Value |
|--------|-------|
| Total Transactions | 1,296,675 |
| Fraudulent Cases | 7,506 |
| Fraud Rate | 0.58% |
| Total Volume | $91.2M |
| Categories Monitored | 14 |
| US States Covered | 51 |

## 🗄️ BigQuery Optimization

The `transactions` table is optimized for analytical queries:

- **Partitioned** by `trans_date_trans_time` (DAY partitioning)
  - Reduces data scanned for date-range queries
- **Clustered** by `category`, `state`
  - Optimizes fraud analysis queries by merchant type and geography

## 🔄 dbt Models

| Model | Description | Rows |
|-------|-------------|------|
| `fraud_by_category` | Fraud rate per merchant category | 14 |
| `fraud_by_state` | Fraud distribution by US state | 51 |
| `fraud_by_merchant` | Top fraudulent merchants | 50 |
| `fraud_by_time` | Temporal fraud patterns | 12,877 |
| `fraud_by_age_gender` | Demographics analysis | 18 |

## ⚙️ Airflow DAG

The `fraud_detection_pipeline` DAG runs **@daily** with 4 tasks:
```
check_bigquery_connection
        │
        ▼
validate_fraud_data
        │
        ▼
check_dbt_tables
        │
        ▼
generate_fraud_report
```

## 🚀 How to Reproduce

### Prerequisites
- Python 3.10+
- Docker Desktop
- Terraform
- Java 11+
- GCP Account with billing enabled

### 1. Clone the repository
```bash
git clone https://github.com/Sara-elghayati/-fraud-detection-platform.git
cd -fraud-detection-platform
```

### 2. Setup GCP credentials
```bash
# Place your GCP service account key
cp your-service-account-key.json gcp-key.json
export GOOGLE_APPLICATION_CREDENTIALS=gcp-key.json
```

### 3. Provision infrastructure with Terraform
```bash
cd terraform
terraform init
terraform apply
```
Creates: GCS bucket + BigQuery dataset

### 4. Start Kafka with Docker
```bash
docker-compose up -d
```

### 5. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 6. Stream data to Kafka
```bash
python producer.py
```

### 7. Load data to BigQuery
```bash
python upload_to_bq.py
```

### 8. Run dbt transformations
```bash
cd fraud_dbt
dbt run
```

### 9. Launch Streamlit dashboard
```bash
streamlit run dashboard.py
```

## 📁 Project Structure
```
fraud-detection-platform/
├── 📂 terraform/                    # Infrastructure as Code
│   └── main.tf                      # GCS + BigQuery resources
├── 📂 fraud_dbt/                    # dbt transformation models
│   └── models/fraud/
│       ├── sources.yml
│       ├── fraud_by_category.sql
│       ├── fraud_by_state.sql
│       ├── fraud_by_merchant.sql
│       ├── fraud_by_time.sql
│       └── fraud_by_age_gender.sql
├── 📂 dags/                         # Airflow DAG
│   └── fraud_detection_dag.py
├── producer.py                      # Kafka producer
├── consumer.py                      # Kafka consumer
├── spark_streaming.py               # Spark streaming job
├── upload_to_bq.py                  # BigQuery data loader
├── dashboard.py                     # Streamlit dashboard
├── docker-compose.yml               # Kafka + Zookeeper
├── requirements.txt                 # Python dependencies
└── README.md
```

## 👤 Author

**Sara El Ghayati**
- GitHub: [@Sara-elghayati](https://github.com/Sara-elghayati)
- Project: DE Zoomcamp 2025 Capstone

---
*Built with ❤️ as part of the Data Engineering Zoomcamp 2025*
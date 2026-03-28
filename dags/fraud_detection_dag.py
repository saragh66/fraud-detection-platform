from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'fraud-detection',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fraud_detection_pipeline',
    default_args=default_args,
    description='End-to-end fraud detection pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['fraud', 'bigquery', 'dbt'],
)

def check_bigquery_connection(**kwargs):
    from google.cloud import bigquery
    import os
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/gcp-key.json"
    client = bigquery.Client(project="fraud-detection-project-491323")
    result = client.query("SELECT COUNT(*) as total FROM `fraud-detection-project-491323.fraud_detection.transactions`").to_dataframe()
    total = result['total'][0]
    print(f"✅ BigQuery connection OK — {total:,} rows found")
    if total == 0:
        raise ValueError("No data found in BigQuery!")
    return "connection_ok"

def validate_fraud_data(**kwargs):
    from google.cloud import bigquery
    import os
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/gcp-key.json"
    client = bigquery.Client(project="fraud-detection-project-491323")
    result = client.query("""
        SELECT
            COUNT(*) as total_rows,
            SUM(is_fraud) as total_frauds,
            ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate,
            COUNT(DISTINCT category) as categories,
            COUNT(DISTINCT state) as states
        FROM `fraud-detection-project-491323.fraud_detection.transactions`
    """).to_dataframe()
    print(f"✅ Data Validation Report:")
    print(f"   Total rows     : {result['total_rows'][0]:,}")
    print(f"   Total frauds   : {result['total_frauds'][0]:,}")
    print(f"   Fraud rate     : {result['fraud_rate'][0]}%")
    print(f"   Categories     : {result['categories'][0]}")
    print(f"   States         : {result['states'][0]}")
    return "validation_ok"

def check_dbt_tables(**kwargs):
    from google.cloud import bigquery
    import os
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/gcp-key.json"
    client = bigquery.Client(project="fraud-detection-project-491323")
    tables = [
        "fraud_by_category",
        "fraud_by_state",
        "fraud_by_merchant",
        "fraud_by_time",
        "fraud_by_age_gender"
    ]
    for table in tables:
        result = client.query(f"""
            SELECT COUNT(*) as total
            FROM `fraud-detection-project-491323.fraud_detection.{table}`
        """).to_dataframe()
        print(f"✅ {table}: {result['total'][0]} rows")
    return "dbt_tables_ok"

def generate_report(**kwargs):
    from google.cloud import bigquery
    import os
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/gcp-key.json"
    client = bigquery.Client(project="fraud-detection-project-491323")
    result = client.query("""
        SELECT category, fraud_rate, total_frauds
        FROM `fraud-detection-project-491323.fraud_detection.fraud_by_category`
        ORDER BY fraud_rate DESC
        LIMIT 3
    """).to_dataframe()
    print("✅ Top 3 Most Fraudulent Categories:")
    for _, row in result.iterrows():
        print(f"   {row['category']}: {row['fraud_rate']}% ({row['total_frauds']} frauds)")
    return "report_ok"

t1 = PythonOperator(
    task_id='check_bigquery_connection',
    python_callable=check_bigquery_connection,
    dag=dag,
)

t2 = PythonOperator(
    task_id='validate_fraud_data',
    python_callable=validate_fraud_data,
    dag=dag,
)

t3 = PythonOperator(
    task_id='check_dbt_tables',
    python_callable=check_dbt_tables,
    dag=dag,
)

t4 = PythonOperator(
    task_id='generate_fraud_report',
    python_callable=generate_report,
    dag=dag,
)

t1 >> t2 >> t3 >> t4
import pandas as pd
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/fraud-detection-project/gcp-key.json"

client = bigquery.Client(project="fraud-detection-project-491323")

print("Chargement du dataset...")
df = pd.read_csv("data/fraudTrain.csv")

# Convertit la date
df["trans_date_trans_time"] = pd.to_datetime(df["trans_date_trans_time"])

# Convertit cc_num en string
df["cc_num"] = df["cc_num"].astype(str)
df["trans_num"] = df["trans_num"].astype(str)

# Ajoute risk_level
df["risk_level"] = df["amt"].apply(
    lambda x: "HIGH" if x > 1000 else ("MEDIUM" if x > 500 else "LOW")
)

print(f"Lignes : {len(df)}")

table_id = "fraud-detection-project-491323.fraud_detection.transactions"

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="trans_date_trans_time",
    ),
    clustering_fields=["category", "state"],
)

print("Upload vers BigQuery avec partition + cluster...")
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

print(f"✅ {len(df)} lignes chargées !")
print("✅ Table partitionnée par JOUR (trans_date_trans_time)")
print("✅ Table clusterisée par CATEGORY + STATE")
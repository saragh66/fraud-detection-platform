from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, LongType
)
import os

# ── Auth ──────────────────────────────────────────────────────────────────────
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/fraud-detection-project/gcp-key.json"

# ── Paths ─────────────────────────────────────────────────────────────────────
# file:/// prefix is required on Windows so Spark does not mistake
# the drive letter "C" for a Hadoop filesystem scheme
GCS_CONNECTOR_JAR = "file:///C:/fraud-detection-project/jars/gcs-connector-hadoop3-2.2.22-shaded.jar"
GCS_KEY           = "C:/fraud-detection-project/gcp-key.json"
GCS_OUTPUT        = "gs://fraud-detection-bucket-491323/transactions/"
GCS_CHECKPOINT    = "gs://fraud-detection-bucket-491323/checkpoints/"
KAFKA_BROKER      = "localhost:9092"
KAFKA_TOPIC       = "fraud-transactions"

# ── SparkSession ───────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("FraudDetection")

    # Kafka connector – resolved from Maven (no Guava conflict)
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )

    # GCS shaded JAR – loaded directly via file:/// to avoid the
    # "No FileSystem for scheme C" error on Windows
    .config("spark.jars", GCS_CONNECTOR_JAR)

    # Tell Hadoop which class handles gs:// paths
    .config(
        "spark.hadoop.fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    )
    .config(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    )

    # Service-account auth
    .config(
        "spark.hadoop.google.cloud.auth.service.account.enable",
        "true"
    )
    .config(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        GCS_KEY
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ── Schema ─────────────────────────────────────────────────────────────────────
schema = StructType([
    StructField("trans_date_trans_time", StringType()),
    StructField("cc_num",                StringType()),
    StructField("merchant",              StringType()),
    StructField("category",              StringType()),
    StructField("amt",                   DoubleType()),
    StructField("first",                 StringType()),
    StructField("last",                  StringType()),
    StructField("gender",                StringType()),
    StructField("city",                  StringType()),
    StructField("state",                 StringType()),
    StructField("lat",                   DoubleType()),
    StructField("long",                  DoubleType()),
    StructField("city_pop",              IntegerType()),
    StructField("job",                   StringType()),
    StructField("dob",                   StringType()),
    StructField("trans_num",             StringType()),
    StructField("unix_time",             LongType()),
    StructField("merch_lat",             DoubleType()),
    StructField("merch_long",            DoubleType()),
    StructField("is_fraud",              IntegerType()),
])

# ── Kafka source ───────────────────────────────────────────────────────────────
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# ── Transform ──────────────────────────────────────────────────────────────────
transactions = (
    df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    )
    .select("data.*")
    .withColumn(
        "risk_level",
        when(col("amt") > 1000, "HIGH")
        .when(col("amt") > 500,  "MEDIUM")
        .otherwise("LOW")
    )
)

# ── GCS sink ───────────────────────────────────────────────────────────────────
query = (
    transactions.writeStream
    .format("parquet")
    .option("path",               GCS_OUTPUT)
    .option("checkpointLocation", GCS_CHECKPOINT)
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)

print("✅ Spark Streaming démarré — écriture vers GCS !")
query.awaitTermination()
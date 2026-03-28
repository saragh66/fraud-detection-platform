from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("trans_date_trans_time", StringType()),
    StructField("cc_num", StringType()),
    StructField("merchant", StringType()),
    StructField("category", StringType()),
    StructField("amt", DoubleType()),
    StructField("first", StringType()),
    StructField("last", StringType()),
    StructField("gender", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("lat", DoubleType()),
    StructField("long", DoubleType()),
    StructField("city_pop", IntegerType()),
    StructField("job", StringType()),
    StructField("dob", StringType()),
    StructField("trans_num", StringType()),
    StructField("unix_time", LongType()),
    StructField("merch_lat", DoubleType()),
    StructField("merch_long", DoubleType()),
    StructField("is_fraud", IntegerType())
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "fraud-transactions") \
    .option("startingOffsets", "earliest") \
    .load()

transactions = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

transactions = transactions.withColumn(
    "risk_level",
    when(col("amt") > 1000, "HIGH")
    .when(col("amt") > 500, "MEDIUM")
    .otherwise("LOW")
)

# Sauvegarde locale
query = transactions \
    .writeStream \
    .format("parquet") \
    .option("path", "C:/fraud-detection-project/output/transactions/") \
    .option("checkpointLocation", "C:/fraud-detection-project/output/checkpoints/") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

print("✅ Spark Streaming démarré !")
query.awaitTermination()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import *

# Skema payload sesuai output dari Debezium
schema = StructType([
    StructField("ID", StructType([
        StructField("scale", IntegerType(), False),
        StructField("value", BinaryType(), False)
    ]), False),
    StructField("FIRST_NAME", StringType(), True),
    StructField("LAST_NAME", StringType(), True),
    StructField("EMAIL", StringType(), True),
    StructField("PHONE", StringType(), True),
    StructField("CREATED_AT", LongType(), True),
    StructField("__op", StringType(), True),
    StructField("__table", StringType(), True),
    StructField("__source_ts_ms", LongType(), True),
    StructField("__deleted", StringType(), True)
])

# Buat SparkSession dengan konfigurasi YARN Cluster
spark = SparkSession.builder \
    .appName("CDC to Kudu") \
    .master("yarn") \
    .config("spark.submit.deployMode", "cluster") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.kudu.master", "kudu-master:7051") \
    .getOrCreate()

# Read dari Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "customers_topic") \
    .option("startingOffsets", "latest") \
    .load()

# Ambil hanya value dan decode
json_df = df.selectExpr("CAST(value AS STRING) as json")
parsed_df = json_df.select(from_json("json", schema).alias("data")).select("data.*")

# Konversi ID ke integer dari base64
parsed_df = parsed_df.withColumn("ID", expr("cast(binary_to_long(ID.value) as long)"))

# Filter hanya insert dan update
filtered_df = parsed_df.filter(col("__op").isin("c", "u", "r"))

# Tulis ke Kudu
kudu_target_table = "impala::default.customers"
query = filtered_df.writeStream \
    .format("kudu") \
    .option("kudu.master", "kudu-master:7051") \
    .option("kudu.table", kudu_target_table) \
    .option("checkpointLocation", "/user/spark/checkpoints/customers") \
    .outputMode("append") \
    .start()

query.awaitTermination()

# Fungsi UDF konversi binary ke long
from pyspark.sql.functions import udf
import base64

def binary_to_long(b):
    if b:
        return int.from_bytes(b, byteorder='big')
    return None

spark.udf.register("binary_to_long", binary_to_long, LongType())


spark = SparkSession.builder \
    .appName("CDC_Kafka_Console") \
    .config("spark.submit.deployMode", "cluster") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()
    
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()
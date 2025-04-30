from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BinaryType

# =========================
# Manual BigInteger decoder
# =========================
def decode_bigint(value_bytes):
    if value_bytes is None:
        return None
    try:
        value = 0
        for b in bytearray(value_bytes):
            value = (value << 8) + (b if isinstance(b, int) else ord(b))
        return value
    except Exception:
        return None

decode_bigint_udf = udf(decode_bigint, LongType())

# =========================
# SparkSession with Kudu JAR
# =========================
spark = SparkSession.builder \
    .appName("CDC_Kafka_to_Kudu") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================
# Define Debezium payload schema
# =========================
payload_schema = StructType([
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
    StructField("__deleted", StringType(), True),
])

# =========================
# Kafka Config
# =========================
kafka_bootstrap_servers = "192.168.10.140:9092"
topic_name = "server11.INVENTORY.CUSTOMERS"

# =========================
# Read Kafka Stream
# =========================
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")

df_parsed = df_json.select(from_json(col("json_str"), StructType([
    StructField("payload", payload_schema)
])).alias("data")).select("data.payload.*")

df_decoded = df_parsed.withColumn("id_numeric", decode_bigint_udf(col("ID.value")))\

# =========================
# Stream Write to Kudu (Direct Write)
# =========================
kudu_master = "192.168.10.141:7051,192.168.10.142:7051,192.168.10.145:7051"
kudu_table = "impala::inventory.customers"

def write_to_kudu(batch_df, batch_id):
    if not batch_df.rdd.isEmpty():
        batch_df.select(
            col("id_numeric").alias("id_numeric"),
            col("FIRST_NAME").alias("first_name"),
            col("LAST_NAME").alias("last_name"),
            col("EMAIL").alias("email"),
            col("PHONE").alias("phone"),
            col("CREATED_AT").alias("created_at"),
            col("__op").alias("__op"),
            col("__table").alias("__table"),
            col("__source_ts_ms").alias("__source_ts_ms"),
            col("__deleted").alias("__deleted")
        ).write \
            .format("kudu") \
            .option("kudu.master", kudu_master) \
            .option("kudu.table", kudu_table) \
            .mode("append") \
            .save()


query = df_decoded.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_kudu) \
    .option("checkpointLocation", "hdfs:///user/aditya/cloudera/checkpoints/cdc_to_kudu2")\
    .start()

query.awaitTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BinaryType
from pyspark.sql import DataFrame

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

# Build SparkSession
spark = SparkSession.builder \
    .appName("CDC_Kafka_Kudu_Upsert") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.kudu.master", "192.168.10.141:7051") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Debezium schema
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

# Kafka stream
kafka_bootstrap_servers = "192.168.10.140:9092"
topic_name = "server8.INVENTORY.CUSTOMERS"

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "latest") \
    .load()

df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")

df_parsed = df_json.select(from_json(col("json_str"), StructType([
    StructField("payload", payload_schema)
])).alias("data")).select("data.payload.*")

df_decoded = df_parsed.withColumn("ID_numeric", decode_bigint_udf(col("ID.value")))

# =========================
# Sink function
# =========================
def upsert_to_kudu(batch_df: DataFrame, batch_id: int):
    if batch_df.count() == 0:
        print(f"[{batch_id}] No data to write to Kudu.")
        return

    # Filter deleted records if needed
    upserts = batch_df.filter(col("__deleted") != "true") \
        .select(
            col("ID_numeric").alias("ID"),
            "FIRST_NAME", "LAST_NAME", "EMAIL", "PHONE", "CREATED_AT"
        )

    # Logging (console)
    print(f"[{batch_id}] Writing {upserts.count()} records to Kudu.")

    # Upsert ke Kudu
    upserts.write \
        .format("kudu") \
        .option("kudu.table", "impala::default.customers") \
        .option("kudu.master", "192.168.10.141:7051") \
        .mode("append") \
        .save()

# =========================
# Trigger write stream
# =========================
query = df_decoded.writeStream \
    .foreachBatch(upsert_to_kudu) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoints/cdc_kudu") \
    .start()

query.awaitTermination()

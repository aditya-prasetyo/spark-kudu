from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BinaryType

# =========================
# Manual BigInteger decoder (compatible with Python 2 & 3)
# =========================
def decode_bigint(value_bytes):
    if value_bytes is None:
        return None
    try:
        value = 0
        for b in bytearray(value_bytes):
            value = (value << 8) + (b if isinstance(b, int) else ord(b))  # for Python 2/3
        return value
    except Exception:
        return None 

from pyspark.sql.types import LongType
decode_bigint_udf = udf(decode_bigint, LongType())

# =========================
# SparkSession with YARN cluster config
# =========================
spark = SparkSession.builder \
    .appName("CDC_Kafka_Console") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================
# Define schema matching Debezium payload
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
# Kafka Configs
# =========================
kafka_bootstrap_servers = "192.168.10.140:9092"
topic_name = "server10.INVENTORY.CUSTOMERS"

# =========================
# Read stream from Kafka
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

# Decode ID.value from binary to long
df_decoded = df_parsed.withColumn("ID_numeric", decode_bigint_udf(col("ID.value")))

# =========================
# Output to console
# =========================
query = df_decoded.select(
    "ID_numeric", "FIRST_NAME", "LAST_NAME", "EMAIL", "PHONE", "CREATED_AT", "__op", "__deleted"
).writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

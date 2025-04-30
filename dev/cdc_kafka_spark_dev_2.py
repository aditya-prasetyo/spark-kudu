from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import LongType
import base64

# =============================
# UDF Decode Base64 BigInteger
# =============================
def decode_base64_bigint(value_base64):
    if value_base64 is None:
        return None
    try:
        value_bytes = base64.b64decode(value_base64)
        value = 0
        for b in bytearray(value_bytes):
            value = (value << 8) + (b if isinstance(b, int) else ord(b))  # Python 2/3 compatible
        return value
    except Exception as e:
        return None

decode_bigint_udf = udf(decode_base64_bigint, LongType())

# =============================
# Spark Session
# =============================
spark = SparkSession.builder \
    .appName("CDC_Kafka_Console_Dynamic_Debezium") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =============================
# Kafka Config
# =============================
kafka_bootstrap = "192.168.10.140:9092"
topic = "server9.INVENTORY.CUSTOMERS"
offset_mode = "earliest"  # gunakan 'latest' untuk production

# =============================
# Read Raw Kafka Stream
# =============================
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", topic) \
    .option("startingOffsets", offset_mode) \
    .load()

df_json = df_kafka.selectExpr("CAST(value AS STRING) as json_str")

# =============================
# Ambil schema sample (static 1x collect untuk dev)
# =============================
sample_df = df_json.limit(100)
sample_rows = [row["json_str"] for row in sample_df.collect()]
rdd_sample = spark.sparkContext.parallelize(sample_rows)
schema = spark.read.json(rdd_sample).schema

print("\n=== Inferred Schema ===")
schema.printTreeString()

# =============================
# Parsing & Navigasi ke payload.after
# =============================
df_parsed = df_json.select(from_json(col("json_str"), schema).alias("data"))
df_after = df_parsed.select("data.payload.after.*")

# =============================
# Decode ID.value
# =============================
if "ID" in df_parsed.select("data.payload.after").schema["after"].dataType:
    df_with_id = df_after.withColumn("ID_numeric", decode_bigint_udf(col("ID.value")))
else:
    df_with_id = df_after

# =============================
# Console Output
# =============================
df_with_id.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start() \
    .awaitTermination()

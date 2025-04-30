from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json
from pyspark.sql import DataFrame
import json

# ===============================
# Build SparkSession
# ===============================
spark = SparkSession.builder \
    .appName("CDC_Kafka_Kudu_SourceDriven") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.kudu.master", "192.168.10.141:7051") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ===============================
# Kafka Config
# ===============================
kafka_bootstrap_servers = "192.168.10.140:9092"
topic = "server8.INVENTORY.CUSTOMERS"

# ===============================
# Read Kafka Stream (JSON)
# ===============================
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load()

df_value = df_raw.selectExpr("CAST(value AS STRING) as json_str")

# ===============================
# Extract dynamic schema from sample
# ===============================
sample_row = df_value.select("json_str").limit(1).collect()
if sample_row:
    sample_json = sample_row[0]["json_str"]
    inferred_schema = schema_of_json(sample_json)
else:
    raise Exception("Tidak ada sample JSON untuk infer schema.")

df_parsed = df_value.select(from_json(col("json_str"), inferred_schema).alias("data")).select("data.*")

# ===============================
# Sink to Kudu
# ===============================
def sink_to_kudu(batch_df: DataFrame, batch_id: int):
    try:
        if batch_df.rdd.isEmpty():
            print(f"[Batch {batch_id}] Kosong, skip.")
            return

        # Cek apakah schema cocok dengan schema Kudu
        kudu_table = "impala::default.customers"
        kudu_master = "192.168.10.141:7051"

        kudu_df = spark.read \
            .format("kudu") \
            .option("kudu.master", kudu_master) \
            .option("kudu.table", kudu_table) \
            .load()

        spark_schema = set((f.name, f.dataType) for f in batch_df.schema.fields)
        kudu_schema = set((f.name, f.dataType) for f in kudu_df.schema.fields)

        if spark_schema != kudu_schema:
            print(f"[Batch {batch_id}] ⚠️ WARNING: Schema mismatch between source and sink.")
            print(f"Source fields: {[f.name for f in batch_df.schema.fields]}")
            print(f"Kudu fields:   {[f.name for f in kudu_df.schema.fields]}")
            raise Exception("Schema mismatch - sink aborted to avoid data loss.")

        # Write ke Kudu
        batch_df.write \
            .format("kudu") \
            .option("kudu.master", kudu_master) \
            .option("kudu.table", kudu_table) \
            .mode("append") \
            .save()

        print(f"[Batch {batch_id}] ✅ Success write to Kudu.")

    except Exception as e:
        print(f"[Batch {batch_id}] ❌ ERROR saat sink: {e}")
        raise e  # penting agar offset tidak disimpan

# ===============================
# Stream
# ===============================
query = df_parsed.writeStream \
    .foreachBatch(sink_to_kudu) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoints/cdc_kudu_source_driven") \
    .start()

query.awaitTermination()

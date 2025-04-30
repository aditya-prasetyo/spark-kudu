from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

# ===============================
# Inisialisasi SparkSession
# ===============================
spark = SparkSession.builder \
    .appName("Create_Kudu_Customers_Table") \
    .config("kudu.master", "192.168.10.141:7051,192.168.10.142:7051,192.168.10.145:7051") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ===============================
# Skema Data Debezium Kafka
# ===============================
schema = StructType([
    StructField("ID_numeric", LongType(), False),
    StructField("FIRST_NAME", StringType(), True),
    StructField("LAST_NAME", StringType(), True),
    StructField("EMAIL", StringType(), True),
    StructField("PHONE", StringType(), True),
    StructField("CREATED_AT", LongType(), True),
    StructField("__op", StringType(), True),
    StructField("__deleted", StringType(), True),
])

# ===============================
# Buat DataFrame kosong
# ===============================
empty_df = spark.createDataFrame([], schema)

# ===============================
# Buat Table Kudu
# ===============================
kudu_table = "impala::inventory.customers"

empty_df.write \
    .format("kudu") \
    .option("kudu.master", "192.168.10.141:7051,192.168.10.142:7051,192.168.10.145:7051") \
    .option("kudu.table", kudu_table) \
    .option("kudu.key_columns", "ID_numeric") \
    .option("kudu.hash_columns", "ID_numeric") \
    .option("kudu.num_hash_partitions", "3") \
    .mode("overwrite") \
    .save()

print(f"✅ Table {kudu_table} berhasil dibuat di Kudu.")

spark.stop()

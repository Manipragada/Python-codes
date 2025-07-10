import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from airflow.models import Variable

# -----------------------------
# 🛠️ Logging Setup
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -----------------------------
# 🚀 SparkSession Initialization
# -----------------------------
spark = SparkSession.builder \
    .appName("S3 Multi-DF RDD Pipeline with Logging") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# -----------------------------
# 📂 Load S3 Paths from Airflow Variables
# -----------------------------
source_path_1 = Variable.get("s3_source_path_1")
source_path_2 = Variable.get("s3_source_path_2")
target_path   = Variable.get("s3_target_path")

logger.info(f"✅ Source Path 1: {source_path_1}")
logger.info(f"✅ Source Path 2: {source_path_2}")
logger.info(f"✅ Target Path  : {target_path}")

# -----------------------------
# 🧾 Define Schemas
# -----------------------------
schema1 = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("value", IntegerType(), True)
])

schema2 = StructType([
    StructField("id", IntegerType(), True),
    StructField("category", StringType(), True)
])

# -----------------------------
# 📘 Read DataFrames from S3
# -----------------------------
df1 = spark.read.option("header", "true").schema(schema1).csv(source_path_1)
logger.info("📘 DataFrame 1 Schema:")
df1.printSchema()
logger.info("📘 DataFrame 1 Preview:")
df1.show(5)

df2 = spark.read.option("header", "true").schema(schema2).csv(source_path_2)
logger.info("📘 DataFrame 2 Schema:")
df2.printSchema()
logger.info("📘 DataFrame 2 Preview:")
df2.show(5)

# -----------------------------
# 🔗 Join DataFrames
# -----------------------------
df_joined = df1.join(df2, on="id", how="inner")
logger.info("🔗 Joined DataFrame Schema:")
df_joined.printSchema()
logger.info("🔗 Joined DataFrame Preview:")
df_joined.show(5)

# -----------------------------
# 🔁 DataFrame to RDD Processing
# -----------------------------
rdd_joined = df_joined.rdd.map(
    lambda row: (row.id, row.name.upper(), row.category.lower(), row.value * 2)
)

logger.info("🌀 Sample RDD Output:")
for rec in rdd_joined.take(5):
    logger.info(rec)

# -----------------------------
# 🔄 RDD back to DataFrame
# -----------------------------
final_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name_upper", StringType(), True),
    StructField("category_lower", StringType(), True),
    StructField("value_scaled", IntegerType(), True)
])

df_final = spark.createDataFrame(rdd_joined, schema=final_schema)
logger.info("🧾 Final DataFrame Schema:")
df_final.printSchema()
logger.info("🧾 Final DataFrame Preview:")
df_final.show(5)

# -----------------------------
# 🔍 Filter DataFrame
# -----------------------------
df_filtered = df_final.filter(df_final.value_scaled > 100)
logger.info("🔍 Filtered DataFrame (value_scaled > 100):")
df_filtered.show(5)

# -----------------------------
# 💾 Write to Target S3
# -----------------------------
logger.info(f"💾 Writing filtered data to: {target_path}")
df_filtered.write.mode("overwrite").option("header", "true").csv(target_path)
logger.info("✅ Write operation completed.")

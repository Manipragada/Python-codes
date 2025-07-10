from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from airflow.models import Variable

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("S3 Multi-DF RDD Pipeline with Debug") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Read S3 paths from Airflow Variables
source_path_1 = Variable.get("s3_source_path_1")
source_path_2 = Variable.get("s3_source_path_2")
target_path   = Variable.get("s3_target_path")

# Debug: Print S3 Paths
print(f"✅ Source Path 1: {source_path_1}")
print(f"✅ Source Path 2: {source_path_2}")
print(f"✅ Target Path  : {target_path}")

# Schema definitions
schema1 = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("value", IntegerType(), True)
])

schema2 = StructType([
    StructField("id", IntegerType(), True),
    StructField("category", StringType(), True)
])

# Step 1: Read first file into DataFrame
df1 = spark.read.option("header", "true").schema(schema1).csv(source_path_1)
print("📘 DataFrame 1 Schema:")
df1.printSchema()
print("📘 DataFrame 1 Preview:")
df1.show(5)

# Step 2: Read second file into DataFrame
df2 = spark.read.option("header", "true").schema(schema2).csv(source_path_2)
print("📘 DataFrame 2 Schema:")
df2.printSchema()
print("📘 DataFrame 2 Preview:")
df2.show(5)

# Step 3: Join both DataFrames
df_joined = df1.join(df2, on="id", how="inner")
print("🔗 Joined DataFrame Schema:")
df_joined.printSchema()
print("🔗 Joined DataFrame Preview:")
df_joined.show(5)

# Step 4: Convert to RDD and process
rdd_joined = df_joined.rdd.map(
    lambda row: (row.id, row.name.upper(), row.category.lower(), row.value * 2)
)
print("🌀 Sample RDD Output:")
for rec in rdd_joined.take(5):
    print(rec)

# Step 5: Convert back to DataFrame
final_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name_upper", StringType(), True),
    StructField("category_lower", StringType(), True),
    StructField("value_scaled", IntegerType(), True)
])

df_final = spark.createDataFrame(rdd_joined, schema=final_schema)
print("🧾 Final DataFrame Schema:")
df_final.printSchema()
print("🧾 Final DataFrame Preview:")
df_final.show(5)

# Step 6: Apply filter
df_filtered = df_final.filter(df_final.value_scaled > 100)
print("🔍 Filtered DataFrame Preview (value_scaled > 100):")
df_filtered.show(5)

# Step 7: Write result to S3
print(f"💾 Writing filtered data to: {target_path}")
df_filtered.write.mode("overwrite").option("header", "true").csv(target_path)
print("✅ Write completed.")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from airflow.models import Variable  # Only needed if you're using Airflow

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("S3 Multi-DF RDD Pipeline") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Airflow Variables for S3 paths (or use environment variables/config otherwise)
source_path_1 = Variable.get("s3_source_path_1")  # e.g., "s3a://your-bucket/input1/"
source_path_2 = Variable.get("s3_source_path_2")  # e.g., "s3a://your-bucket/input2/"
target_path   = Variable.get("s3_target_path")    # e.g., "s3a://your-bucket/output/"

# Schema for both files
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

# Step 2: Read second file into DataFrame
df2 = spark.read.option("header", "true").schema(schema2).csv(source_path_2)

# Step 3: Join both DataFrames
df_joined = df1.join(df2, on="id", how="inner")

# Step 4: Convert joined DataFrame to RDD
rdd_joined = df_joined.rdd.map(
    lambda row: (row.id, row.name.upper(), row.category.lower(), row.value * 2)
)

# Step 5: Convert back to DataFrame
final_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name_upper", StringType(), True),
    StructField("category_lower", StringType(), True),
    StructField("value_scaled", IntegerType(), True)
])

df_final = spark.createDataFrame(rdd_joined, schema=final_schema)

# Step 6: Further DataFrame transformation (example: filter)
df_filtered = df_final.filter(df_final.value_scaled > 100)

# Step 7: Write result to S3
df_filtered.write.mode("overwrite").option("header", "true").csv(target_path)

print(f"Processed data written to: {target_path}")

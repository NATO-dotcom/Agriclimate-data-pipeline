from pathlib import Path
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Dynamically find the project root regardless of where execution starts
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

YIELDS_PATH = str(PROJECT_ROOT / "spark_output" / "yields_parquet")
REGIONS_PATH = str(PROJECT_ROOT / "region_lookup.csv")
LOCAL_OUTPUT_PATH = str(PROJECT_ROOT / "spark_output" / "gold_yield_report.parquet")

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("AgriClimate_MinIO_Pythonic") \
    .getOrCreate()

print("Reading data from local disk...")
yields_df = spark.read.parquet(YIELDS_PATH)
regions_df = spark.read.option("header", "true").csv(REGIONS_PATH)

print("Joining and Aggregating...")
joined_df = yields_df.join(F.broadcast(regions_df), "region_id", "left")

final_report = joined_df \
    .groupBy("region_name", "climate_zone") \
    .agg(
        F.sum("yield_tons").alias("total_yield_tons"),
        F.count("*").alias("record_count")
    )

print("Converting Gold dataset to Pandas...")
gold_df = final_report.toPandas()
print(gold_df)

print("Saving Gold data locally...")
gold_df.to_parquet(LOCAL_OUTPUT_PATH)

print("Uploading to MinIO via native boto3...")
s3_client = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9000",
    aws_access_key_id="minio_admin",
    aws_secret_access_key="minio_password"
)

try:
    s3_client.head_bucket(Bucket="agri-data")
    print("Bucket 'agri-data' already exists.")
except Exception:
    s3_client.create_bucket(Bucket="agri-data")
    print("Created missing bucket: 'agri-data'")

s3_client.upload_file(LOCAL_OUTPUT_PATH, "agri-data", "gold_yield_report.parquet")

print("Success! Your Gold data is safely in MinIO.")
spark.stop()
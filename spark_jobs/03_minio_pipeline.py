import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("AgriClimate_MinIO_Pythonic") \
    .getOrCreate()

print("Reading data from local disk...")
yields_df = spark.read.parquet("../spark_output/yields_parquet")
regions_df = spark.read.option("header", "true").csv("../region_lookup.csv")

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
local_path = "../spark_output/gold_yield_report.parquet"
gold_df.to_parquet(local_path)

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

# Upload the file
s3_client.upload_file(local_path, "agri-data", "gold_yield_report.parquet")

print("Success! Your Gold data is safely in MinIO.")
spark.stop()
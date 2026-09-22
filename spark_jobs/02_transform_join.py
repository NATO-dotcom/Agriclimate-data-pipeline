from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("AgriClimate_Transform") \
    .getOrCreate()

print("1. Reading large Parquet data (Yields)...")
# Notice we don't need to define a schema! Parquet remembers it automatically.
yields_df = spark.read.parquet("../spark_output/yields_parquet")

print("2. Reading small CSV data (Regions)...")
regions_df = spark.read.option("header", "true").csv("../region_lookup.csv")

print("3. Performing Broadcast Join...")
joined_df = yields_df.join(
    F.broadcast(regions_df),
    yields_df.region_id == regions_df.region_id,
    "left"
)

print("4. Aggregating total yields per region...")
# GroupBy is a "Wide Transformation". This forces the CPU cores to shuffle data.
final_report = joined_df \
    .groupBy("region_name", "climate_zone") \
    .agg(
        F.sum("yield_tons").alias("total_yield_tons"),
        F.count("*").alias("record_count")
    ) \
    .orderBy(F.col("total_yield_tons").desc())

print("--- Final Aggregated Report ---")
final_report.show()

print("5. Saving Gold layer to Parquet...")
final_report.write \
    .mode("overwrite") \
    .parquet("../spark_output/gold_yield_report")
    
    
print("\n" + "="*50)
print("🚀 Spark Web UI is live!")
print("Open your web browser and go to: http://localhost:4040")
print("="*50 + "\n")
input("Press Enter in this terminal to shut down Spark and exit... ")

spark.stop()
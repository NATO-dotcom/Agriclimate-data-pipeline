# from pyspark.sql import SparkSession

# # 1. Initialize the Spark Engine
# spark = SparkSession.builder \
#     .master("local[*]") \
#     .appName("AgriClimate_FirstLook") \
#     .getOrCreate()

# print("Spark Session Created Successfully!")

# # 2. Read raw CSV data
# # We tell Spark to figure out the column types (inferSchema) and that there is a header row
# df = spark.read \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .csv("../yield.csv")

# # 3. Explore the DataFrame
# print("--- DataFrame Schema ---")
# df.printSchema()

# print("--- Top 5 Rows ---")
# df.show(5)

# # Shut down the Spark engine
# spark.stop()



from pyspark.sql import SparkSession
from pyspark.sql import types

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("AgriClimate_FirstLook") \
    .getOrCreate()

# 1. Define the strict blueprint
custom_schema = types.StructType([
    types.StructField("harvest_year", types.IntegerType(), True),
    types.StructField("region_id", types.StringType(), True),
    types.StructField("crop_type", types.StringType(), True),
    types.StructField("yield_tons", types.DoubleType(), True)
])

# 2. Read the data using the blueprint (No inferSchema)
df = spark.read \
    .option("header", "true") \
    .schema(custom_schema) \
    .csv("../yield.csv")

print("--- Strict Schema ---")
df.printSchema()
df.show()

# 3. Partition and write to Parquet
print("Writing to Parquet...")
df.repartition(2) \
    .write \
    .mode("overwrite") \
    .parquet("../spark_output/yields_parquet")

print("Success!")
spark.stop()
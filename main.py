from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, regexp_replace, to_date

spark = SparkSession.builder \
    .appName("ChocolateSalesPartitioning") \
    .getOrCreate()

file_path = "Chocolate Sales (2).csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

df_clean = df.withColumn("Amount_Clean", regexp_replace(col("Amount"), r"[\$,]", "").cast("float")) \
             .withColumn("Date_Clean", to_date(col("Date"), "dd/MM/yyyy"))

print("--- Strategy 1: Repartition by Number ---")
df_hash_part = df_clean.repartition(4) 
print(f"Number of partitions: {df_hash_part.rdd.getNumPartitions()}")

print("Pipeline 1 Results (Canada Sales Summary by Product):")
pipeline_1 = df_hash_part \
    .filter(col("Country") == "Canada") \
    .groupBy("Product") \
    .agg(sum("Amount_Clean").alias("Total_Sales"), sum("Boxes Shipped").alias("Total_Boxes")) \
    .orderBy(col("Total_Sales").desc())

pipeline_1.show(truncate=False)

print("\n--- Strategy 2: Repartition by Column ---")
df_col_part = df_clean.repartition("Country")

print("Pipeline 2 Results (2024 Top Salesperson per Country):")
pipeline_2 = df_col_part \
    .filter(col("Date_Clean") >= "2024-01-01") \
    .groupBy("Country", "Sales Person") \
    .agg(sum("Amount_Clean").alias("Total_Sales")) \
    .orderBy(col("Country").asc(), col("Total_Sales").desc())

pipeline_2.show(truncate=False)

print("\n--- Writing Partitioned Data to Disk ---")
output_path = "partitioned_chocolate_sales"

df_clean.write.partitionBy("Country").mode("overwrite").csv(output_path, header=True)

print(f"Success! Partitioned data has been saved to the '{output_path}' folder.")
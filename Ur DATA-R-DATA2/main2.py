from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, sum as s
from pyspark.sql.types import IntegerType, StringType, DoubleType, DateType

#  start spark session
spark = SparkSession.builder.appName("your-data-our-data").getOrCreate()

# create a dataframe from the csv file
df = spark.read.csv("dirty_cafe_sales.csv", header=True, inferSchema=True)
df.show(n=200)
# remove duplicates and rows with empty columns
df_no_dups = df.dropna().drop_duplicates()
df_no_dups.show(n=200)
# create a list of all keywords that we need to clean
keywords = ["ERROR", "UNKNOWN"]
# build a condition that dynamically check every columns if it contains the keywords
condition = None
for c in df_no_dups.columns:
    col_codition = None
    for k in keywords:
        check = lower(col(c).cast("string")).contains(k.lower())
        col_codition = check if col_codition is None else col_codition | check
    condition = col_codition if condition is None else condition | col_codition

# filter the dataframe using the condition we use `~` to invert/negate the condition and sort it alphabetically by item name
df_cols = df_no_dups.columns
print(df_cols)
df_clean = (
    df_no_dups.filter(~condition)
    .select(
        col(df_cols[0]),
        col(df_cols[1]),
        col(df_cols[2]).cast(IntegerType()),
        col(df_cols[3]).cast(DoubleType()),
        col(df_cols[4]).cast(DoubleType()),
        col(df_cols[5]),
        col(df_cols[6]),
        col(df_cols[7]).cast(DateType()),
    )
    .orderBy(col("item"))
)
df_clean.show()
df_clean.printSchema()

# *  First Insight
item_sales_summary = (
    df_clean.groupBy("Item")
    .agg(s("Quantity").alias("Total Quantity"), s("Total Spent").alias("Total Sales"))
    .orderBy("Item")
)
# item_sales_summary.show()

# * Second Insight
sales_per_payment = df_clean.groupBy("Payment Method").agg(
    s("Total Spent").alias("Total Sales")
)
# sales_per_payment.show()

# * Third Insight
sales_per_loc = df_clean.groupBy("Location").agg(s("Total Spent").alias("Total Sales"))
# sales_per_loc.show()

item_sales_summary.write.parquet("item_sale_Summary.parquet", mode="overwrite")
sales_per_payment.write.parquet("sales_per_payment.parquet", mode="overwrite")
sales_per_loc.write.parquet("sales_per_loc.parquet", mode="overwrite")


spark.stop()

# TODO: Group by Items and total the quantity, Total spent to create a sale summary for every item
# TODO: Group by Payment methods and get the sum of total spent to see sales on every payment methods
# TODO: Group by Location and get the sum of total spent to create sale summary for every location

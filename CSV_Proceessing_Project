from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, date_format
import json

spark = SparkSession.builder \
    .appName("CSV Processing") \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(r"C:\Users\pooja\Downloads\order_items_data_2_.csv")

rows_before = df.count()

df_no_duplicates = df.dropDuplicates()
duplicate_rows_removed = rows_before - df_no_duplicates.count()

non_empty_df = df_no_duplicates.na.drop(how="all")
empty_rows_removed = df_no_duplicates.count() - non_empty_df.count()

discarded_rows = df.subtract(non_empty_df)
discarded_rows.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(r"C:\Users\pooja\Downloads\discarded_rows.csv")

rows_after = non_empty_df.count()

stats = {
    "rows_before_processing": rows_before,
    "rows_after_processing": rows_after,
    "duplicate_rows_removed": duplicate_rows_removed,
    "empty_rows_removed": empty_rows_removed
}

with open(r"C:\Users\pooja\Downloads\processing_stats.json", "w") as f:
    json.dump(stats, f, indent=4)

monthly_metrics = non_empty_df.withColumn(
    "month", date_format(col("purchase_date"), "yyyy-MM")
).groupBy("month").agg(count("*").alias("total_rows"))

monthly_metrics.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(r"C:\Users\pooja\Downloads\monthly_metrics.csv")

spark.stop()

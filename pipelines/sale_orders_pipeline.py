import dlt
from pyspark.sql.functions import col, sum as _sum, count

@dlt.table(
    name="bronze.bronze_sales_orders",
    comment="Raw sales orders ingested from CSV landing zone"
)
def bronze_sales_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/learning/bronze/raw_files/")
    )

@dlt.table(
    name="silver.silver_sales_orders",
    comment="Cleaned, deduplicated sales orders"
)
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
@dlt.expect_or_drop("valid_price", "unit_price > 0")
@dlt.expect("valid_status", "status IS NOT NULL")
def silver_sales_orders():
    return (
        dlt.read("bronze.bronze_sales_orders")
        .dropDuplicates()
        .dropna(how="all")
    )

@dlt.table(
    name="gold.gold_sales_summary",
    comment="Aggregated revenue by state and category"
)
def gold_sales_summary():
    df = dlt.read("silver.silver_sales_orders")
    return (
        df.withColumn("revenue", (col("quantity") * col("unit_price")) - col("discount"))
          .groupBy("state", "category")
          .agg(
              _sum("revenue").alias("total_revenue"),
              _sum("quantity").alias("total_quantity_sold"),
              count("*").alias("order_count")
          )
    )
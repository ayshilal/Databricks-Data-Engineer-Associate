import dlt
from pyspark.sql import functions as F


@dlt.table(name="bronze_orders_raw", comment="Raw ingest")
def bronze_orders_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/cert_lab/bronze/landing_ext/_schema/dlt",
        )
        .load("/Volumes/cert_lab/bronze/landing_ext/orders")
    )


@dlt.table(name="silver_orders_clean")
@dlt.expect("valid_qty", "quantity > 0")                        # warn: record kept, metric logged
@dlt.expect_or_drop("valid_price", "unit_price IS NOT NULL")    # drop the bad row
@dlt.expect_or_fail("valid_id", "order_id IS NOT NULL")         # fail the whole update
def silver_orders_clean():
    return dlt.read_stream("bronze_orders_raw").withColumn(
        "line_total", F.col("quantity") * F.col("unit_price")
    )


@dlt.table(name="gold_product_revenue")
def gold_product_revenue():
    return (
        dlt.read("silver_orders_clean")
        .groupBy("product")
        .agg(F.sum("line_total").alias("revenue"))
    )
# Databricks notebook source
from pyspark.sql import functions as F

raw = spark.createDataFrame([
    (1, "  widget ", "5",  "10.50", "2026-01-05", "alice@x.com"),
    (2, "GIZMO",     None, "20.00", "2026-01-06", "BOB@X.COM "),
    (3, "widget",    "3",  None,    "2026-01-07", None),
    (3, "widget",    "3",  "9.99",  "2026-01-07", "carol@x.com"),
    (4, None,        "2",  "15.00", "not-a-date", "dave@x.com"),
], "order_id int, product string, quantity string, unit_price string, order_date string, email string")

raw.write.mode("overwrite").saveAsTable("cert_lab.bronze.orders_dirty")
display(raw)

# COMMAND ----------

# MAGIC %md
# MAGIC That builds a deliberately messy five-row table so Lab 3.1 has something to clean. Each row carries a specific defect:
# MAGIC
# MAGIC Row 1 — " widget " with surrounding spaces, for trim.
# MAGIC
# MAGIC Row 2 — "GIZMO" in caps for lower, a null quantity, and "BOB@X.COM " with both case and whitespace problems.
# MAGIC
# MAGIC Row 3 — null unit_price and null email.
# MAGIC
# MAGIC Row 4 — same order_id as row 3, for deduplication.
# MAGIC
# MAGIC Row 5 — null product and "not-a-date" in a date field.
# MAGIC
# MAGIC Note every column is declared string except order_id. That's deliberate — it's how data actually arrives from CSV and JSON, and it forces the casting step.

# COMMAND ----------

silver = (spark.table("cert_lab.bronze.orders_dirty")
    .withColumn("product",    F.lower(F.trim(F.col("product"))))      # standardize strings
    .withColumn("email",      F.lower(F.trim(F.col("email"))))
    .withColumn("quantity",   F.col("quantity").cast("int"))          # fix data types
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("order_date", F.try_to_date(F.col("order_date"), "yyyy-MM-dd"))  # bad dates -> null
    .na.fill({"quantity": 0, "product": "unknown"})                   # fill nulls
    .na.drop(subset=["unit_price"])                                   # drop unusable rows
    .dropDuplicates(["order_id"])                                     # dedupe
    .withColumn("line_total", F.col("quantity") * F.col("unit_price"))  # derive
)

silver.write.mode("overwrite").saveAsTable("cert_lab.silver.orders")
silver.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cert_lab.silver.orders AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   coalesce(lower(trim(product)), 'unknown')  AS product,
# MAGIC   lower(trim(email))                          AS email,
# MAGIC   coalesce(CAST(quantity AS INT), 0)          AS quantity,
# MAGIC   CAST(unit_price AS DOUBLE)                  AS unit_price,
# MAGIC   try_to_date(order_date, 'yyyy-MM-dd')       AS order_date
# MAGIC FROM cert_lab.bronze.orders_dirty
# MAGIC WHERE unit_price IS NOT NULL
# MAGIC QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY unit_price DESC) = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC A Nice query we found here

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC order_id,unit_price,
# MAGIC   max(unit_price) OVER ( PARTITION BY order_id) AS max_for_this_order
# MAGIC FROM cert_lab.silver.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- add QUALIFY: filters on rn, leaves 4 rows
# MAGIC SELECT order_id, unit_price
# MAGIC FROM cert_lab.bronze.orders_dirty
# MAGIC QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY unit_price DESC) = 1;
# MAGIC
# MAGIC --OVER only attaches to window functions: row_number(), rank(), max(), sum(), lag(), and so on.

# COMMAND ----------



# COMMAND ----------

# Build a second table to join against: 30 customers, alternating region EU/US
customers = spark.createDataFrame(
    [(i, f"cust_{i}", "EU" if i % 2 else "US") for i in range(1, 31)],
    "customer_id int, customer_name string, region string"
)
customers.write.mode("overwrite").saveAsTable("cert_lab.silver.customers")

# COMMAND ----------

o = spark.table("cert_lab.bronze.orders_copy")
c = spark.table("cert_lab.silver.customers")


o.join(c, "customer_id", "inner").count()
o.join(c, "customer_id", "left").count() # only matches
# all orders, nulls where no customer
o.join(c, "customer_id", "right").count()
o.join(c, "customer_id", "outer").count()
o.join(c, "customer_id", "left_anti").count() # orders with NO matching customer
o.join(c, "customer_id", "left_semi").count() # orders WITH a match, no customer cols added
o.crossJoin(c).count() 

print("total orders", o.count())
print("inner       ", o.join(c, "customer_id", "inner").count())
print("left        ", o.join(c, "customer_id", "left").count())
print("left_anti   ", o.join(c, "customer_id", "left_anti").count())
print("left_semi   ", o.join(c, "customer_id", "left_semi").count())
print("cross       ", o.crossJoin(c).count())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cert_lab.silver.customers;
# MAGIC
# MAGIC select * from cert_lab.bronze.orders_copy;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- semi: orders that HAVE a matching customer
# MAGIC SELECT o.*
# MAGIC FROM cert_lab.bronze.orders_copy o
# MAGIC LEFT SEMI JOIN cert_lab.silver.customers c
# MAGIC   ON o.customer_id = c.customer_id;
# MAGIC
# MAGIC -- anti: orders with NO matching customer
# MAGIC SELECT o.*
# MAGIC FROM cert_lab.bronze.orders_copy o
# MAGIC LEFT ANTI JOIN cert_lab.silver.customers c
# MAGIC   ON o.customer_id = c.customer_id;

# COMMAND ----------

o.join(c, (o.customer_id == c.customer_id) & (o.product.isNotNull()), "inner").display()
o.join(c, "customer_id", "inner").display()

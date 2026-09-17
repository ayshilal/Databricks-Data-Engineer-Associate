# Databricks notebook source
import json

nested = [{"order_id": i,
           "customer": {"id": i % 20, "tier": "gold" if i % 3 == 0 else "silver"},
           "items": [{"sku": f"S{j}", "qty": j + 1} for j in range(1 + i % 3)],
           "tags": ["online", "promo"]} for i in range(1, 101)]

dbutils.fs.put("/Volumes/cert_lab/bronze/landing_ext/nested/orders_nested.json",
               "\n".join(json.dumps(r) for r in nested), overwrite=True)

# COMMAND ----------

df = spark.read.json("/Volumes/cert_lab/bronze/landing_ext/nested/")
df.printSchema()
df.write.mode("overwrite").saveAsTable("cert_lab.bronze.orders_nested")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC order_id,
# MAGIC customer.id AS customer_id, -- dot notation into a struct
# MAGIC customer.tier AS tier,
# MAGIC item.sku,
# MAGIC item.qty,
# MAGIC tags[0] AS first_tag, -- array indexing
# MAGIC size(tags) AS tag_count
# MAGIC FROM cert_lab.bronze.orders_nested
# MAGIC LATERAL VIEW EXPLODE(items) AS item; -- one row per array element
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC One order with three line items becomes three rows. order_id repeats on each; item holds a different element each time.
# MAGIC
# MAGIC "Lateral" means the expression can refer to columns from the row it's attached to — EXPLODE(items) uses this row's items. That's what makes it work per-row rather than as a standalone table.
# MAGIC
# MAGIC Two things worth knowing:
# MAGIC
# MAGIC EXPLODE drops rows whose array is empty or null. Use EXPLODE_OUTER if you need to keep them with a null in the exploded column — same distinction as inner versus left join.
# MAGIC
# MAGIC And Databricks lets you skip the syntax entirely:
# MAGIC
# MAGIC sql
# MAGIC SELECT order_id, item.sku FROM cert_lab.bronze.orders_nested, LATERAL EXPLODE(items) AS item;
# MAGIC
# MAGIC or in PySpark just .withColumn("item", explode("items")), which is usually what people write.
# MAGIC
# MAGIC The exam cares that you know explode is the operation that flattens an array into rows, and that its output row count is the sum of the array lengths.

# COMMAND ----------

from pyspark.sql.functions import col, explode, size
(spark.table("cert_lab.bronze.orders_nested")
.withColumn("item", explode("items"))
.select("order_id", col("customer.id").alias("customer_id"),
"customer.tier", "item.sku", "item.qty", size("tags").alias("tag_count"))
.display())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   raw:order_id::int              AS via_colon,
# MAGIC   from_json(raw, 'order_id INT, qty INT') AS parsed,
# MAGIC   from_json(raw, 'order_id INT, qty INT').qty AS via_struct
# MAGIC FROM (SELECT '{"order_id":1,"qty":5}' AS raw);
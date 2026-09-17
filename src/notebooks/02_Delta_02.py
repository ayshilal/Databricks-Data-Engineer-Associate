# Databricks notebook source
vol = "/Volumes/cert_lab/bronze/landing_ext/orders"
dbutils.fs.mkdirs(vol)

import json, random

def make_batch(n, start_id, path):
    recs = [{"order_id": start_id + i,
             "customer_id": random.randint(1, 50),
             "product": random.choice(["widget", "gizmo", "doohickey"]),
             "quantity": random.randint(1, 10),
             "unit_price": round(random.uniform(5, 200), 2)}
            for i in range(n)]
    dbutils.fs.put(path, "\n".join(json.dumps(r) for r in recs), overwrite=True)

make_batch(200, 1, f"{vol}/batch_01.json")
display(dbutils.fs.ls(vol))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS cert_lab.bronze.orders_copy (
# MAGIC     order_id BIGINT, customer_id BIGINT, product STRING, 
# MAGIC     quantity BIGINT, unit_price DOUBLE );
# MAGIC
# MAGIC COPY INTO cert_lab.bronze.orders_copy
# MAGIC FROM '/Volumes/cert_lab/bronze/landing_ext/orders'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ('inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');cert_lab.bronze.orders_copy
# MAGIC

# COMMAND ----------

make_batch(200, 201, f"{vol}/batch_02.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO cert_lab.bronze.orders_copy
# MAGIC FROM '/Volumes/cert_lab/bronze/landing_ext/orders'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ('inferSchema' = 'true');
# MAGIC SELECT count(*) FROM cert_lab.bronze.orders_copy;
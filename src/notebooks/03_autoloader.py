# Databricks notebook source
src = "/Volumes/cert_lab/bronze/landing_ext/orders"
ckpt = "/Volumes/cert_lab/bronze/landing_ext/_ckpt/orders_al"
schema_loc = "/Volumes/cert_lab/bronze/landing_ext/_schema/orders_al"
(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "json")
.option("cloudFiles.schemaLocation", schema_loc)
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
.load(src)
.writeStream
.option("checkpointLocation", ckpt)
.option("mergeSchema", "true")
.trigger(availableNow=True) .toTable("cert_lab.bronze.orders_autoloader"))

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `cert_lab`; select * from `bronze`.`orders_autoloader` limit 100;

# COMMAND ----------

src = "/Volumes/cert_lab/bronze/landing_ext/orders"

import json
recs = [{"order_id": 900+i, "customer_id": 7, "product": "widget",
"quantity": 2, "unit_price": 9.99, "discount_pct": 0.1} for i in range(20)]
dbutils.fs.put(f"{src}/batch_03.json",
"\n".join(json.dumps(r) for r in recs), overwrite=True)  

# COMMAND ----------

# MAGIC %md
# MAGIC **Rerun the Auto Loader**

# COMMAND ----------

src = "/Volumes/cert_lab/bronze/landing_ext/orders"
ckpt = "/Volumes/cert_lab/bronze/landing_ext/_ckpt/orders_al"
schema_loc = "/Volumes/cert_lab/bronze/landing_ext/_schema/orders_al"
(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "json")
.option("cloudFiles.schemaLocation", schema_loc)
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
.load(src)
.writeStream
.option("checkpointLocation", ckpt)
.option("mergeSchema", "true")
.trigger(availableNow=True) .toTable("cert_lab.bronze.orders_autoloader"))

# COMMAND ----------

# MAGIC %md
# MAGIC Above first returned CloudFileNotFoundException error, so I reran

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM cert_lab.bronze.orders_autoloader WHERE _rescued_data IS NOT NULL;

# COMMAND ----------

# MAGIC %md after rerunning _rescued_data column in orders_autoloader has 0 records.
# MAGIC And discount_pct is now present. Let's confirm in another way: 

# COMMAND ----------

# MAGIC %sql DESCRIBE cert_lab.bronze.orders_autoloader
# MAGIC

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

w.secrets.create_scope(scope="cert-scope")
w.secrets.put_secret(scope="cert-scope", key="sql-user", string_value="reader")

print([s.name for s in w.secrets.list_scopes()])

# COMMAND ----------

print(dbutils.secrets.get("cert-scope", "sql-user"))
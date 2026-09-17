# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE MATERIALIZED VIEW gold.daily_sales
# MAGIC AS SELECT
# MAGIC   date_trunc('DAY', order_ts) AS day,
# MAGIC   store_id,
# MAGIC   sum(amount) AS total_sales,
# MAGIC   count(*) AS order_count
# MAGIC FROM silver.orders
# MAGIC GROUP BY 1, 2;
# MAGIC
# MAGIC DESCRIBE CATALOG EXTENDED dbw_cert_lab;
# MAGIC
# MAGIC SHOW STORAGE CREDENTIALS;
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE MATERIALIZED VIEW cert_lab.gold.mv_sales_by_product 
# MAGIC
# MAGIC SCHEDULE EVERY 1 DAY
# MAGIC
# MAGIC as SELECT product, sum(quantity * unit_price) as revenue from cert_lab.bronze.orders_copy group by product;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cert_lab.gold.mv_sales_by_product;
# Databricks notebook source
# MAGIC %sql
# MAGIC -- 1 Create a managed Delta table
# MAGIC CREATE OR REPLACE TABLE cert_lab.bronze.demo_delta (
# MAGIC     id INT, name STRING,amount DOUBLE, td TIMESTAMP
# MAGIC );
# MAGIC -- INSERT sample data
# MAGIC -- INSERT INTO cert_lab.bronze.demo_delta VALUES
# MAGIC -- (1, 'alpha', 10.5, current_timestamp()),
# MAGIC -- (2, 'beta', 20.0, current_timestamp()),
# MAGIC -- (3, 'gamma', 30.25, current_timestamp());
# MAGIC
# MAGIC -- View table history
# MAGIC -- DESCRIBE HISTORY cert_lab.bronze.demo_delta;
# MAGIC
# MAGIC -- View table details
# MAGIC -- DESCRIBE DETAIL cert_lab.bronze.demo_delta;
# MAGIC
# MAGIC -- Update a record
# MAGIC -- UPDATE cert_lab.bronze.demo_delta SET amount = 99.0 WHERE id = 1;
# MAGIC
# MAGIC -- Delete a record
# MAGIC -- DELETE FROM cert_lab.bronze.demo_delta WHERE id = 3;
# MAGIC
# MAGIC -- Time travel: select all records
# MAGIC -- SELECT * FROM cert_lab.bronze.demo_delta ;
# MAGIC
# MAGIC -- Restore table to version 0
# MAGIC -- RESTORE TABLE cert_lab.bronze.demo_delta TO VERSION AS OF 0;
# MAGIC
# MAGIC -- Compaction and cleanup
# MAGIC -- OPTIMIZE cert_lab.bronze.demo_delta; -- file compaction
# MAGIC -- VACUUM cert_lab.bronze.demo_delta RETAIN 168 HOURS DRY RUN; --retantion can't be less then 7 days
# MAGIC
# MAGIC
# MAGIC
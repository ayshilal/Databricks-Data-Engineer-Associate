# Databricks notebook source
print("transform running")
spark.sql("SELECT count(*) FROM cert_lab.silver.silver_orders_clean").show()

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")
print(env)
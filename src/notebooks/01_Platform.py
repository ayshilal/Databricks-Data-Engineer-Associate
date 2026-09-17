# Databricks notebook source
try:    cluster_name = spark.conf.get("spark.databricks.clusterUsageTags.clusterName")
except Exception:
    cluster_name = "Cluster name not available"

spark.sql("SELECT current_catalog(), current_schema(), current_user()").show()
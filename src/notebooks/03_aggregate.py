# Databricks notebook source
print("aggregate running")
spark.sql("SELECT * FROM cert_lab.gold.mv_sales_by_product").show()
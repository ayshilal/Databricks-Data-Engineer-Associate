# Databricks notebook source
dbutils.jobs.taskValues.set(key="row_count", value=spark.table("cert_lab.bronze.orders_copy").count())
# Databricks notebook source
df = spark.read.format("csv").load("abfss://temp@devrajprojstorageaccount.dfs.core.windows.net/Wealth-AccountData.csv",header=True,inferschema=True)

df.display()

# COMMAND ----------

dbutils.help()

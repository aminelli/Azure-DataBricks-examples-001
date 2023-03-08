# Databricks notebook source
display(dbutils.fs.ls('/databricks-datasets'))

# COMMAND ----------

f = open('/dbfs/databricks-datasets/README.md', 'r')
print(f.read())

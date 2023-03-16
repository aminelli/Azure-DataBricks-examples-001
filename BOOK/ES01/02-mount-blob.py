# Databricks notebook source
storageAccount="storage001xxtestxxblob"
storageKey ="s7Byb4jC8luLXWO3kSKo9iXszHvCaGqg/jBmps2YEnOnLAsPUB0uYjjD8RJa+GQYanEC5LQgpd+N+AStwin9tw=="
mountpoint = "/mnt/Blob"
storageEndpoint =   "wasbs://rawdata@{}.blob.core.windows.net".format(storageAccount)
storageConnSting = "fs.azure.account.key.{}.blob.core.windows.net".format(storageAccount)

try:
  dbutils.fs.mount(
  source = storageEndpoint,
  mount_point = mountpoint,
  extra_configs = {storageConnSting:storageKey})
except:
    print("Already mounted...."+mountpoint)

# COMMAND ----------

# MAGIC %fs ls /mnt/Blob

# COMMAND ----------

display(dbutils.fs.ls("/mnt/Blob"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/Blob"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/Blob"))

# COMMAND ----------

df_ord= spark.read.format("csv").option("header",True).load("dbfs:/mnt/Blob/Orders.csv")

# COMMAND ----------

display(df_ord.limit(10))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/Blob"))

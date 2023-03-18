# Databricks notebook source
# MAGIC %md
# MAGIC # STORAGE BLOB

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metodo con accesso diretto allo storage con key

# COMMAND ----------

# Variabili di appoggio
storageAccount = "sacorsoblob000"
# acct_info = "fs.azure.account.key.{}.dfs.core.windows.net".format(storageAccount)
acct_info = "fs.azure.account.key.{}.blob.core.windows.net".format(storageAccount)
accessKey = "CYRGhoLpzUXcQhhreZDpE2A02Y0KgHNE1ELgrIxHuxULX2sT4wcFoBQLQKrDLJb7ixAT/N64mx36+AStmfso6w=="

display(acct_info)

# COMMAND ----------

# Connessione passata a spark

spark.conf.set(acct_info, accessKey)

# COMMAND ----------

# csvFilePath = "abfss://rawdata@{}.dfs.core.windows.net/Orders.csv".format(storageAccount)
csvFilePath = "wasbs://rawdata@{}.blob.core.windows.net/Orders.csv".format(storageAccount)
display(csvFilePath)
dbutils.fs.ls(csvFilePath)

# COMMAND ----------

ordersDF = spark.read.format("csv").option("header", True).load(csvFilePath)
display(ordersDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metodo con key + mount

# COMMAND ----------

# Variabili di appoggio
storageAccount     = "sacorsoblob000"
storageKey         = "CYRGhoLpzUXcQhhreZDpE2A02Y0KgHNE1ELgrIxHuxULX2sT4wcFoBQLQKrDLJb7ixAT/N64mx36+AStmfso6w=="
mountpoint         = "/mnt/BlobStorage"
storageEndPoint    = "wasbs://rawdata@{}.blob.core.windows.net".format(storageAccount)
storageConnString  = "fs.azure.account.key.{}.blob.core.windows.net".format(storageAccount)



# try:
dbutils.fs.mount(
    source        = storageEndPoint,
    mount_point   = mountpoint,
    extra_configs = {storageConnString : storageKey }
)




# COMMAND ----------

# MAGIC %fs ls mnt/BlobStorage

# COMMAND ----------

csvFilePath = "dbfs:{}/Orders.csv".format(mountpoint)
ordersDF2 = spark.read.format("csv").option("header", True).load(csvFilePath)
display(ordersDF2)

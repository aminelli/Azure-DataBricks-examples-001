# Databricks notebook source
# MAGIC %md
# MAGIC # STORAGE GEN 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metodo con APP di appoggio + mount

# COMMAND ----------

# Variabili di appoggio
storageAccount = "sacorsogen2000"
mountpoint = "/mnt/Gen2"
storageEndPoint = "abfss://rawdata@{}.dfs.core.windows.net/".format(storageAccount)

print(storageEndPoint)

# ID Management
clientID       = "28827ed2-76b6-40f4-b085-1fee5902bfa4"
tenantID       = "6c7d8b3b-8c71-4013-be58-e78af03cde75"
clientSecret   = "Th58Q~MyRIWywFpsN3oZvg~uii2HA34WNlsEgc5I"
oauth2Endpoint = "https://login.microsoftonline.com/{}/oauth2/token".format(tenantID)

print(oauth2Endpoint)

configs = {"fs.azure.account.auth.type" : "OAuth" ,
           "fs.azure.account.oauth.provider.type" : "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id" : clientID,
           "fs.azure.account.oauth2.client.secret" : clientSecret,
           "fs.azure.account.oauth2.client.endpoint" : oauth2Endpoint}


#try:
dbutils.fs.mount(
source = storageEndPoint,
mount_point = mountpoint,
extra_configs = configs)
#except:
#print("Error :" + mountpoint)



# COMMAND ----------

# MAGIC %fs ls /mnt/Gen2

# COMMAND ----------

display(dbutils.fs.ls("/mnt/Gen2"))

# COMMAND ----------

orders_data_frame = spark.read.format("csv").option("header", True).load("dbfs:/mnt/Gen2/Orders.csv")
display(orders_data_frame)

# COMMAND ----------

dbutils.fs.unmount("/mnt/Gen2")
display(dbutils.fs.ls("/mnt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metodo con accessi diretto allo storage con key

# COMMAND ----------

# Variabili di appoggio
storageAccount = "sacorsogen2000"
acct_info = "fs.azure.account.key.{}.dfs.core.windows.net".format(storageAccount)
accessKey = "P9B0EqvhWjhMDeQSL8VmDlvHWEE8pTAjO/fDeXvC1Fddh8V36ujCYN2fCGUgFfikhGgP3Lm0IIJc+AStCr25Ug=="

display(acct_info)





# COMMAND ----------

# Connessione passata a spark

spark.conf.set(acct_info, accessKey)

# COMMAND ----------

csvFilePath = "abfss://rawdata@{}.dfs.core.windows.net/Orders.csv".format(storageAccount)
display(csvFilePath)
dbutils.fs.ls(csvFilePath)

# COMMAND ----------

ordersDF = spark.read.format("csv").option("header", True).load(csvFilePath)
display(ordersDF)

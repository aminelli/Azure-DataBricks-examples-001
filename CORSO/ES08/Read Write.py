# Databricks notebook source
# MAGIC %md
# MAGIC # READ AND WRITE ON EXTERNAL STORAGE

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# Variabili di appoggio

storageAccount    = "sacorsoblob000"
storageKey        = "CYRGhoLpzUXcQhhreZDpE2A02Y0KgHNE1ELgrIxHuxULX2sT4wcFoBQLQKrDLJb7ixAT/N64mx36+AStmfso6w=="
mountpoint        = "/mnt/BlobTest"
storageEndPoint   = "wasbs://rawdata@{}.blob.core.windows.net".format(storageAccount)
storageConnString = "fs.azure.account.key.{}.blob.core.windows.net".format(storageAccount)
parquetCustomerDestMount = "{}/Customer/parquetFiles".format(mountpoint)

try:
    dbutils.fs.mount(
        source        = storageEndPoint,
        mount_point   = mountpoint,
        extra_configs = { storageConnString : storageKey }
    )
except:
    print("Mount già effettuato..." + mountpoint)


# COMMAND ----------

# MAGIC %fs ls /mnt/BlobTest

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.ls("/mnt/BlobTest/Customer/csvFiles/"))

# COMMAND ----------

csvFolder = "{}/Customer/csvFiles/".format(mountpoint)
print(csvFolder)
customerDF = spark.read.format("csv").option("header", True).load(csvFolder)


# COMMAND ----------

customerDF.printSchema()

# COMMAND ----------

customerDF = spark.read.format("csv").option("header", True).option("inferSchema", True).load(csvFolder)

# COMMAND ----------

customerDF.printSchema()

# COMMAND ----------

display(customerDF.limit(100))

# COMMAND ----------

customer_schema = StructType([
     StructField("C_CUSTKEY"     , IntegerType()),
     StructField("C_NAME"        , StringType()),
     StructField("C_ADDRESS"     , StringType()),
     StructField("C_NATIONKEY"   , ShortType()),
     StructField("C_PHONE"       , StringType()),
     StructField("C_ACCTBAL"     , DoubleType()),
     StructField("C_MKTSEGMENT"  , StringType()),
     StructField("C_COMMENT"     , StringType())
])



# COMMAND ----------

customerDF = spark.read.format("csv").option("header", True).schema(customer_schema).load(csvFolder)

# COMMAND ----------

customerDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scrivere i dati letti dall'azure Blob sul mout effettuato sotto forma di DBFS (Lake)

# COMMAND ----------

customer_partitioned = customerDF.repartition(10)
customer_partitioned.write.mode("overwrite").option("header", True).parquet(parquetCustomerDestMount)

# COMMAND ----------

display(dbutils.fs.ls(parquetCustomerDestMount))

# COMMAND ----------

# MAGIC %md
# MAGIC ## USO DIRETTO DELLE API

# COMMAND ----------

# Variabili di appoggio
# storageAccount    = "sacorsoblob000"
# storageKey        = "CYRGhoLpzUXcQhhreZDpE2A02Y0KgHNE1ELgrIxHuxULX2sT4wcFoBQLQKrDLJb7ixAT/N64mx36+AStmfso6w=="
# mountpoint        = "/mnt/BlobTest"
# storageEndPoint   = "wasbs://rawdata@{}.blob.core.windows.net".format(storageAccount)
# storageConnString = "fs.azure.account.key.{}.blob.core.windows.net".format(storageAccount)
# parquetCustomerDestMount = "{}/Customer/parquetFiles".format(mountpoint)

spark.conf.set(storageConnString, storageKey)
storageEndPointFolders = "{}/Customer".format(storageEndPoint)
display(storageEndPointFolders)

# COMMAND ----------

display(dbutils.fs.ls(storageEndPointFolders))

# COMMAND ----------

csvFolderApi = "{}/csvFiles/".format(storageEndPointFolders)
customerDF = spark.read.format("csv").option("header", True).schema(customer_schema).load(csvFolderApi)

# COMMAND ----------

display(customerDF.limit(10))

# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "e2fc345e-6d36-4233-9d97-c488bf701e6f",
          "fs.azure.account.oauth2.client.secret": "kkf5T1K6ItqBh.T.~320ssFSDLAY514-1.",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/e4e34038-ea1f-4882-b6e8-ccd776459ca0/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://fire-data@foostorage.dfs.core.windows.net/",
  mount_point = "/mnt/foostorage_mount",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/foostorage_mount/fire-data

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/foostorage_mount/fire_delta/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/foostorage_mount/fire_delta/_delta_log/

# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/foostorage_mount/fire_delta/_delta_log/00000000000000000000.json

# COMMAND ----------

display(spark.read.json("dbfs:/mnt/foostorage_mount/fire_delta/_delta_log/00000000000000000000.json"))

# COMMAND ----------



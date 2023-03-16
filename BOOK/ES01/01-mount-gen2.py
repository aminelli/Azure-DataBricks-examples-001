# Databricks notebook source
storageAccount="storage001xxtestxxag2"
mountpoint = "/mnt/Gen2"
storageEndPoint ="abfss://rawdata@{}.dfs.core.windows.net/".format(storageAccount)
print ('Mount Point ='+mountpoint)

#ClientId, TenantId and Secret is for the Application(ADLSGen2App) was have created as part of this recipe
clientID ="d424da27-0573-499b-9a0f-d68569188263"
tenantID ="6c7d8b3b-8c71-4013-be58-e78af03cde75"
clientSecret ="LkL8Q~Vbrl1OBV6uIFAyG82vqArreZyHPM2nzcH2"
oauth2Endpoint = "https://login.microsoftonline.com/{}/oauth2/token".format(tenantID)


configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": clientID,
           "fs.azure.account.oauth2.client.secret": clientSecret,
           "fs.azure.account.oauth2.client.endpoint": oauth2Endpoint}

try:
  dbutils.fs.mount(
  source = storageEndPoint,
  mount_point = mountpoint,
  extra_configs = configs)
except:
    print("Already mounted...."+mountpoint)


# COMMAND ----------

# MAGIC %fs ls /mnt/Gen2

# COMMAND ----------

display(dbutils.fs.ls("/mnt/Gen2"))

# COMMAND ----------

df_ord=spark.read.format("csv").option("header",True).load("dbfs:/mnt/Gen2/Orders.csv")

# COMMAND ----------

display(df_ord)

# COMMAND ----------

dbutils.fs.unmount("/mnt/Gen2")

# COMMAND ----------

storageAccount="storage001xxtestxxag2"
acct_info="fs.azure.account.key.{}.dfs.core.windows.net".format(storageAccount)

accesskey="8jalAEUA+EB/YKmr8GVSy5x2xNwU4vf3ppbomJ6V7VO8p9ErWlph7rU99ijHGIdCHRXtJxvgoKzY+ASt33wpaw==" 
print(acct_info)

# COMMAND ----------

spark.conf.set(acct_info,accesskey)

# COMMAND ----------

dbutils.fs.ls("abfss://rawdata@storage001xxtestxxag2.dfs.core.windows.net/Orders.csv")

# COMMAND ----------

ordersDF =spark.read.format("csv").option("header",True).load("abfss://rawdata@storage001xxtestxxag2.dfs.core.windows.net/Orders.csv")

# COMMAND ----------

display(ordersDF)

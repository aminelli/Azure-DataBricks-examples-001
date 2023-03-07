# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "232cfdcd-72be-4305-b2a5-a63962f0973e",
          "fs.azure.account.oauth2.client.secret": "z_pbm5ZIr85cfe0.-_ci-6lo72TEdvnXmS",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/e4e34038-ea1f-4882-b6e8-ccd776459ca0/oauth2/token"}

dbutils.fs.mount(
  source = "abfss://analytics-container@analyticsdatalakepackt.dfs.core.windows.net/",
  mount_point = "/mnt/analyticsdatalakepackt_mnt",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs mounts

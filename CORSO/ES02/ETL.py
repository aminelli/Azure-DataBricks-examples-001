# Databricks notebook source
# MAGIC %md
# MAGIC # ETL EXAMPLE

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/structured-streaming/events/

# COMMAND ----------

f = open("/dbfs/databricks-datasets/structured-streaming/events/file-0.json","r")
print(f.read())

# COMMAND ----------

from pyspark.sql.functions import input_file_name, current_timestamp

# Definiamo le variabili che useremo nei comandi successivi
file_path = "/databricks-datasets/structured-streaming/events"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_' ) ").first()[0]
table_name = f"{username}_etl_quickstart"
checkpoint_path= f"/tmp/{username}/_checkpoint/etl_quickstart"

print(file_path)
print(username)
print(table_name)
print(checkpoint_path)

# Clear deegli eventuali dati creati precedentemente
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configurazione di un Auto Loader per effettuare il data ingest da Json in Delta Table

(spark.readStream
     .format("cloudFiles")
     .option("cloudFiles.format","json")
     .option("cloudFiles.schemaLocation", checkpoint_path)
     .load(file_path)
     .select("*", input_file_name().alias("source_file"), current_timestamp().alias("processing_time"))
     .writeStream
     .option("checkpointLocation", checkpoint_path)
     .trigger(availableNow=True)
     .toTable(table_name))



# COMMAND ----------

df = spark.read.table(table_name)
display(df)

# COMMAND ----------

# MAGIC %fs ls /tmp/antonio_minelli_etlforma_com/_checkpoint/etl_quickstart

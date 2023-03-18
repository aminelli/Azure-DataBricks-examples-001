# Databricks notebook source
# MAGIC %md
# MAGIC # RECUPERO DATI DATASET AIRLINES

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/airlines/

# COMMAND ----------

f = open("/dbfs/databricks-datasets/airlines/README.md","r")
print(f.read())

# COMMAND ----------

# f = open("/dbfs/databricks-datasets/airlines/part-00000","r")
# print(f.read())

# COMMAND ----------

airlines = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter",",")
    .csv("dbfs:/databricks-datasets/airlines/part-00000")
)

display(airlines.limit(100))

# COMMAND ----------

airlines.write.mode("overwrite").format("delta").save("dbfs:/corso/airlines")

# COMMAND ----------

# MAGIC %fs ls /corso/airlines

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/corso/airlines"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/corso/airlines/_delta_log"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS airlines_dt;
# MAGIC 
# MAGIC CREATE TABLE airlines_dt USING DELTA LOCATION "dbfs:/corso/airlines"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as nrRec from airlines_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from airlines_dt limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from airlines_dt where month = '10'

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from airlines_dt where month = '10'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from airlines_dt where dest = 'SFO'

# COMMAND ----------

# MAGIC %sql
# MAGIC update airlines_dt SET Dest = 'San Francisco' where dest = 'SFO'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from airlines_dt where Dest = 'San Francisco'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY airlines_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from airlines_dt where Dest = 'SFO'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from airlines_dt VERSION AS OF 1 where Dest = 'SFO' 

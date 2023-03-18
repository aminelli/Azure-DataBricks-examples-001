# Databricks notebook source
# MAGIC %md
# MAGIC # ESTRAZIONE DATI

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/Rdatasets/

# COMMAND ----------

f = open('/dbfs/databricks-datasets/Rdatasets/README.md','r')
print(f.read())

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/Rdatasets/data-001/

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/Rdatasets/data-001/csv

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/Rdatasets/data-001/csv/ggplot2/

# COMMAND ----------

f = open('/dbfs/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv','r')
print(f.read())

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS diamonds;
# MAGIC 
# MAGIC CREATE TABLE diamonds USING CSV OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")

# COMMAND ----------

diamonds = (spark.read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
)

diamonds.write.format("delta").save("/mnt/delta/diamonds2")

# COMMAND ----------

# MAGIC %fs ls /mnt/delta

# COMMAND ----------

# MAGIC %fs ls /mnt/delta/diamonds2

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS diamonds2;
# MAGIC 
# MAGIC CREATE TABLE diamonds2 USING DELTA LOCATION "/mnt/delta/diamonds2"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM diamonds

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC select color, avg(price) as priceavg from diamonds group by color order by color

# Databricks notebook source
# MAGIC %md
# MAGIC # Antonio Minelli
# MAGIC ## Il mio primo notebook
# MAGIC ##### Titolo di livello 5
# MAGIC  - List down the default databricks-dataset
# MAGIC  - Identify the csv file which we want to read
# MAGIC  - Display few row in the csv file
# MAGIC  - Write the dataframe to a spark table
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC List all the default datasets that are available to us when we create a cluster

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/COVID

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/learning-spark-v2/

# COMMAND ----------

# read the mnm-dataset.csv file and display few records from the dataset.
sparkDF = spark.read.csv('/databricks-datasets/learning-spark-v2/mnm_dataset.csv', header="true", inferSchema="true")

# COMMAND ----------

display(sparkDF)

# COMMAND ----------

sparkDF.write.mode("overwrite").saveAsTable("MnmData")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MnmData;

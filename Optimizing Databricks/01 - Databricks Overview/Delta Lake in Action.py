# Databricks notebook source
# Creating a Spark dataframe
airlines = (spark.read
            .option("header",True)
            .option("inferSchema",True)
            .option("delimiter",",")
            .csv("dbfs:/databricks-datasets/airlines/part-00000")
)

# View the dataframe
display(airlines.limit(5))

# COMMAND ----------

# Write Spark dataframe as Delta
airlines.write.mode("overwrite").format("delta").save("dbfs:/airlines/")

# COMMAND ----------

# View the location where the data is written in Delta format
display(dbutils.fs.ls("dbfs:/airlines/"))

# COMMAND ----------

# Peek inside the _delta_log folder
display(dbutils.fs.ls("dbfs:/airlines/_delta_log/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create delta table `airlines_delta_table`
# MAGIC DROP TABLE IF EXISTS airlines_delta_table;
# MAGIC CREATE TABLE airlines_delta_table USING DELTA LOCATION "dbfs:/airlines/";

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Return count of delta table
# MAGIC SELECT COUNT(*) as count FROM airlines_delta_table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete transaction on delta table
# MAGIC DELETE FROM airlines_delta_table WHERE Month = '10'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update transaction on delta table
# MAGIC UPDATE airlines_delta_table SET Dest = 'San Francisco' WHERE Dest = 'SFO'

# COMMAND ----------

# View the location where the data is written in Delta format
display(dbutils.fs.ls("dbfs:/airlines/"))

# COMMAND ----------

# Peek inside the _delta_log folder
display(dbutils.fs.ls("dbfs:/airlines/_delta_log/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Time travel
# MAGIC DESCRIBE HISTORY airlines_delta_table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Return count of rows where Dest = 'SFO' in current version that is version 2
# MAGIC SELECT COUNT(*) FROM airlines_delta_table WHERE Dest = 'SFO'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Return count of rows where Dest = 'SFO' in version 1
# MAGIC SELECT COUNT(*) FROM airlines_delta_table VERSION AS OF 1 WHERE Dest = 'SFO'

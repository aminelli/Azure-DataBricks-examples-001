# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/structured-streaming/events/

# COMMAND ----------

from pyspark.sql.functions import *

# Define the schema for reading streaming
schema = "time STRING, action STRING"

# Creating a streaming dataframe 
stream_read = (spark
               .readStream
               .format("json")
               .schema(schema)
               .option("maxFilesPerTrigger", 1)
               .load("dbfs:/databricks-datasets/structured-streaming/events/")
)

# COMMAND ----------

# Performing transformation on streaming dataframe
stream_read = stream_read.withColumn("time",from_unixtime(col("time")))

# Display the streaming dataframe
display(stream_read)

# COMMAND ----------

# Write stream to Data Lake
# Make sure to replace the <mount_point> with the respective mount point
(stream_read
 .writeStream
 .format("delta")
 .option("checkpointLocation", "dbfs:/mnt/<mount_point>/write_stream_checkpointing")
 .start("dbfs:/mnt/<mount_point>/write_stream"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Make sure to replace the <mount_point> with the respective mount point
# MAGIC CREATE TABLE stream_delta_table
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/mnt/<mount_point>/write_stream'

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT COUNT(*) FROM stream_delta_table

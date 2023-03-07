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
query = (stream_read
 .writeStream
 .format("delta")
 .option("checkpointLocation", "dbfs:/write_stream_checkpointing")
 .start("dbfs:/write_stream"))

# COMMAND ----------

query.id

# COMMAND ----------

query.runId

# COMMAND ----------

query.name

# COMMAND ----------

query.explain()

# COMMAND ----------

query.stop()

# COMMAND ----------

query.awaitTermination(5)

# COMMAND ----------

query.exception()

# COMMAND ----------

query.recentProgress

# COMMAND ----------

query.lastProgress

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

# Trying to sort the streaming dataframe by time column
# Using a try-catch block to catch and print the exception
try:
  (display(stream_read.sort(col("time").desc())))
except Exception as e:
  print(e)

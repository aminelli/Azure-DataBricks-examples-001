# Databricks notebook source
from pyspark.sql.functions import *

# Define the schema for reading streaming
schema = "time STRING, action STRING"

# Creating a streaming dataframe 
stream_read = (spark
               .read
               .format("json")
               .schema(schema)
               .load("dbfs:/databricks-datasets/structured-streaming/events/")
)

# Display the first 10 records
display(stream_read.limit(10))

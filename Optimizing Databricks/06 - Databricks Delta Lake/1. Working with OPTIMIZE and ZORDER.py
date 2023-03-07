# Databricks notebook source
# MAGIC %md
# MAGIC ### Working with OPTIMIZE and ZORDER

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

manual_schema = StructType([
  StructField('Year',IntegerType(),True),
  StructField('Month',IntegerType(),True),
  StructField('DayofMonth',IntegerType(),True),
  StructField('DayOfWeek',IntegerType(),True),
  StructField('DepTime',StringType(),True),
  StructField('CRSDepTime',IntegerType(),True),
  StructField('ArrTime',StringType(),True),
  StructField('CRSArrTime',IntegerType(),True),
  StructField('UniqueCarrier',StringType(),True),
  StructField('FlightNum',IntegerType(),True),
  StructField('TailNum',StringType(),True),
  StructField('ActualElapsedTime',StringType(),True),
  StructField('CRSElapsedTime',StringType(),True),
  StructField('AirTime',StringType(),True),
  StructField('ArrDelay',StringType(),True),
  StructField('DepDelay',StringType(),True),
  StructField('Origin',StringType(),True),
  StructField('Dest',StringType(),True),
  StructField('Distance',StringType(),True),
  StructField('TaxiIn',StringType(),True),
  StructField('TaxiOut',StringType(),True),
  StructField('Cancelled',IntegerType(),True),
  StructField('CancellationCode',StringType(),True),
  StructField('Diverted',IntegerType(),True),
  StructField('CarrierDelay',StringType(),True),
  StructField('WeatherDelay',StringType(),True),
  StructField('NASDelay',StringType(),True),
  StructField('SecurityDelay',StringType(),True),
  StructField('LateAircraftDelay',StringType(),True)
])

# COMMAND ----------

# Read csv files to create Spark dataframe
airlines_1987_to_2008 = (
  spark
  .read
  .option("header",True)
  .option("delimiter",",")
  .schema(manual_schema)
  .csv("dbfs:/databricks-datasets/asa/airlines/*")
)

# View the dataframe
display(airlines_1987_to_2008)

# COMMAND ----------

# Write the dataframe to DBFS in the delta format 
airlines_1987_to_2008.write.format('delta').mode('overwrite').save('dbfs:/delta_lake_optimizations/airlines_1987_to_2008')

# COMMAND ----------

# MAGIC %fs ls dbfs:/delta_lake_optimizations/airlines_1987_to_2008

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE airlines_unoptimized
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/delta_lake_optimizations/airlines_1987_to_2008'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE airlines_unoptimized
# MAGIC ZORDER BY (Year, Origin)

# COMMAND ----------

# MAGIC %fs ls dbfs:/delta_lake_optimizations/airlines_1987_to_2008

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY airlines_unoptimized

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM airlines_unoptimized VERSION AS OF 0 WHERE Year LIKE '19%' AND Origin IN ('SYR','JFK','LAX')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM airlines_unoptimized VERSION AS OF 1 WHERE Year LIKE '19%' AND Origin IN ('SYR','JFK','LAX')

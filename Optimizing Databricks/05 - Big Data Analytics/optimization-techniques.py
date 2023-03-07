# Databricks notebook source
# MAGIC %md 
# MAGIC ### Restricting use of collect() method

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# Read csv files to create Spark dataframe
airlines_1987_to_2008 = (
  spark
  .read
  .option("header",True)
  .option("delimiter",",")
  .option("inferSchema",True)
  .csv("dbfs:/databricks-datasets/asa/airlines/*")
)
# View the dataframe
display(airlines_1987_to_2008)

# COMMAND ----------

# Using the collect function
airlines_1987_to_2008.select('Year').distinct().collect()

# COMMAND ----------

# Run with caution
airlines_1987_to_2008.collect()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Limiting use of inferSchema

# COMMAND ----------

# Read csv files to create Spark dataframe
airlines_1987_to_2008 = (
  spark
  .read
  .option("header",True)
  .option("delimiter",",")
  .option("inferSchema",True)
  .csv("dbfs:/databricks-datasets/asa/airlines/*")
)
# View the dataframe
display(airlines_1987_to_2008)

# COMMAND ----------

from pyspark.sql.types import *

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

# MAGIC %md 
# MAGIC ### Pandas vs Koalas

# COMMAND ----------

from pyspark.sql.functions import *

# Read csv files to create Spark dataframe
airlines_1987_to_2008 = (
  spark
  .read
  .option("header",True)
  .option("delimiter",",")
  .option("inferSchema",True)
  .csv("dbfs:/databricks-datasets/asa/airlines/*")
)
# View the dataframe
display(airlines_1987_to_2008)

# COMMAND ----------

airlines_1987_to_2008.count()

# COMMAND ----------

# Converting spark dataframe to Pandas
pandas_df = airlines_1987_to_2008.toPandas()

# COMMAND ----------

# Converting spark dataframe to Koalas
import databricks.koalas as ks
koalas_df = airlines_1987_to_2008.to_koalas()

# COMMAND ----------

# Return first 5 records
koalas_df.head()

# COMMAND ----------

koalas_df.describe()

# COMMAND ----------

  koalas_df.groupby('Year').count()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Using built-in spark functions

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

# Using higher order functions

built_in_df = (airlines_1987_to_2008
        .withColumn('Origin',
                    when(col('Origin') == 'ATL',regexp_replace(col('Origin'),'ATL','Hartsfield-Jackson International Airport'))
                    .when(col('Origin') == 'DFW',regexp_replace(col('Origin'),'DFW','Dallas/Fort Worth International Airport'))
                    .when(col('Origin') == 'DEN',regexp_replace(col('Origin'),'DEN','Denver International Airport'))
                    .when(col('Origin') == 'ORD',regexp_replace(col('Origin'),'ORD','O Hare International Airport'))
                    .when(col('Origin') == 'LAX',regexp_replace(col('Origin'),'LAX','Los Angeles International Airport'))
                    .when(col('Origin') == 'CLT',regexp_replace(col('Origin'),'CLT','Charlotte Douglas International Airport'))
                    .when(col('Origin') == 'LAS',regexp_replace(col('Origin'),'LAS','McCarran International Airport'))
                    .when(col('Origin') == 'PHX',regexp_replace(col('Origin'),'PHX','Phoenix Sky Harbor International Airport'))
                    .when(col('Origin') == 'MCO',regexp_replace(col('Origin'),'MCO','Orlando International Airport'))
                    .when(col('Origin') == 'SEA',regexp_replace(col('Origin'),'SEA','Seattle–Tacoma International Airport'))
                    .when(col('Origin') == 'MIA',regexp_replace(col('Origin'),'MIA','Miami International Airport'))
                    .when(col('Origin') == 'IAH',regexp_replace(col('Origin'),'IAH','George Bush Intercontinental Airport'))
                    .when(col('Origin') == 'JFK',regexp_replace(col('Origin'),'JFK','John F. Kennedy International Airport'))
                    .otherwise(None)
                   )
       )

# Write the dataframe to DBFS
built_in_df.write.format('delta').mode('overwrite').save('dbfs:/built_in_df')

# COMMAND ----------

import re

# Creating UDF
airports = {'ATL':'Hartsfield-Jackson International Airport','DFW':'Dallas/Fort Worth International Airport','DEN':'Denver International Airport','ORD':'O Hare International Airport','LAX':'Los Angeles International Airport','CLT':'Hartsfield-Jackson International Airport','LAS':'McCarran International Airport','PHX':'Phoenix Sky Harbor International Airport','MCO':'Orlando International Airport','SEA':'Seattle–Tacoma International Airport','MIA':'Miami International Airport','IAH':'George Bush Intercontinental Airport','JFK':'John F. Kennedy International Airport'}

def replace_origin(origin):
  
  for key in airports:
    
    if origin == key:
      replaced = re.sub(key,airports[key],origin)
      return replaced
  
  return None

replace_origin_udf = udf(replace_origin,StringType())

# Creating another dataframe and using UDF
udf_df = (airlines_1987_to_2008
        .withColumn('Origin',replace_origin_udf('Origin'))
       )

# Writing dataframe to 
udf_df.write.format('delta').mode('overwrite').save('dbfs:/udf_df')

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Columns Predicate Pushdown

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

# Write the dataframe to DBFS
airlines_1987_to_2008.write.format('parquet').mode('overwrite').save('dbfs:/columns_predicate_pushdown')

# COMMAND ----------

# Write the dataframe to DBFS
airlines_1987_to_2008.write.format('parquet').mode('overwrite').partitionBy('DayOfWeek').save('dbfs:/columns_predicate_pushdown_partitioned')

# COMMAND ----------

without_cpp_df = (spark
               .read
               .format('parquet')
               .load('dbfs:/columns_predicate_pushdown')
               .filter(col('DayOfWeek') == 7)
)
without_cpp_df.count()

# COMMAND ----------

with_cpp_df = (spark
               .read
               .format('parquet')
               .load('dbfs:/columns_predicate_pushdown_partitioned')
               .filter(col('DayOfWeek') == 7)
)
with_cpp_df.count()

# COMMAND ----------

# MAGIC %fs ls dbfs:/columns_predicate_pushdown_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partitioning strategies

# COMMAND ----------

# MAGIC %scala
# MAGIC // Read csv files to create Spark dataframe
# MAGIC val airlines_1987_to_2008 = (
# MAGIC   spark
# MAGIC   .read
# MAGIC   .option("header",true)
# MAGIC   .option("delimiter",",")
# MAGIC   .option("inferSchema",true)
# MAGIC   .csv("dbfs:/databricks-datasets/asa/airlines/*")
# MAGIC )
# MAGIC // View the dataframe
# MAGIC display(airlines_1987_to_2008)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Check number of partitions in spark dataframe
# MAGIC airlines_1987_to_2008.rdd.getNumPartitions

# COMMAND ----------

# MAGIC %scala
# MAGIC display(airlines_1987_to_2008
# MAGIC   .rdd
# MAGIC   .mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
# MAGIC   .toDF("partition_number","number_of_records")
# MAGIC   )

# COMMAND ----------

spark.sparkContext.defaultParallelism

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

# Read csv files to create Spark dataframe
airlines_1987_to_2008 = (
  spark
  .read
  .option("header",True)
  .option("delimiter",",")
  .schema(manual_schema)
  .csv("dbfs:/databricks-datasets/asa/airlines/*")
)

# Partition and write the dataframe
(airlines_1987_to_2008.write
 .format('delta')
 .mode('overwrite')
 .partitionBy('Year','Month')
 .save('dbfs:/airlines_1987_to_2008_partitioned')
)

# COMMAND ----------

# MAGIC %fs ls dbfs:/airlines_1987_to_2008_partitioned

# COMMAND ----------

# MAGIC %fs ls dbfs:/airlines_1987_to_2008_partitioned/Year=1987/

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE airlines
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/airlines_1987_to_2008_partitioned'

# COMMAND ----------

# MAGIC %sql DESCRIBE DETAIL airlines

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bucketing

# COMMAND ----------

from pyspark.sql.functions import *
# Create a sample dataframe
sample_df = spark.range(start = 1, end = 100000001, step = 1, numPartitions = 100).select(col("id").alias("key"),rand(seed = 12).alias("value"))
display(sample_df)

# COMMAND ----------

# Check number of spark partitions
sample_df.rdd.getNumPartitions()

# COMMAND ----------

# Check count of dataframe
sample_df.count()

# COMMAND ----------

# Write as bucketed table
(sample_df
 .write
 .format('parquet')
 .mode('overwrite')
 .bucketBy(100, "key")
 .sortBy("value")
 .saveAsTable('bucketed_table')
)

# COMMAND ----------

# MAGIC %sql DESCRIBE TABLE EXTENDED bucketed_table

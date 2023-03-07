# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

# Read csv files to create Spark dataframe
airlines_1987_to_2008 = (spark.read.option("header",True).option("delimiter",",").option("inferSchema",True).csv("dbfs:/databricks-datasets/asa/airlines/*"))
# View the dataframe
display(airlines_1987_to_2008)

# COMMAND ----------

# Return count of records in dataframe
airlines_1987_to_2008.count()

# COMMAND ----------

# Select the columns - Origin, Dest and Distance
display(airlines_1987_to_2008.select("Origin","Dest","Distance"))

# COMMAND ----------

# Import the class to use functions
from pyspark.sql.functions import *
# Filtering data with 'where' method
display(airlines_1987_to_2008.where(col("Year") == "2001"))

# COMMAND ----------

# Chaining example and using 'filter' method
display(airlines_1987_to_2008.select("Year","Origin","Dest").filter(col("Year") == "2001"))

# COMMAND ----------

# Create a new dataframe exluding dropped column
airlines_1987_to_2008_drop_DayofMonth = airlines_1987_to_2008.drop("DayofMonth")
# Display the new dataframe
display(airlines_1987_to_2008_drop_DayofMonth)

# COMMAND ----------

# Create column 'Weekend' and a new dataframe
AddNewColumn = (airlines_1987_to_2008
                .select('DayOfWeek')
                .withColumn("Weekend",col("DayOfWeek").isin(6,7)))
# Display the new dataframe
display(AddNewColumn)

# COMMAND ----------

# Cast ActualElapsedTime column to integer
AddNewColumn = airlines_1987_to_2008.withColumn("ActualElapsedTime",col("ActualElapsedTime").cast("int"))
# Display the dataframe
display(AddNewColumn)

# COMMAND ----------

airlines_1987_to_2008.printSchema()

# COMMAND ----------

AddNewColumn.printSchema()

# COMMAND ----------

# Rename 'DepTime' to 'DepartureTime'
RenameColumn = AddNewColumn.withColumnRenamed("DepTime","DepartureTime")
# Display the resulting dataframe
display(RenameColumn)

# COMMAND ----------

# Drop rows based on Year and Month
DropRows = airlines_1987_to_2008.dropDuplicates(["Year","Month"])
# Display the dataframe
display(DropRows)

# COMMAND ----------

display(airlines_1987_to_2008.limit(10))

# COMMAND ----------

# Sort by descending order using sort()
display(airlines_1987_to_2008
        .select("Year")
        .dropDuplicates()
        .sort(col("Year").desc()))

# COMMAND ----------

# Sorting by ascending order using sort()
display(airlines_1987_to_2008
        .select("Year")
        .dropDuplicates()
        .sort(col("Year").asc()))

# COMMAND ----------

# Sort by descending order using orderBy()
display(airlines_1987_to_2008
        .select("Year")
        .dropDuplicates()
        .orderBy(col("Year").desc()))

# COMMAND ----------

# Sorting by ascending order using orderBy()
display(airlines_1987_to_2008
        .select("Year")
        .dropDuplicates().orderBy(col("Year").asc()))

# COMMAND ----------

# Grouping data and returning count
display(airlines_1987_to_2008.groupBy("Origin").count())

# COMMAND ----------

# Grouping data and finding maximum value for each 'Dest'
display(airlines_1987_to_2008
        .select(col("Dest"),col("ArrDelay").cast("int").alias("ArrDelay"))
        .groupBy("Dest").max("ArrDelay"))

# COMMAND ----------

# Visualizing data!
display(airlines_1987_to_2008
        .select(col("Dest"),col("ArrDelay").cast("int").alias("ArrDelay"))
        .groupBy("Dest").max("ArrDelay")
        .limit(10))

# COMMAND ----------

# Write data in Delta format
AddNewColumn.write.format("delta").option("path","/mnt/analyticsdatalakepackt_mnt/transformed_data/").save()

# COMMAND ----------

# MAGIC %fs ls /mnt/analyticsdatalakepackt_mnt/transformed_data/

# COMMAND ----------

# MAGIC %fs ls /mnt/analyticsdatalakepackt_mnt

# COMMAND ----------

# MAGIC %fs mounts

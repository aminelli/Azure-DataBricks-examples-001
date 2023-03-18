# Databricks notebook source
# MAGIC %md
# MAGIC # NEW DB

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/samples/lending_club/

# COMMAND ----------

f = open("/dbfs/databricks-datasets/samples/lending_club/readme.md","r")
print(f.read())

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE DATABASE lending;
# MAGIC USE lending;

# COMMAND ----------

lending_data_frame = spark.read.parquet("dbfs:/databricks-datasets/samples/lending_club/parquet/")

# COMMAND ----------

display(lending_data_frame.limit(100))

# COMMAND ----------

from pyspark.sql.functions import *

lending_data_frame_2 = lending_data_frame.select("loan_status", "int_rate", "revol_util", "issue_d", "earliest_cr_line", "emp_length", "verification_status", "total_pymnt", "loan_amnt", "grade", "annual_inc", "dti", "addr_state", "term", "home_ownership", "purpose", "application_type", "delinq_2yrs", "total_acc")

lending_data_frame_3 = (lending_data_frame_2
    .withColumn("int_rate", regexp_replace("int_rate","%","").cast('float'))
    .withColumn("revol_util", regexp_replace("revol_util","%","").cast('float'))
    .withColumn("issue_year", substring(col("issue_d"), 5, 4).cast('double'))
    .withColumn("earliest_year", substring(col("earliest_cr_line"), 5, 4).cast('double'))
)

lending_data_frame_4 = lending_data_frame_3.withColumn("len_year", col("issue_year") - col("earliest_year"))

lending_data_frame_5 = lending_data_frame_4.withColumn("calc_diff", round(col("total_pymnt") - col("loan_amnt"), 2))

# COMMAND ----------

lending_data_frame_5.write.format("delta").save("dbfs:/corso/lending_club_delta")

# COMMAND ----------

# MAGIC %fs ls /corso/lending_club_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE lending_club_data
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/corso/lending_club_delta"

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lending_club_data LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC Select grade as GRADE, SUM(loan_amnt) as TotalAmount
# MAGIC FROM lending_club_data 
# MAGIC WHERE grade IS NOT NULL
# MAGIC group by grade
# MAGIC order by grade

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT addr_state, count(*) as loans
# MAGIC FROM lending_club_data
# MAGIC WHERE 
# MAGIC addr_state != 'debt_consolidation' 
# MAGIC AND
# MAGIC addr_state != '531xx' 
# MAGIC AND
# MAGIC addr_state IS NOT NULL
# MAGIC GROUP BY addr_state

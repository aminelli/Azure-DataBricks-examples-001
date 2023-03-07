# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create a new Hive database
# MAGIC CREATE DATABASE packt_databricks;
# MAGIC USE packt_databricks;

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/samples/lending_club/parquet/

# COMMAND ----------

# Extract or read the data
lending_club = spark.read.parquet("dbfs:/databricks-datasets/samples/lending_club/parquet/")

# COMMAND ----------

# Display top 10 rows
display(lending_club.limit(10))

# COMMAND ----------

from pyspark.sql.functions import *

# Selecting the columns that we are interested in
lending_club = lending_club.select("loan_status", "int_rate", "revol_util", "issue_d", "earliest_cr_line", "emp_length", "verification_status", "total_pymnt", "loan_amnt", "grade", "annual_inc", "dti", "addr_state", "term", "home_ownership", "purpose", "application_type", "delinq_2yrs", "total_acc")

# Transforming string columns into numeric columns using 'regexp_replace' and 'substring' functions
lending_club = (lending_club.withColumn('int_rate', regexp_replace('int_rate', '%', '').cast('float'))
                       .withColumn('revol_util', regexp_replace('revol_util', '%', '').cast('float'))
                       .withColumn('issue_year',  substring(col("issue_d"), 5, 4).cast('double') )
                       .withColumn('earliest_year', substring(col("earliest_cr_line"), 5, 4).cast('double')))

# Creating the 'credit_length' column
lending_club = lending_club.withColumn('credit_length', col("issue_year") - col("earliest_year"))

# Creating the 'net' column, the total amount of money earned or lost per loan
lending_club = lending_club.withColumn('net', round(col("total_pymnt") - col("loan_amnt"), 2))

# COMMAND ----------

# Write to Azure Data Lake in delta format
lending_club.write.format("delta").save("dbfs:/mnt/analyticsdatalakepackt_mnt/lending_club_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create delta table in packt_databricks database
# MAGIC CREATE TABLE lending_club_delta
# MAGIC USING DELTA 
# MAGIC LOCATION 'dbfs:/mnt/analyticsdatalakepackt_mnt/lending_club_delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View the top rows from delta table
# MAGIC SELECT * FROM lending_club_delta LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total loan amount issued for each grade
# MAGIC SELECT grade AS `Grade`, SUM(loan_amnt) AS `Total Amount Issued`
# MAGIC FROM lending_club_delta
# MAGIC WHERE grade IS NOT NULL
# MAGIC GROUP BY grade
# MAGIC ORDER BY grade

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Statewise distribution of loans
# MAGIC SELECT addr_state, COUNT(*) AS `Number of loans`
# MAGIC FROM lending_club_delta
# MAGIC WHERE addr_state != 'debt_consolidation' 
# MAGIC   AND addr_state != '531xx' 
# MAGIC   AND addr_state IS NOT NULL 
# MAGIC GROUP BY addr_state

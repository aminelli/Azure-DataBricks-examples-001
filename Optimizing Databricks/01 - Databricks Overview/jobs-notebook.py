# Databricks notebook source
# MAGIC %sql
# MAGIC -- Creating a delta table and storing data in DBFS
# MAGIC -- Our table's name is 'insurance_claims' and has four columns
# MAGIC CREATE OR REPLACE TABLE insurance_claims (
# MAGIC   user_id INT NOT NULL,
# MAGIC   city STRING NOT NULL,
# MAGIC   country STRING NOT NULL,
# MAGIC   amount INT NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/tmp/insurance_claims';
# MAGIC 
# MAGIC -- Insert a new record in 'insurance_claims'
# MAGIC INSERT INTO insurance_claims (user_id, city, country, amount)
# MAGIC VALUES (100, 'Mumbai', 'India', 200000);
# MAGIC 
# MAGIC -- Insert a new record in 'insurance_claims'
# MAGIC INSERT INTO insurance_claims (user_id, city, country, amount)
# MAGIC VALUES (101, 'Delhi', 'India', 400000);
# MAGIC 
# MAGIC -- Insert a new record in 'insurance_claims'
# MAGIC INSERT INTO insurance_claims (user_id, city, country, amount)
# MAGIC VALUES (102, 'Chennai', 'India', 100000);
# MAGIC 
# MAGIC -- Insert a new record in 'insurance_claims'
# MAGIC INSERT INTO insurance_claims (user_id, city, country, amount)
# MAGIC VALUES (103, 'Bengaluru', 'India', 700000);

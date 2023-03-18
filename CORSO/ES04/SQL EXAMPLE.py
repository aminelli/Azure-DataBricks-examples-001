# Databricks notebook source
# MAGIC %md
# MAGIC # SQL EXAMPLES

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE amounts (
# MAGIC   user_id INT    NOT NULL,
# MAGIC   city    STRING NOT NULL,
# MAGIC   country STRING NOT NULL,
# MAGIC   amount  INT    NOT NULL  
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/corso/amounts";
# MAGIC 
# MAGIC INSERT INTO amounts (user_id, city, country, amount)
# MAGIC VALUES (100, "Milano", "Italia", 20000);
# MAGIC 
# MAGIC INSERT INTO amounts (user_id, city, country, amount)
# MAGIC VALUES (200, "Roma", "Italia", 50000);
# MAGIC 
# MAGIC INSERT INTO amounts (user_id, city, country, amount)
# MAGIC VALUES (300, "Londra", "Gran Bretagna", 6000);
# MAGIC 
# MAGIC INSERT INTO amounts (user_id, city, country, amount)
# MAGIC VALUES (301, "Parigi", "Francia", 100);

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from amounts

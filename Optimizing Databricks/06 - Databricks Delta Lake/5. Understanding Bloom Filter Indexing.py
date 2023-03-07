# Databricks notebook source
# MAGIC %md
# MAGIC ### Bloom Filter Indexing

# COMMAND ----------

# Check if bloom filter index is enabled or not
spark.conf.get('spark.databricks.io.skipping.bloomFilter.enabled')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bloom_filter_test (
# MAGIC   id BIGINT NOT NULL,
# MAGIC   hashed_col_a STRING NOT NULL,
# MAGIC   hashed_col_b STRING NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/bloom_filter_test'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE BLOOMFILTER INDEX
# MAGIC ON TABLE bloom_filter_test
# MAGIC FOR COLUMNS(hashed_col_a OPTIONS (fpp=0.1, numItems=1000000))

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte (
# MAGIC   SELECT
# MAGIC     monotonically_increasing_id() AS id,
# MAGIC     sha(CAST(id AS string)) AS hashed_col_a,
# MAGIC     sha(CAST(id AS string)) AS hashed_col_b
# MAGIC   FROM 
# MAGIC     RANGE(0, 1000000, 1, 100)  -- start, end, step, numPartitions
# MAGIC )
# MAGIC INSERT INTO bloom_filter_test 
# MAGIC SELECT id, hashed_col_a, hashed_col_b 
# MAGIC FROM cte

# COMMAND ----------

# MAGIC %sql SELECT * FROM bloom_filter_test

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM bloom_filter_test 
# MAGIC WHERE hashed_col_a IN ('79816ecb0a75e0b29ec93a3e4845cf4f0b5d4d4d','3554dce55f341edd431fc711f6816673f081452d','cf2f328d24859d56d55d2b610b12525e60b21895')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM bloom_filter_test 
# MAGIC WHERE hashed_col_b IN ('79816ecb0a75e0b29ec93a3e4845cf4f0b5d4d4d','3554dce55f341edd431fc711f6816673f081452d','cf2f328d24859d56d55d2b610b12525e60b21895')

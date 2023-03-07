# Databricks notebook source
# MAGIC %md
# MAGIC ### Environment Setup

# COMMAND ----------

# Importing necessary libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
import mlflow
import mlflow.spark
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import RFormula
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from mlflow.tracking import MlflowClient

# COMMAND ----------

# MAGIC %fs head dbfs:/databricks-datasets/bikeSharing/README.md

# COMMAND ----------

bike_sharing_schema = StructType([
  StructField('instant',IntegerType(),False),
  StructField('dteday',StringType(),False),
  StructField('season',IntegerType(),False),
  StructField('yr',IntegerType(),False),
  StructField('mnth',IntegerType(),False),
  StructField('hr',IntegerType(),False),
  StructField('holiday',IntegerType(),False),
  StructField('weekday',IntegerType(),False),
  StructField('workingday',IntegerType(),False),
  StructField('weathersit',IntegerType(),False),
  StructField('temp',DoubleType(),False),
  StructField('atemp',DoubleType(),False),
  StructField('hum',DoubleType(),False),
  StructField('windspeed',DoubleType(),False),
  StructField('casual',IntegerType(),False),
  StructField('registered',IntegerType(),False),
  StructField('cnt',IntegerType(),False)
])

# Read the CSV file
bike_sharing = spark.read.option("header",True).schema(bike_sharing_schema).csv("dbfs:/databricks-datasets/bikeSharing/data-001/hour.csv")
# Display the dataframe
display(bike_sharing)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploring Data

# COMMAND ----------

# Return count of dataset
bike_sharing.count()

# COMMAND ----------

display(bike_sharing.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Machine learning

# COMMAND ----------

# Split the dataframe into test and train
(trainDF, testDF) = bike_sharing.randomSplit([.8, .2], seed=42)
# Print the dataset in training and testing dataframe
print("# of records in training dataframe:",trainDF.count())
print("# of records in testing dataset:",testDF.count())

# COMMAND ----------

def ml_mlflow_register(run_name,formula,label,metric):
  """
  This function trains a linear regression model and registers the model in the MLflow registery
  Argument:
  1. run_name - Name of the run
  2. formula - Formula for feature selection using RFormula in the form 'label ~ feature1 + feature2'
  3. label - The column to predict
  4. metric - The metric to evaluate a model on
  """
  with mlflow.start_run(run_name=run_name) as run:
    # Define pipeline
    r_formula = RFormula(formula=formula, featuresCol="features", labelCol=label, handleInvalid="skip")
    linear_reg = LinearRegression(featuresCol="features", labelCol=label)
    pipeline = Pipeline(stages=[r_formula, linear_reg])
    pipelineModel = pipeline.fit(trainDF)
    
    # Log tags
    mlflow.set_tag("Owner", "ML Engineer I")
    
    # Log parameters
    mlflow.log_param("label", label)

    # Log model
    mlflow.spark.log_model(pipelineModel, "model")

    # Evaluate predictions
    predictions = pipelineModel.transform(testDF)
    regression_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol=label, metricName=metric)
    rmse = regression_evaluator.evaluate(predictions)

    # Log metrics
    mlflow.log_metric(metric, rmse)

# COMMAND ----------

# First experiment run
ml_mlflow_register("first_temp_hum","cnt ~ temp + hum","cnt","rmse")

# COMMAND ----------

# Second experiment run
ml_mlflow_register("second_all","cnt ~ season + yr + mnth + hr + holiday + weekday + workingday + weathersit + temp + atemp + hum + windspeed","cnt","rmse")

# COMMAND ----------

# Third experiment run
ml_mlflow_register("second_only_weather","cnt ~ weathersit + temp + atemp + hum + windspeed","cnt","rmse")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying past runs

# COMMAND ----------

# Create a view to store metadata of all experiment runs
mlflow_client_df = spark.read.format("mlflow-experiment").load()
mlflow_client_df.createOrReplaceTempView("my_experiment_runs")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View all runs
# MAGIC SELECT * FROM my_experiment_runs

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View experiment_id, run_id, start_time, end_time, metrics, artifcat_uri
# MAGIC SELECT experiment_id, run_id, start_time, end_time, metrics.rmse as rmse, artifact_uri FROM my_experiment_runs

# COMMAND ----------

# To instantiate a model
selected_model_uri = ""
model_to_use = mlflow_spark.load_model(selected_model_uri)

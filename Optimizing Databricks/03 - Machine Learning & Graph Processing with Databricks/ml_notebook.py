# Databricks notebook source
# MAGIC %md
# MAGIC # Environment Setup

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://data.sfgov.org/api/views/nuek-vuh3/rows.csv?accessType=DOWNLOAD -P /dbfs/firedata/dataset/

# COMMAND ----------

# MAGIC %fs
# MAGIC mv /firedata/dataset/rows.csv?accessType=DOWNLOAD /firedata/dataset/Fire_Incidents.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://data.sfgov.org/api/views/wr8u-xric/rows.csv?accessType=DOWNLOAD -P /dbfs/firedata/dataset/

# COMMAND ----------

# MAGIC %fs
# MAGIC mv /firedata/dataset/rows.csv?accessType=DOWNLOAD /firedata/dataset/Non_Medical_Incidents.csv

# COMMAND ----------

# MAGIC %fs ls dbfs:/firedata/dataset/

# COMMAND ----------

# MAGIC %md
# MAGIC # Problem Statement

# COMMAND ----------

# MAGIC %md 
# MAGIC The **DataSF project** was launched in 2009 and contains hundreds of datasets from the city of **San Francisco**. Open government data has the potential to increase the quality of life for residents, create more efficient government services, better public decisions, and even new local businesses and services.
# MAGIC 
# MAGIC The dataset comes from **SF's fire department** and conatins data about all the fire unit's responses to calls. Each record includes the call number, incident number, address, unit identifier, call type, and disposition. All relevant time intervals are also included. Because this dataset is based on responses, and since most calls involved multiple units, there are multiple records for each call number. Addresses are associated with a block number, intersection or call box, not a specific address.
# MAGIC 
# MAGIC The question that we will try to answer in this analysis is,
# MAGIC 
# MAGIC **How has the Christmas and New Year holidays affected demand for Firefighters?**

# COMMAND ----------

# Inferring the schema manually
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

manual_schema = StructType([StructField('CallNumber', IntegerType(), True),
                     StructField('UnitID', StringType(), True),
                     StructField('IncidentNumber', IntegerType(), True),
                     StructField('CallType', StringType(), True),                  
                     StructField('CallDate', StringType(), True),       
                     StructField('WatchDate', StringType(), True),       
                     StructField('ReceivedDtTm', StringType(), True),       
                     StructField('EntryDtTm', StringType(), True),       
                     StructField('DispatchDtTm', StringType(), True),       
                     StructField('ResponseDtTm', StringType(), True),       
                     StructField('OnSceneDtTm', StringType(), True),       
                     StructField('TransportDtTm', StringType(), True),                  
                     StructField('HospitalDtTm', StringType(), True),       
                     StructField('CallFinalDisposition', StringType(), True),       
                     StructField('AvailableDtTm', StringType(), True),       
                     StructField('Address', StringType(), True),       
                     StructField('City', StringType(), True),       
                     StructField('ZipcodeofIncident', IntegerType(), True),       
                     StructField('Battalion', StringType(), True),                 
                     StructField('StationArea', StringType(), True),       
                     StructField('Box', StringType(), True),       
                     StructField('OriginalPriority', StringType(), True),       
                     StructField('Priority', StringType(), True),       
                     StructField('FinalPriority', IntegerType(), True),       
                     StructField('ALSUnit', BooleanType(), True),       
                     StructField('CallTypeGroup', StringType(), True),
                     StructField('NumberofAlarms', IntegerType(), True),
                     StructField('UnitType', StringType(), True),
                     StructField('Unitsequenceincalldispatch', IntegerType(), True),
                     StructField('FirePreventionDistrict', StringType(), True),
                     StructField('SupervisorDistrict', StringType(), True),
                     StructField('NeighborhoodDistrict', StringType(), True),
                     StructField('Location', StringType(), True),
                     StructField('RowID', StringType(), True)])

# COMMAND ----------

# Read the csv file as Spark dataframe but this time using the manuallu defined schema 
fire_incidents = (spark.read
                 .option("header",True)
                 .schema(manual_schema) # Use the schema defined in previous cell
                 .csv("dbfs:/firedata/dataset/Fire_Incidents.csv")
                 )

# COMMAND ----------

# Displaying first 10 rows
display(fire_incidents.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # Exploratory Data Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q1. How many different types of calls were made to the Fire Department?

# COMMAND ----------

# Display the different call type categories (type of incident) and their count
display(fire_incidents.groupBy("CallType").count())

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Q2. How many years of fire service calls is present in the data?

# COMMAND ----------

fire_incidents.printSchema()

# COMMAND ----------

# Overcome SparkUpgradeException due to differences between Spark 2.0 and 3.0
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

# Converting the string datetime column to timestamp 

from pyspark.sql.functions import *

from_pattern1 = 'MM/dd/yyyy'
from_pattern2 = 'MM/dd/yyyy hh:mm:ss aa'

fire_incidents = (fire_incidents
  .withColumn('CallDate', unix_timestamp(fire_incidents['CallDate'], from_pattern1).cast("timestamp"))
  .withColumn('WatchDate', unix_timestamp(fire_incidents['WatchDate'], from_pattern1).cast("timestamp"))
  .withColumn('ReceivedDtTm', unix_timestamp(fire_incidents['ReceivedDtTm'], from_pattern2).cast("timestamp"))
  .withColumn('EntryDtTm', unix_timestamp(fire_incidents['EntryDtTm'], from_pattern2).cast("timestamp"))
  .withColumn('DispatchDtTm', unix_timestamp(fire_incidents['DispatchDtTm'], from_pattern2).cast("timestamp"))
  .withColumn('ResponseDtTm', unix_timestamp(fire_incidents['ResponseDtTm'], from_pattern2).cast("timestamp"))
  .withColumn('OnSceneDtTm', unix_timestamp(fire_incidents['OnSceneDtTm'], from_pattern2).cast("timestamp"))
  .withColumn('TransportDtTm', unix_timestamp(fire_incidents['TransportDtTm'], from_pattern2).cast("timestamp"))
  .withColumn('HospitalDtTm', unix_timestamp(fire_incidents['HospitalDtTm'], from_pattern2).cast("timestamp"))
  .withColumn('AvailableDtTm', unix_timestamp(fire_incidents['AvailableDtTm'], from_pattern2).cast("timestamp")))

# COMMAND ----------

# Displaying first 5 rows
display(fire_incidents.limit(5))

# COMMAND ----------

# Years worth of data present with us!
display(fire_incidents.select(year("CallDate").alias('Year')).distinct())

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Q3. How many service calls were logged in between 25th December to 31st December, 2019?

# COMMAND ----------

# Number of service calls logged in between 25th Dec. to 31st Dec. 2019
fire_incidents.filter((year("CallDate") == 2019) & (month("CallDate") == 12) & (dayofmonth("CallDate") >= 25)).count()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Q4. What is the distribution of service calls made between 1st December, 2019 to 1st January 2020?

# COMMAND ----------

# Line plot to showcase trend of service calls made between 1st Dec. 2019 to 1st Jan. 2020
display(fire_incidents
        .filter((col("CallDate") >= '2019-12-01') & (col("CallDate") < '2020-01-02'))
        .groupBy(col("CallDate").alias("Date"))
        .count()
        .orderBy(col("Date")))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Q5. What is the number of service calls made between 25th Dec. to 31st Dec. for the past 20 years? (Trend)

# COMMAND ----------

# Number of service calls logged in between 25th Dec. to 31st Dec. between 2000 and 2020
display(fire_incidents
        .filter((month("CallDate") == 12) & (dayofmonth("CallDate") >= 25))
        .groupBy(year('CallDate').alias('Year'))
        .count()
        .orderBy('Year'))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Q6. Which neighborhood generated the most calls last year?

# COMMAND ----------

# Create view to run the SQL syntax
fire_incidents.createOrReplaceTempView('fire_incidents_view')

# COMMAND ----------

spark.sql("""SELECT NeighborhoodDistrict, COUNT(*) AS Count
FROM fire_incidents_view
WHERE year(CallDate) = 2019
GROUP BY NeighborhoodDistrict
ORDER BY COUNT(1) DESC""")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Q7. What was the primary non-medical reason most people called the fire department from Tenderloin last year during the Christmas holidays?

# COMMAND ----------

# Read the new csv file and rename column with spaces
non_medical_incidents = (spark.read
                         .option("header",True)
                         .option("inferSchema", True)
                         .csv("dbfs:/firedata/dataset/Non_Medical_Incidents.csv")
                         .withColumnRenamed("Incident Number","IncidentNumber")
                         .withColumnRenamed("Exposure Number","ExposureNumber")
                         .withColumnRenamed("Incident Date","IncidentDate")
                         .withColumnRenamed("Call Number","CallNumber")
                         .withColumnRenamed("Alarm DtTm","AlarmDtTm")
                         .withColumnRenamed("Arrival DtTm","ArrivalDtTm")
                         .withColumnRenamed("Close DtTm","CloseDtTm")
                         .withColumnRenamed("Station Area","StationArea")
                         .withColumnRenamed("Suppression Units","SuppressionUnits")
                         .withColumnRenamed("Suppression Personnel","SuppressionPersonnel")
                         .withColumnRenamed("EMS Units","EMSUnits")
                         .withColumnRenamed("EMS Personnel","EMSPersonnel")
                         .withColumnRenamed("Other Units","OtherUnits")
                         .withColumnRenamed("Other Personnel","OtherPersonnel")
                         .withColumnRenamed("First Unit On Scene","FirstUnitOnScene")
                         .withColumnRenamed("Estimated Property Loss","EstimatedPropertyLoss")
                         .withColumnRenamed("Estimated Contents Loss","EstimatedContentsLoss")
                         .withColumnRenamed("Fire Fatalities","FireFatalities")
                         .withColumnRenamed("Fire Injuries","FireInjuries")
                         .withColumnRenamed("Civilian Fatalities","CivilianFatalities")
                         .withColumnRenamed("Civilian Injuries","CivilianInjuries")
                         .withColumnRenamed("Number of Alarms","NumberofAlarms")
                         .withColumnRenamed("Primary Situation","PrimarySituation")
                         .withColumnRenamed("Mutual Aid","MutualAid")
                         .withColumnRenamed("Action Taken Primary","ActionTakenPrimary")
                         .withColumnRenamed("Action Taken Secondary","ActionTakenSecondary")
                         .withColumnRenamed("Action Taken Other","ActionTakenOther")
                         .withColumnRenamed("Detector Alerted Occupants","DetectorAlertedOccupants")
                         .withColumnRenamed("Property Use","PropertyUse")
                         .withColumnRenamed("Area of Fire Origin","AreaofFireOrigin")
                         .withColumnRenamed("Ignition Cause","IgnitionCause")
                         .withColumnRenamed("Ignition Factor Primary","IgnitionFactorPrimary")
                         .withColumnRenamed("Ignition Factor Secondary","IgnitionFactorSecondary")
                         .withColumnRenamed("Heat Source","HeatSource")
                         .withColumnRenamed("Item First Ignited","ItemFirstIgnited")
                         .withColumnRenamed("Human Factors Associated with Ignition","HumanFactorsAssociatedwithIgnition")
                         .withColumnRenamed("Structure Type","StructureType")
                         .withColumnRenamed("Structure Status","StructureStatus")
                         .withColumnRenamed("Floor of Fire Origin","FloorofFireOrigin")
                         .withColumnRenamed("Fire Spread","FireSpread")
                         .withColumnRenamed("No Flame Spead","NoFlameSpread")
                         .withColumnRenamed("Number of floors with minimum damage","NumberOfFloorsWithMinimumDamage")
                         .withColumnRenamed("Number of floors with significant damage","NumberOfFloorsWithSignificantDamage")
                         .withColumnRenamed("Number of floors with heavy damage","NumberOfFloorsWithHeavyDamage")
                         .withColumnRenamed("Number of floors with extreme damage","NumberOfFloorsWithExtremeDamage")
                         .withColumnRenamed("Detectors Present","DetectorsPresent")
                         .withColumnRenamed("Detector Type","DetectorType")
                         .withColumnRenamed("Detector Operation","DetectorOperation")
                         .withColumnRenamed("Detector Effectiveness","DetectorEffectiveness")
                         .withColumnRenamed("Detector Failure Reason","DetectorFailureReason")
                         .withColumnRenamed("Automatic Extinguishing System Present","AutomaticExtinguishingSytemPresent")
                         .withColumnRenamed("Automatic Extinguishing Sytem Perfomance","AutomaticExtinguishingSytemPerfomance")
                         .withColumnRenamed("Automatic Extinguishing Sytem Failure Reason","AutomaticExtinguishingSytemFailureReason")
                         .withColumnRenamed("Automatic Extinguishing Sytem Type","AutomaticExtinguishingSytemType")
                         .withColumnRenamed("Number of Sprinkler Heads Operating","NumberofSprinklerHeadsOperating")
                         .withColumnRenamed("Supervisor District","SupervisorDistrict")
                        )

# COMMAND ----------

# Display 3 records from the non_medical_incidents dataframe 
display(non_medical_incidents.limit(3))

# COMMAND ----------

# Join the two dataframes on the IncidentNumber column
joinedDF = (fire_incidents
            .join(non_medical_incidents,fire_incidents.IncidentNumber == non_medical_incidents.IncidentNumber)
            .drop(non_medical_incidents.IncidentNumber)
            .drop(non_medical_incidents.City)
            .drop(non_medical_incidents.StationArea)
            .drop(non_medical_incidents.CallNumber)
            .drop(non_medical_incidents.Battalion)
            .drop(non_medical_incidents.SupervisorDistrict)
            .drop(non_medical_incidents.NumberofAlarms)
            .drop(non_medical_incidents.Address)
            .drop(non_medical_incidents.Box))

# COMMAND ----------

# Display the joined dataframe
display(joinedDF.limit(3))

# COMMAND ----------

# Primary non-medical reasons for calling fire service in Tenderloin during Christmas holidays last year
display(joinedDF
        .filter((col("CallDate") >= '2019-12-24') & (col("CallDate") < '2020-01-02') & (col('NeighborhoodDistrict') == 'Tenderloin'))
        .groupBy("PrimarySituation")
        .count()
        .orderBy(col('count').desc())
       )

# COMMAND ----------

# MAGIC %md
# MAGIC # Machine Learning

# COMMAND ----------

# MAGIC %md
# MAGIC **Our ML scenario will involve predicting whether a fire incident requires ALS i.e Advanced Life Support or not**. ALS is a special team of fire personnel deployed for critical scenarios. The column we'll predict is `ALSUnit`, a boolean column. We have selected the following columns as features from the `fire_incidents` dataframe:
# MAGIC 
# MAGIC - CallType
# MAGIC - StationArea
# MAGIC - FinalPriority
# MAGIC - NumberofAlarms
# MAGIC - UnitType - Categorical
# MAGIC - Response Time (derived feature) = DispatchDtTm - ReceivedDtTm
# MAGIC 
# MAGIC Since it is a **classification** problem, we'll use the **Decision Tree** algorithm for our use case.

# COMMAND ----------

# Importing Libraries
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# COMMAND ----------

# Selecting Feature columns and Target column 
selectionDF = (
  fire_incidents.select("CallType","StationArea","FinalPriority","NumberofAlarms","UnitType","ReceivedDtTm","DispatchDtTm","ALSUnit")
  .withColumn("ResponseDtTm",unix_timestamp("DispatchDtTm") - unix_timestamp("ReceivedDtTm")) # In seconds
  .withColumn("StationArea",fire_incidents.StationArea.cast(IntegerType()))
  .withColumn("ALSUnit",fire_incidents.ALSUnit.cast(IntegerType()))
)

# COMMAND ----------

# Display the dataframe
display(selectionDF)

# COMMAND ----------

# Splitting the dataframe into training and test dataframes
training = selectionDF.filter(year("DispatchDtTm")!=2018)
test = selectionDF.filter(year("DispatchDtTm")==2018)

# COMMAND ----------

# Print the count of training and testing datasets
print(training.count(), test.count())

# COMMAND ----------

# Model training

indexerCallType = StringIndexer(inputCol="CallType", outputCol="CallTypeIndex", handleInvalid="skip")
indexerUnitType = StringIndexer(inputCol="UnitType", outputCol="UnitTypeIndex", handleInvalid="skip")

assembler = VectorAssembler(
    inputCols=["CallTypeIndex", "UnitTypeIndex", "StationArea", "FinalPriority", "ResponseDtTm"],
    outputCol="features", handleInvalid="skip")

dt = DecisionTreeClassifier(maxDepth=3, labelCol="ALSUnit", featuresCol="features")

pipeline = Pipeline(stages=[indexerCallType, indexerUnitType, assembler, dt])

model = pipeline.fit(training)

# COMMAND ----------

# Make predictions
predictions = model.transform(test)

# COMMAND ----------

# Display Actual vs Predictions
display(predictions.select("ALSUnit", "prediction"))

# COMMAND ----------

# Determine accuracy

evaluator = MulticlassClassificationEvaluator(
  labelCol="ALSUnit", 
  predictionCol="prediction", 
  metricName="accuracy")

accuracy = evaluator.evaluate(predictions)

print(accuracy)

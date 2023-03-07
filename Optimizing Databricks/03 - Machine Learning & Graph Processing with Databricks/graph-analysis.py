# Databricks notebook source
# MAGIC %md
# MAGIC ### Importing libraries and functions

# COMMAND ----------

from pyspark.sql.functions import *
from graphframes import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating graph dataframes

# COMMAND ----------

# Create vertices
vertices = spark.createDataFrame([
  ("a", "Dr John", 54, "Cardiac Surgery", "Mumbai"),
  ("b", "Dr Nancy", 36, "Liver Transplant", "Mumbai"),
  ("c", "Dr Stark", 55, "Dentistry", "New Delhi"),
  ("d", "Dr Ross", 29, "Liver Transplant", "New Delhi"),
  ("e", "Dr Mensah", 32, "Dentistry", "New Delhi"),
  ("f", "Dr Brown", 36, "Cardiac Surgery", "New Delhi"),
  ("g", "Dr Ramish", 60, "Cardiac Surgery", "Mumbai")], ["id", "name", "age", "specialization", "location"])

# COMMAND ----------

# Create edges
edges = spark.createDataFrame([
  ("c", "d", "Work in CSM Hospital"),
  ("b", "c", "Attended New Delhi doctors conference"),
  ("c", "b", "Attended New Delhi doctors conference"),
  ("f", "c", "Friend"),
  ("e", "f", "Follows on Twitter"),
  ("c", "e", "Professor in college"),
  ("e", "c", "Student"),
  ("a", "g", "Cousin")
], ["src", "dst", "relationship"])

# COMMAND ----------

# Create a graph from vertices and edges
graph = GraphFrame(vertices, edges)
print(graph)

# COMMAND ----------

# Display vertices
display(graph.vertices)

# COMMAND ----------

# Display edges
display(graph.edges)

# COMMAND ----------

# Incoming degree of vertices
display(graph.inDegrees)

# COMMAND ----------

# Outgoing degree of vertices
display(graph.outDegrees)

# COMMAND ----------

# Degree of vertices
display(graph.degrees)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run queries on vertices dataframe

# COMMAND ----------

# Youngest doctor
youngest_doctor = graph.vertices.groupBy().min("age")
display(youngest_doctor)

# COMMAND ----------

# How many doctors practising in Mumbai?
doctors_in_mumbai = graph.vertices.filter(col("location") == "Mumbai").count()
print(doctors_in_mumbai)

# COMMAND ----------

# How many doctors practising in dentistry in New Delhi?
dentistry_new_delhi = graph.vertices.filter((col("location") == "New Delhi") & (col("specialization") == "Dentistry")).count()
print(dentistry_new_delhi)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run queries on edges dataframe

# COMMAND ----------

# Find out count of relationships about attending New Delhi's doctors conference
conference_count = graph.edges.filter(col("relationship") == "Attended New Delhi doctors conference").count()
print(conference_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Subgraphs

# COMMAND ----------

# Doctor who attended conference or is older than 50 years
subgraph = graph.filterEdges("relationship = 'Attended New Delhi doctors conference'").filterVertices("age > 50")
display(subgraph.vertices)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Breadth-first search (BFS)

# COMMAND ----------

# Search from Dr Brown for doctors with age > 40
display(graph.bfs("name = 'Dr Brown'","age > 40"))

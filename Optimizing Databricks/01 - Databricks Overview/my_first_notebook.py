# Databricks notebook source
print("This is my first Databricks notebook!")

# COMMAND ----------

# MAGIC %scala
# MAGIC println("This is my first Databricks notebook!")

# COMMAND ----------

# MAGIC %r
# MAGIC print("This is my first Databricks notebook!", quote = FALSE)

# COMMAND ----------

# MAGIC %md
# MAGIC Render HTML, CSS, and JavaScript code using the function: displayHTML (available in Python, Scala and R)

# COMMAND ----------

html = """<h1 style ="color:blue;text-align:center;font-family:Courier">It is time to render HTML!</h1>"""
displayHTML(html)

# COMMAND ----------

# Create text widget
dbutils.widgets.text("FullName","","Your Name")

# COMMAND ----------

# Get value of text widget
full_name_get = dbutils.widgets.get("FullName")

# COMMAND ----------

# Print the statement
print("Good Evening",full_name_get)

# COMMAND ----------

print("Let's run a notebook inside another!")

# COMMAND ----------

# MAGIC %fs ls 

# Databricks notebook source
# DBTITLE 1,Pandas
import pandas as pd
 
df= pd.read_csv("0.sample/people.csv")
display(df)

# COMMAND ----------

# DBTITLE 1,pyspark
import os
 
df=spark.read.csv(f"file:{os.getcwd()}/0.sample/people.csv", header=True) # "file:" prefix and absolute file path are required for PySpark
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limitation
# MAGIC
# MAGIC You cannot programmatically write to a file.

# COMMAND ----------



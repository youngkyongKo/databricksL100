# Databricks notebook source
# user 별 database및 변수 설정 
databricks_user = spark.sql("SELECT current_user()").collect()[0][0].split('@')[0].replace(".", "_")
print(databricks_user)

spark.sql("DROP DATABASE IF EXISTS delta_{}_db CASCADE".format(str(databricks_user)))
spark.sql("CREATE DATABASE IF NOT EXISTS delta_{}_db".format(str(databricks_user)))
spark.sql("USE delta_{}_db".format(str(databricks_user)))

# COMMAND ----------



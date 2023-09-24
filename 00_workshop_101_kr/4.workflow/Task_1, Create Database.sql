-- Databricks notebook source
-- MAGIC %python
-- MAGIC import re
-- MAGIC username = spark.sql("SELECT current_user()").first()[0]
-- MAGIC clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
-- MAGIC spark.conf.set("da.user_db", f"dbacademy_{clean_username}_adewd_jobs")

-- COMMAND ----------

DROP DATABASE IF EXISTS ${da.user_db} CASCADE;

CREATE DATABASE IF NOT EXISTS ${da.user_db};
USE ${da.user_db}

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS task_1
(run_time TIMESTAMP);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS task_2
(task STRING, run_time TIMESTAMP);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS task_3
(task STRING, run_time TIMESTAMP);

-- COMMAND ----------

INSERT INTO task_1 VALUES (current_timestamp());

-- COMMAND ----------

SELECT COUNT(*) FROM task_1

-- COMMAND ----------

SELECT * FROM task_1


-- COMMAND ----------

-- MAGIC %python
-- MAGIC # job-level parameter test
-- MAGIC dbutils.widgets.text("year", "2014")
-- MAGIC myYear = getArgument("year")
-- MAGIC display(spark.sql(f"SELECT '{myYear}', current_timestamp()"))

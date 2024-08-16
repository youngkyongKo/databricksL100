# Databricks notebook source
# MAGIC %md
# MAGIC # ![](https://redislabs.com/wp-content/uploads/2016/12/lgo-partners-databricks-125x125.png)  Data Engineering with Databricks 
# MAGIC <br>
# MAGIC
# MAGIC # Incremental Multi-Hop in the Lakehouse
# MAGIC
# MAGIC 이 노트북에서는 Spark Structured Streaming 과 Delta Lake를 사용해서 통합된 Multi Hop 파이프라인에서 손쉽게 streaming 과 batch workload를 합치는 방법에 대해서 다룹니다. 
# MAGIC
# MAGIC ![](https://files.training.databricks.com/images/sslh/multi-hop-simple.png)
# MAGIC
# MAGIC 파이프라인의 각 Bronze,Silver,Gold 데이터 단계별로 프로세싱되는 과정을 거치면서 가장 최신의 데이터를 준실시간성(near-real time)으로 처리해서 분석가에게 제공되게 됩니다.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC - **Bronze** 테이블은 다양한 소스((JSON files, RDBMS data,  IoT data등)에서 수집한 원본 데이터를 저장합니다. 
# MAGIC
# MAGIC - **Silver** 테이블은 우리 데이터에 좀더 정제된 view를 제공합니다. 다양한 bronze 테이블과 조인하거나 불필요한 정보의 제거, 업데이트등을 수행합니다. 
# MAGIC
# MAGIC - **Gold** 테이블은 주로 리포트나 대시보드에서 사용되는 비지니스 수준의 aggregation을 수행한 뷰를 제공합니다.  일간사용자수나 상품별 매출등의 뷰가 이 예입니다. 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Datasets Used
# MAGIC
# MAGIC 이 예제에서는 의학기기에서 발생하는 센서데이터와 환자개인정보(PII) 2가지의 데이터셋을 사용합니다. 
# MAGIC #### Recordings
# MAGIC JSON형태의 센서데이터 스키마 정보는 다음과 같습니다. 
# MAGIC
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | device_id | int |
# MAGIC | mrn | long |
# MAGIC | time | double |
# MAGIC | heartrate | double |
# MAGIC
# MAGIC #### PII
# MAGIC 이 데이터는 외부시스템에서 가져온 환자이름으로 구분되는 환자정보 테이블로 위의 데이터셋과 조인해서 사용할 것입니다. 
# MAGIC
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | mrn | long |
# MAGIC | name | string |

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ./Include/Classroom-Setup-7.1

# COMMAND ----------

# DBTITLE 1,센서데이터 로딩 시뮬레이션
DA.data_factory.load()

# COMMAND ----------

currentUser = spark.sql("SELECT current_user()").collect()[0][0]

# COMMAND ----------

display(dbutils.fs.ls(f"/user/{currentUser}/dbacademy/dewd/7.1/source/tracker/"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Bronze Table: Ingesting Raw JSON Recordings
# MAGIC
# MAGIC 아래의 코드는 위 경로에 적재된 JSON파일을 autoloader를 사용해서 읽는 예제입니다. 
# MAGIC Spark Dataframe API를 써서 incremental read를 설정하고, 데이터에 대해서 쉽게 접근하기 위해서 temp view를 우선 생성했습니다. 
# MAGIC
# MAGIC **NOTE**: For a JSON data source, Auto Loader will default to inferring each column as a string. Here, we demonstrate specifying the data type for the **`time`** column using the **`cloudFiles.schemaHints`** option. Note that specifying improper types for a field will result in null values.

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaHints", "time DOUBLE")
    .option("cloudFiles.schemaLocation", f"{DA.paths.checkpoints}/bronze")
    .load(DA.paths.data_landing_location)
    .createOrReplaceTempView("recordings_raw_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM recordings_raw_temp

# COMMAND ----------

# MAGIC %md
# MAGIC 여기서는 원본 데이터에 추가적인 metadata를 넣어서 해당 raw가 어느 파일에서 언제 수집되었는지의 정보를 추가합니다. 이 정보는 실제 쿼리에서는 사용되지 않고 debugging/참고 용도로 사용하도록 하겠습니다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW recordings_bronze_temp AS (
# MAGIC   SELECT *, current_timestamp() receipt_time, input_file_name() source_file
# MAGIC   FROM recordings_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM recordings_bronze_temp;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC The code below passes our enriched raw data back to PySpark API to process an incremental write to a Delta Lake table.

# COMMAND ----------

# DBTITLE 1,temp dataframe을 Delta Lake Table형태로 저장(incremental write) 
(spark.table("recordings_bronze_temp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/bronze")
      .outputMode("append")
      .table("bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Trigger another file arrival with the following cell and you'll see the changes immediately detected by the streaming query you've written.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM bronze;

# COMMAND ----------

# DBTITLE 1,파일을 추가해서 incremental하게 bronze 테이블이 증가하는지 확인
DA.data_factory.load()

# COMMAND ----------

display(dbutils.fs.ls(f"/user/{currentUser}/dbacademy/dewd/7.1/source/tracker/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM bronze;

# COMMAND ----------

# DBTITLE 1,환자 PII 데이터 load
(spark.read
      .format("csv")
      .schema("mrn STRING, name STRING")
      .option("header", True)
      .load(f"{DA.paths.data_source}/patient/patient_info.csv")
      .createOrReplaceTempView("pii"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pii

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Silver Table: sensor recording 데이터 Enrich
# MAGIC 두번째 단계에서 우리는 아래 enrichment작업들을 수행할 것입니다:
# MAGIC - PII 데이터와 조인해서 환자 이름을 추가 
# MAGIC - sensor 데이터내의 사람이 읽을 수 있는  **`'yyyy-MM-dd HH:mm:ss'`** 포맷으로 timestamp 파싱
# MAGIC - heart rate가 0보다 작은 row는 삭제(쓰레기데이터로 판단)

# COMMAND ----------

(spark.readStream
  .table("bronze")
  .createOrReplaceTempView("bronze_tmp"))

# COMMAND ----------

# DBTITLE 1,두 테이블 조인
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW recordings_w_pii AS (
# MAGIC   SELECT device_id, a.mrn, b.name, cast(from_unixtime(time, 'yyyy-MM-dd HH:mm:ss') AS timestamp) time, heartrate
# MAGIC   FROM bronze_tmp a
# MAGIC   INNER JOIN pii b
# MAGIC   ON a.mrn = b.mrn
# MAGIC   WHERE heartrate > 0)

# COMMAND ----------

# DBTITLE 1,recordings_enriched delta 테이블로 write
(spark.table("recordings_w_pii")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/recordings_enriched")
      .outputMode("append")
      .table("recordings_enriched"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 또 다른 파일을 추가해서 데이터들이 잘 전파되는지 확인해 보자. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM recordings_enriched

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM recordings_enriched

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Gold Table: 일평균 
# MAGIC
# MAGIC 이제 Here we read a stream of data from **`recordings_enriched`** 테이블에서 데이터 스트림을 읽어서 각 환자별 일 평균 hear rate를 계산하는 aggregate golden 테이블을 만들자.

# COMMAND ----------

(spark.readStream
  .table("recordings_enriched")
  .createOrReplaceTempView("recordings_enriched_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW patient_avg AS (
# MAGIC   SELECT mrn, name, mean(heartrate) avg_heartrate, date_trunc("DD", time) date
# MAGIC   FROM recordings_enriched_temp
# MAGIC   GROUP BY mrn, name, date_trunc("DD", time))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 아래 코드의 **`.trigger(availableNow=True)`**  구문은 Structured Streaming 을 그대로 사용하면서도 micro batch 에서 모든 처리 가능한 데이터를 수행하기 위해 이 job을 1회성으로 수행하는 트리거링합니다. 
# MAGIC
# MAGIC - end-to-end fault tolerant 프로세싱 
# MAGIC - Upstream 데이터 소스에서의 변경을 자동 감지
# MAGIC
# MAGIC 수집데이터의 대략적인 사이즈를 알고 있다면 위의 작업을 주기적으로 스케쥴해서 비용 효율적으로 incremental한 부분만 빠르게 처리할 수 있습니다. 
# MAGIC
# MAGIC 또한 

# COMMAND ----------

(spark.table("patient_avg")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/daily_avg")
      .trigger(availableNow=True)
      .table("daily_patient_avg"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC #### Important Considerations for complete Output with Delta
# MAGIC
# MAGIC When using **`complete`** output mode, we rewrite the entire state of our table each time our logic runs. While this is ideal for calculating aggregates, we **cannot** read a stream from this directory, as Structured Streaming assumes data is only being appended in the upstream logic.
# MAGIC
# MAGIC **NOTE**: Certain options can be set to change this behavior, but have other limitations attached. For more details, refer to <a href="https://docs.databricks.com/delta/delta-streaming.html#ignoring-updates-and-deletes" target="_blank">Delta Streaming: Ignoring Updates and Deletes</a>.
# MAGIC
# MAGIC The gold Delta table we have just registered will perform a static read of the current state of the data each time we run the following query.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_patient_avg

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM daily_patient_avg
# MAGIC WHERE date BETWEEN "2020-01-17" AND "2020-01-31"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Process Remaining Records
# MAGIC 아래 셀을 돌려서 2020년치 모든 파일을 한꺼번에 로드해 봅니다. 

# COMMAND ----------

DA.data_factory.load(continuous=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM recordings_enriched

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Wrapping Up
# MAGIC
# MAGIC 마지막으로 모든 스트림이 멈췄는지 확인하고 데이터를 정리합니다

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC Delta Lake 와  Structured Streaming 를 활용하면 lakehouse 에서 준실시간 분석을 손쉽게 구성할 수 있습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Additional Topics & Resources
# MAGIC
# MAGIC * <a href="https://docs.databricks.com/delta/delta-streaming.html" target="_blank">Table Streaming Reads and Writes</a>
# MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html" target="_blank">Structured Streaming Programming Guide</a>
# MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a> by Tathagata Das. This is an excellent video describing how Structured Streaming works.
# MAGIC * <a href="https://databricks.com/glossary/lambda-architecture" target="_blank">Lambda Architecture</a>
# MAGIC * <a href="https://bennyaustin.wordpress.com/2010/05/02/kimball-and-inmon-dw-models/#" target="_blank">Data Warehouse Models</a>
# MAGIC * <a href="http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html" target="_blank">Create a Kafka Source Stream</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>

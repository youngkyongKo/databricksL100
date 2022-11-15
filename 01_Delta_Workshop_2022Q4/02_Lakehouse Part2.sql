-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Delta Lake 퀵 스타트(SQL) Part 2
-- MAGIC 
-- MAGIC ## 목적
-- MAGIC 
-- MAGIC 이 노트북에서는 Spark SQL을 사용하여 데이터 및 Delta Lake 형식과 상호 작용하는 다양한 방법을 논의하고 시연합니다. 다양한 데이터 소스를 쿼리하는 방법, 해당 데이터를 Delta에 저장하는 방법, 메타데이터를 관리 및 조회하는 방법, 일반적인 SQL DML 명령(MERGE, UPDATE, DELETE)을 사용하여 데이터를 정제하는 방법, Delta Lake를 관리하고 최적화하는 방법에 대해서 배우게 됩니다.
-- MAGIC 
-- MAGIC 이 코스에서는 다음과 같은 작업을 수행하고 자신만의 Delta Lake를 만들게 됩니다.
-- MAGIC 
-- MAGIC 1. Spark SQL로 다른 종류의 데이터 소스를 직접 쿼리  
-- MAGIC 2. 관리형 테이블 생성(델타 및 비델타 테이블 모두) 
-- MAGIC 3. 메타데이터 생성, 제어, 검색
-- MAGIC 4. MERGE, UPDATE, DELETE 구문을 활용한 데이터셋 정제  
-- MAGIC 5. 데이터의 과거 버전 탐색  
-- MAGIC 6. SQL로 스트리밍 데이터 직접 쿼리  
-- MAGIC 7. 병합, 집계 수행하여 최종 레이어 생성  
-- MAGIC 8. 테이블 최적화 및 데이터 레이크 관리  
-- MAGIC 
-- MAGIC ⚠️ Please use DBR 8.* to run this notebook

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 설정
-- MAGIC 
-- MAGIC 아래의 셀을 실행해서 실습용 데이터베이스를 사용하도록 설정합니다.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC databricks_user = spark.sql("SELECT current_user()").collect()[0][0].split('@')[0].replace(".", "_")
-- MAGIC spark.sql("USE delta_{}_db".format(str(databricks_user)))
-- MAGIC print("데이터베이스명 : delta_{}_db".format((databricks_user)))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Streaming 시각화
-- MAGIC 
-- MAGIC 브론즈 테이블에 저장
-- MAGIC 원시 데이터는 대량 업로드 혹은 스트리밍 소스를 통해서 데이터 레이크에 수집되는 변경되지 않은 데이터 입니다.
-- MAGIC 다음의 함수는 카프카 서버에 덤프된 위키피디아 IRC 채널을 읽습니다. 카프카 서버는 일종의 "소방 호스" 역할을 하며 원시 데이터를 데이터 레이크에 덤프합니다.
-- MAGIC 
-- MAGIC 다음의 첫 단계는 스키마를 설정하는 것으로 추가 설명은 노트북 커멘트를 참고해주세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def untilStreamIsReady(name):
-- MAGIC   queries = list(filter(lambda query: query.name == name, spark.streams.active))
-- MAGIC 
-- MAGIC   if len(queries) == 0:
-- MAGIC     print("The stream is not active.")
-- MAGIC 
-- MAGIC   else:
-- MAGIC     while (queries[0].isActive and len(queries[0].recentProgress) == 0):
-- MAGIC       pass # wait until there is any type of progress
-- MAGIC 
-- MAGIC     if queries[0].isActive:
-- MAGIC       queries[0].awaitTermination(5)
-- MAGIC       print("The stream is active and ready.")
-- MAGIC     else:
-- MAGIC       print("The stream is not active.")

-- COMMAND ----------

-- MAGIC 
-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
-- MAGIC from pyspark.sql.functions import from_json, col
-- MAGIC 
-- MAGIC schema = StructType([
-- MAGIC   StructField("channel", StringType(), True),
-- MAGIC   StructField("comment", StringType(), True),
-- MAGIC   StructField("delta", IntegerType(), True),
-- MAGIC   StructField("flag", StringType(), True),
-- MAGIC   StructField("geocoding", StructType([                 # (OBJECT): Added by the server, field contains IP address geocoding information for anonymous edit.
-- MAGIC     StructField("city", StringType(), True),
-- MAGIC     StructField("country", StringType(), True),
-- MAGIC     StructField("countryCode2", StringType(), True),
-- MAGIC     StructField("countryCode3", StringType(), True),
-- MAGIC     StructField("stateProvince", StringType(), True),
-- MAGIC     StructField("latitude", DoubleType(), True),
-- MAGIC     StructField("longitude", DoubleType(), True),
-- MAGIC   ]), True),
-- MAGIC   StructField("isAnonymous", BooleanType(), True),      # (BOOLEAN): Whether or not the change was made by an anonymous user
-- MAGIC   StructField("isNewPage", BooleanType(), True),
-- MAGIC   StructField("isRobot", BooleanType(), True),
-- MAGIC   StructField("isUnpatrolled", BooleanType(), True),
-- MAGIC   StructField("namespace", StringType(), True),         # (STRING): Page's namespace. See https://en.wikipedia.org/wiki/Wikipedia:Namespace 
-- MAGIC   StructField("page", StringType(), True),              # (STRING): Printable name of the page that was edited
-- MAGIC   StructField("pageURL", StringType(), True),           # (STRING): URL of the page that was edited
-- MAGIC   StructField("timestamp", StringType(), True),         # (STRING): Time the edit occurred, in ISO-8601 format
-- MAGIC   StructField("url", StringType(), True),
-- MAGIC   StructField("user", StringType(), True),              # (STRING): User who made the edit or the IP address associated with the anonymous editor
-- MAGIC   StructField("userURL", StringType(), True),
-- MAGIC   StructField("wikipediaURL", StringType(), True),
-- MAGIC   StructField("wikipedia", StringType(), True),         # (STRING): Short name of the Wikipedia that was edited (e.g., "en" for the English)
-- MAGIC ])
-- MAGIC 
-- MAGIC # start our stream
-- MAGIC (spark.readStream
-- MAGIC   .format("kafka")  
-- MAGIC   .option("kafka.bootstrap.servers", "xxx.xxx.xxx:9092")
-- MAGIC   .option("subscribe", "en")
-- MAGIC   .load()
-- MAGIC   .withColumn("json", from_json(col("value").cast("string"), schema))
-- MAGIC   .select(col("timestamp").alias("kafka_timestamp"), col("json.*"))
-- MAGIC   .writeStream
-- MAGIC   .format("delta")
-- MAGIC   .option("checkpointLocation", '/tmp/delta_rapid_start/{}/checkpoint/'.format(str(databricks_user)))
-- MAGIC   .outputMode("append")
-- MAGIC   .queryName("stream_1p")
-- MAGIC   .toTable('wikiIRC')
-- MAGIC )

-- COMMAND ----------

SELECT * FROM wikiIRC  

-- COMMAND ----------

-- MAGIC %python
-- MAGIC untilStreamIsReady('stream_1p')

-- COMMAND ----------

SELECT * FROM wikiIRC  

-- COMMAND ----------

describe detail wikiIRC

-- COMMAND ----------

describe detail wikiIRC

-- COMMAND ----------

select count(*) from wikiIRC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Medallion 아키텍처

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Silver 테이블
-- MAGIC 실버존은 데이터가 필터링, 정리 및 보강되는 곳입니다.
-- MAGIC 
-- MAGIC **User Data Silver**
-- MAGIC * 회사 표준에 맞게 컬럼명 변경
-- MAGIC 
-- MAGIC **Device Data Silver**
-- MAGIC * 타임스탬프에 파생된 날짜 열 추가
-- MAGIC * 더 이상 필요하지 않은 컬럼 삭제
-- MAGIC * 보폭을 기록할 컬럼 생성(miles walked / steps)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_data_silver
USING DELTA
  SELECT 
    userid AS user_id,
    gender,
    age,
    height,
    weight,
    smoker,
    familyhistory AS family_history,
    chosestlevs AS cholest_levs,
    bp AS blood_pressure,
    risk
  FROM user_data_bronze_delta;
  
SELECT * FROM user_data_silver

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS device_data_silver
USING DELTA
  SELECT
    id,
    device_id,
    user_id,
    calories_burnt,
    miles_walked,
    num_steps,
    miles_walked/num_steps as stride,
    timestamp,
    DATE(timestamp) as date
  FROM device_data_bronze_delta;
  
SELECT * FROM device_data_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gold 테이블
-- MAGIC 골드존은 분석 및 보고에 사용되는 비즈니스 수준의 집계 테이블로 실버 테이블을 함께 조인하고 집계를 수행하는 것과 같은 변환 과정을 통해 생성됩니다.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_daily_averages_gold
USING DELTA
  SELECT
      u.user_id,
      u.gender,
      u.age,
      u.blood_pressure,
      u.cholest_levs,
      AVG(calories_burnt) as avg_calories_burnt,
      AVG(num_steps) as avg_num_steps,
      AVG(miles_walked) as avg_miles_walked
    FROM user_data_silver u
    LEFT JOIN
      (SELECT 
        user_id,
        date,
        MAX(calories_burnt) as calories_burnt,
        MAX(num_steps) as num_steps,
        MAX(miles_walked) as miles_walked
      FROM device_data_silver
      GROUP BY user_id, date) as daily
    ON daily.user_id = u.user_id
    GROUP BY u.user_id,
      u.gender,
      u.age,
      u.blood_pressure,
      u.cholest_levs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Schema Enforcement & Evolution
-- MAGIC **Schema enforcement**, 스키마 유효성 검사라고도 하는 데이터 품질을 보장하기 위한 Delta Lake의 보호 장치입니다. Delta Lake는 쓰기 시 스키마 유효성 검사를 합니다. 즉 , 테이블에 대한 모든 신규 쓰기 시 대상 테이블의 스키마와 호환성이 확인됩니다. 스키마가 호환되지 않는 경우 Delta Lake는 트랜잭션을 완전히 취소하고(데이터가 기록되지 않음) 예외를 발생시켜 사용자에게 불일치에 대해서 알려줍니다.
-- MAGIC 
-- MAGIC **Schema evolution** 시간이 지남에 따라 변화하는 데이터를 수용하기 위해 사용자가 테이블의 현재 스키마를 쉽게 변경 할 수 있는 기능입니다. 가장 일반적인 사례는 하나 이상의 새 컬럼을 포함하도록 스키마를 자동 조정하기 위해 추가 및 덮어쓰기 작업을 수행할 때 사용합니다.
-- MAGIC 
-- MAGIC ### Schema Enforcement
-- MAGIC 테이블에 쓰기에 적합한지 판단 시 Delta Lake는 다음의 규칙을 따름니다. 쓸 데이터프레임이 :
-- MAGIC 
-- MAGIC * 대상 테이블의 스키마에 없는 추가 열을 포함할 수 없습니다. 
-- MAGIC * 대상 테이블의 컬럼 데이터 유형과 다른 컬럼 데이터 유형이 있을 수 없습니다.
-- MAGIC * 대소문자만 다른 열 이름은 포함할 수 없습니다.

-- COMMAND ----------

-- DBTITLE 1,에러 발생 여부 확인
-- You can uncomment the next line to see the error (remove the -- at the beginning of the line)
 INSERT INTO TABLE user_data_bronze_delta VALUES ('this is a test')

-- COMMAND ----------

-- You can uncomment the next line to see the error (remove the -- at the beginning of the line)
INSERT INTO user_data_bronze_delta VALUES (39, 'M', 44, 65, 150, 'N','N','Normal','High',20, 'John Doe')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Schema Evolution

-- COMMAND ----------

-- Let's create a new table
CREATE OR REPLACE TABLE user_data_bronze_delta_new
USING DELTA
SELECT * FROM user_data_bronze_delta

-- COMMAND ----------

-- Set this configuration to allow for schema evolution
SET spark.databricks.delta.schema.autoMerge.enabled = true

-- COMMAND ----------

select * from user_data_bronze_delta

-- COMMAND ----------

-- Create new data to append
ALTER TABLE user_data_bronze_delta_new ADD COLUMN (Name string);

UPDATE user_data_bronze_delta_new
SET Name = 'J. Doe',
  userid = userid + 5;

SELECT * FROM user_data_bronze_delta_new

-- COMMAND ----------

-- Name is now in user_data_bronze_delta as well
INSERT INTO user_data_bronze_delta
SELECT * FROM user_data_bronze_delta_new;

SELECT * FROM user_data_bronze_delta

-- COMMAND ----------

-- shows schema history as of previous version
SELECT * FROM user_data_bronze_delta VERSION AS OF 0

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled = false

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Optimize & Z-Ordering
-- MAGIC 
-- MAGIC 쿼리 속도를 향상시키기 위해 데이터브릭스 상의 Delta Lake는 클라우드 저장소에 저장된 데이터의 레이아웃을 최적화하는 기능을 제공합니다.
-- MAGIC `OPTIMIZE` 명령은 작은 파일을 큰 파일로 합치는 데 사용할 수 있습니다.
-- MAGIC 
-- MAGIC Z-Ordering은 관련 정보를 동일한 파일 집합에 배치해서 읽어야 하는 데이터의 양을 줄여 쿼리 성능을 향상 시키는 기술입니다. 쿼리 조건에 자주 사용되고 해당 열에 높은 카디널리티(distinct 값이 많은)가 있는 경우 `ZORDER BY`를 사용합니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Optimize

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/delta_{}_db.db/device_data_bronze_delta/device_id=1/'.format(str(databricks_user))))

-- COMMAND ----------

-- DBTITLE 1,Optimize 수행 전에 몇 개의 파일이 있는지 확인해 봅시다(numFiles).
desc detail device_data_bronze_delta

-- COMMAND ----------

-- DBTITLE 1,수백KB 수준의 작은 파일들
-- MAGIC %fs
-- MAGIC ls /user/hive/warehouse/delta_sangbae_lim_db.db/device_data_bronze_delta/device_id=1

-- COMMAND ----------

-- DBTITLE 1,Optimize Zorder 수행
OPTIMIZE device_data_bronze_delta 
ZORDER BY num_steps, calories_burnt

/*
{"numFilesAdded": 20, "numFilesRemoved": 140, "filesAdded": {"min": 3982018, "max": 4095614, "avg": 4024556.3, "totalFiles": 20, "totalSize": 80491126}, "filesRemoved": {"min": 435006, "max": 676898, "avg": 612210.9857142858, "totalFiles": 140, "totalSize": 85709538}, "partitionsOptimized": 20, "zOrderStats": {"strategyName": "minCubeSize(107374182400)", "inputCubeFiles": {"num": 0, "size": 0}, "inputOtherFiles": {"num": 140, "size": 85709538}, "inputNumCubes": 0, "mergedFiles": {"num": 140, "size": 85709538}, "numOutputCubes": 20, "mergedNumCubes": null}, "numBatches": 1, "totalConsideredFiles": 140, "totalFilesSkipped": 0, "preserveInsertionOrder": false}
*/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # CLONE
-- MAGIC 
-- MAGIC Clone 명령을 사용해서 특정 버전의 Delta Table의 복사본을 생성할 수 있습니다. Clone에는 2가지 유형이 있습니다 :
-- MAGIC 
-- MAGIC * **deep clone**은 기존 테이블의 메타데이터 외에 소스 테이블의 데이터를 클론 대상에 복사하는 Clone입니다.
-- MAGIC * **shallow clone**은 데이터 파일을 클론 대상에 복사하지 않는 클론입니다. 테이블의 메타 데이터는 소스와 동일합니다. 생성 비용을 절약할 수 있습니다.
-- MAGIC 
-- MAGIC Deep 혹은 Shallow clone에 대한 모든 변경사항은 오직 클론 자체에만 영향을 주며 원본 테이블에는 영향을 주지 않습니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Clone 활용 사례
-- MAGIC 
-- MAGIC * Data archiving : time travel 보다 장기 보유 혹은 DR 목적으로 deep clone 활용
-- MAGIC * Machine learning flow reproduction : 특정 버전의 테이블을 아카이빙 후 ML모델 훈련에 활용
-- MAGIC * Short-term experiments on a production table : 기존 테이블을 훼손하지 않으면서 운영계 테이블에 대한 워크 플로우 테스트(Shallow clone)
-- MAGIC * Data sharing : 동일 기관 내 다른 사업부에서 동일한 데이터에 대한 접근이 필요하나 최신의 데이터는 아니어도 될 경우 활용. 별개의 권한을 부여
-- MAGIC * Table property overrides : 기존 소스 테이블과 개별의 로그 유지 기간을 적용(더 장기간의 보관 주기 설정)
-- MAGIC 
-- MAGIC [here](https://docs.microsoft.com/en-us/azure/databricks/delta/delta-utility#in-this-section).

-- COMMAND ----------

CREATE TABLE user_data_bronze_delta_clone
SHALLOW CLONE user_data_bronze_delta
VERSION AS OF 1;

SELECT * FROM user_data_bronze_delta_clone;

-- COMMAND ----------

CREATE TABLE user_data_bronze_delta_clone_deep
DEEP CLONE user_data_bronze_delta;

SELECT * FROM user_data_bronze_delta_clone_deep

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 작업 환경 정리
-- MAGIC 
-- MAGIC 다음의 코드를 수행하여 이 과정에서 생성한 데이터베이스 및 테이블을 삭제합니다.
-- MAGIC 
-- MAGIC Please uncomment the last cell to clean things up. You can remove the `#` and run the cell again

-- COMMAND ----------

-- DBTITLE 1,스트리밍 중단 및 데이터 정리(필수)
-- MAGIC %python
-- MAGIC for s in spark.streams.active:
-- MAGIC    s.stop()

-- COMMAND ----------

-- DBTITLE 1,데모용 데이터베이스 삭제(Databricks SQL 이후 수행!)
-- MAGIC %python
-- MAGIC spark.sql("DROP DATABASE IF EXISTS delta_{}_db CASCADE".format(str(databricks_user)))
-- MAGIC dbutils.fs.rm('/tmp/delta_rapid_start/{}/'.format(str(databricks_user)), True)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

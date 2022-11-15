-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Delta Lake 퀵 스타트(SQL) Part 1
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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 설정
-- MAGIC 
-- MAGIC 아래의 셀을 실행해서 임시 데이터베이스를 셋업 합니다. 마지막 행에 이번 세션에서 사용할 데이터베이스 이름이 출력됩니다. 확인 부탁 드립니다.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC databricks_user = spark.sql("SELECT current_user()").collect()[0][0].split('@')[0].replace(".", "_")
-- MAGIC 
-- MAGIC spark.sql("DROP DATABASE IF EXISTS delta_{}_db CASCADE".format(str(databricks_user)))
-- MAGIC spark.sql("CREATE DATABASE IF NOT EXISTS delta_{}_db".format(str(databricks_user)))
-- MAGIC spark.sql("USE delta_{}_db".format(str(databricks_user)))
-- MAGIC print("데이터베이스명 : delta_{}_db".format((databricks_user)))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 데이터 레이크 쿼리
-- MAGIC 
-- MAGIC 데이터브릭스는 퍼블릭 클라우드의 오브젝트 스토리지(S3/ADLS/Google Storage)에 저정된 파일을 쿼리하는 것을 지원합니다.
-- MAGIC 이번 과정에서는 데이터브릭스 호스팅 저장소의 파일을 쿼리합니다.
-- MAGIC 
-- MAGIC [These files are mounted to your Databricks File System under the /databricks-datasets/ path](https://docs.databricks.com/data/databricks-datasets.html), which makes them easier to reference from notebook code.
-- MAGIC 
-- MAGIC 기존 Bucket을 마운트 하는 방법(Azure, AWS) [클릭](https://docs.databricks.com/data/databricks-file-system.html#mount-storage).
-- MAGIC 
-- MAGIC Azure Gen2 [클릭](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 매직 커맨드
-- MAGIC 
-- MAGIC [Magic commands](https://docs.databricks.com/notebooks/notebooks-use.html#mix-languages)는 노트북 내에서 언어를 변경할 수 있게 해주는 숏컷
-- MAGIC 
-- MAGIC `%fs`는 [dbutils files sytem utilities]를 사용할 수 있게 해주는 숏컷(https://docs.databricks.com/dev-tools/databricks-utils.html#file-system-utilities). 이 코스에서는 IOT 스트리밍 데이터 디렉터리 아래에 파일 리스트를 확인해볼 것 입니다.
-- MAGIC 
-- MAGIC 디바이스 데이터가 다수의 압축된 JSON 파일로 나누어져 있으며, 개별 파일을 지정하지 않고 디렉터리를 지정해서 해당 데이터를 읽을 것 입니다.

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/iot-stream/data-user

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/iot-stream/data-device

-- COMMAND ----------

-- MAGIC %python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CSV Data
-- MAGIC 
-- MAGIC CSV 데이터는 아래와 같은 방식으로 직접 쿼리할 수 있지만, 데이터에 헤더가 있거나 옵션을 지정하고 싶으면 임시 뷰를 생성해야 합니다.

-- COMMAND ----------

SELECT
  *
FROM
  csv.`/databricks-datasets/iot-stream/data-user/userData.csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 보이는 것 처럼 이 데이터셋은 헤더가 있고, 해당 정보를 적절하게 쿼리하려면 임시 뷰를 만들어야 합니다. 특히 헤더와 스키마를 지정하려면 임시 뷰를 만들어야 합니다. 스키마를 지정하면 초기 추론 스캔을 방지하여 성능을 향상 시킬 수 있습니다. 하지만 이 데이터셋은 작기 때문에 스키마 유추를 하도록 합니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 실습(CSV)

-- COMMAND ----------

-- csv 파일 혹은 해당 파일이 있는 디렉터리(path '/databricks-datasets/iot-stream/data-user/userData.csv')를 사용하는 userData라는 임시뷰를 만들어 봅시다
-- header 파일이 있고(header true),  스키마 추론(inferSchema true)을 사용합니다.
-- 임시 뷰를 생성한 이후 select 문을 수행해서 결과를 확인합니다.

CREATE OR REPLACE TEMPORARY VIEW 뷰이름
USING 파일종류
OPTIONS(
  path '경로',
  header 사용여부,
  inferSchema 사용여부 
);

SELECT * FROM 뷰이름;

-- COMMAND ----------

-- Answer
CREATE OR REPLACE TEMPORARY VIEW userData
USING csv
OPTIONS(
  path '/databricks-datasets/iot-stream/data-user/userData.csv',
  header true,
  inferSchema true 
);

SELECT * FROM userData;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## JSON Data
-- MAGIC 
-- MAGIC JSON 파일은 CSV 처럼 직접 쿼리할 수도 있습니다. Spark는 JSON 파일 전체 디렉터리를 읽고 직접 쿼리할 수 있습니다. CSV 직접 읽기와 유사한 문장을 사용하겠지만 여기에서는 JSON을 지정하겠습니다.

-- COMMAND ----------

SELECT * FROM JSON.`/databricks-datasets/iot-stream/data-device` 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 테이블 생성
-- MAGIC 
-- MAGIC 테이블을 메타스토어에 등록할 수 있고 다른 사용자가 해당 테이블을 사용할 수 있습니다. 메타스토어에 등록된 새 데이터베이스 및 테이블은 왼쪽의 데이터 탭에서 찾을 수 있습니다.
-- MAGIC 
-- MAGIC 두 유형의 테이블을 생성할 수 있습니다. [managed and unmanaged](https://docs.microsoft.com/en-us/azure/databricks/data/tables#--managed-and-unmanaged-tables)
-- MAGIC - Managed 테이블은 메타스토어에 저장되고 모든 관리는 메타스토어에서 합니다. Managed 테이블을 삭제하면 관련 파일 및 데이터가 삭제됩니다.
-- MAGIC - Unmanaged 테이블은 다른 위치에 저장된 파일 및 데이터에 대한 포인터입니다. Unmanaged 테이블을 삭제하면 메타데이터는 삭제되고 해당 데이터셋을 참조하는 별칭도 제거합니다. 하지만 근본적인 데이터와 파일은 원 경로에 지워지지 않은 상태로 남아 있습니다.
-- MAGIC 
-- MAGIC 이 과정에서는 Managed 테이블을 사용합니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CSV User Table
-- MAGIC 
-- MAGIC 실습 첫 번째 테이블을 만들어 봅시다! 초기 테이블은 user_data_raw_csv 파일이며 이 테이블을 사용합니다.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_data_raw_csv(
  userid STRING,
  gender STRING,
  age INT,
  height INT,
  weight INT,
  smoker STRING,
  familyhistory STRING,
  chosestlevs STRING,
  bp STRING,
  risk INT)
USING csv
OPTIONS (path "/databricks-datasets/iot-stream/data-user/userData.csv", header "true");

DESCRIBE TABLE user_data_raw_csv

-- COMMAND ----------

describe formatted user_data_raw_csv

-- COMMAND ----------

SELECT * FROM user_data_raw_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### JSON Device Table

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/iot-stream/data-device

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC du -h /dbfs/databricks-datasets/iot-stream/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####실습(JSON)

-- COMMAND ----------

/*
다음의 컬럼 정보를 이용해서 테이블을 생성합니다.
  calories_burnt DOUBLE,
  device_id INT,
  id STRING,
  miles_walked DOUBLE,
  num_steps INT,
  `timestamp` TIMESTAMP,
  user_id STRING,
  value STRING
*/
CREATE TABLE IF NOT EXISTS device_data_raw_json(
--컬럼 내용 추가
)
USING JSON
OPTIONS (path "/databricks-datasets/iot-stream/data-device")

-- COMMAND ----------

/*
다음의 컬럼 정보를 이용해서 테이블을 생성합니다.

*/
CREATE TABLE IF NOT EXISTS device_data_raw_json(
  calories_burnt DOUBLE,
  device_id INT,
  id STRING,
  miles_walked DOUBLE,
  num_steps INT,
  `timestamp` TIMESTAMP,
  user_id STRING,
  value STRING
)
USING JSON
OPTIONS (path "/databricks-datasets/iot-stream/data-device")

-- COMMAND ----------

select * from device_data_raw_json limit 10;

-- COMMAND ----------

select user_id, device_id, value:user_id, value:num_steps from device_data_raw_json limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### JSON에서 Delta Table 생성

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### 실습(델타 테이블)

-- COMMAND ----------

/*
CTAS 방식(Create Table As Select)으로 델타테이블을 생성합니다.
테이블명 : device_data_bronze_delta
파티션 컬럼 : device_id
JSON 경로 : `/databricks-datasets/iot-stream/data-device`
=> SELECT * FROM JSON.`/databricks-datasets/iot-stream/data-device`
*/
CREATE TABLE IF NOT EXISTS 델타테이블명
USING DELTA
PARTITIONED BY (파티션컬럼)
AS SELECT * FROM JSON.`JSON경로`;

SELECT * FROM 델타테이블명;

-- COMMAND ----------

/*
CTAS 방식(Create Table As Select)으로 델타테이블을 생성합니다.
테이블명 : device_data_bronze_delta
파티션 컬럼 : device_id
JSON 경로 : `/databricks-datasets/iot-stream/data-device`
=> SELECT * FROM JSON.`/databricks-datasets/iot-stream/data-device`
*/
CREATE TABLE IF NOT EXISTS device_data_bronze_delta
USING DELTA
PARTITIONED BY (device_id)
AS SELECT * FROM JSON.`/databricks-datasets/iot-stream/data-device`;

SELECT * FROM device_data_bronze_delta;

-- COMMAND ----------

describe detail device_data_bronze_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CSV 테이블에서 Delta Table 생성

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_data_bronze_delta 
USING DELTA
PARTITIONED BY (gender)
COMMENT "User Data Raw Table - No Transformations"
AS SELECT * FROM user_data_raw_csv;

SELECT * FROM user_data_bronze_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### 메타데이터 추가
-- MAGIC 
-- MAGIC Comment는 컬럼, 테이블 수준에서 추가할 수 있습니다. 
-- MAGIC `ALTER TABLE` 구문을 사용해서 테이블 혹은 컬럼에 comment를 추가합니다.[생성 시에도 가능](https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-hiveformat.html#create-table-with-hive-format) (상기 예 참조).
-- MAGIC 
-- MAGIC Comment 내용도 UI에 표시됩니다.

-- COMMAND ----------

ALTER TABLE user_data_bronze_delta ALTER COLUMN bp COMMENT 'Can be High or Normal';
ALTER TABLE user_data_bronze_delta ALTER COLUMN smoker COMMENT 'Can be Y or N';
ALTER TABLE user_data_bronze_delta ALTER COLUMN familyhistory COMMENT 'Can be Y or N';
ALTER TABLE user_data_bronze_delta ALTER COLUMN chosestlevs COMMENT 'Can be High or Normal';
ALTER TABLE user_data_bronze_delta SET TBLPROPERTIES ('comment' = 'User Data Raw Table describing risk levels based on health factors');

-- COMMAND ----------

-- this shows the column schema and comments
DESCRIBE TABLE user_data_bronze_delta

-- COMMAND ----------

ALTER TABLE user_data_bronze_delta ALTER COLUMN age COMMENT '나이';
ALTER TABLE user_data_bronze_delta ALTER COLUMN userid COMMENT '사용자 아이디 정보이고 unique한 값으로 되어 있습니다';

-- COMMAND ----------

-- you can see the table comment in the detailed Table Information at the bottom - under the columns
DESCRIBE TABLE EXTENDED user_data_bronze_delta

-- COMMAND ----------

-- this one has number of files and sizeInBytes
DESCRIBE DETAIL user_data_bronze_delta

-- COMMAND ----------

-- DBTITLE 1,여기서 잠깐 운영 할 때 이런 자료도 필요합니다.
-- MAGIC %python
-- MAGIC #테이블 리스트 및 empty 데이터프레임 생성
-- MAGIC #spark.sql("use delta_sangbae_lim_db")
-- MAGIC spark.sql("USE delta_{}_db".format(str(databricks_user)))
-- MAGIC spark.sql("drop table IF EXISTS  detailedTableInfoList")
-- MAGIC myTList=spark.sql("show tables").select('tableName').where("isTemporary == false")
-- MAGIC resultDF = spark.sql(f"desc detail {myTList.take(1)[0]['tableName']}")
-- MAGIC resultDF = resultDF.where("1==0")
-- MAGIC #최종 "desc detail 테이블명" 결과를 하나의 데이터프레임으로 생성
-- MAGIC for table in myTList.collect():  
-- MAGIC   myDF=spark.sql(f"desc detail {table[0]}")
-- MAGIC   resultDF = resultDF.union(myDF)
-- MAGIC #최종 수집된 내용을 델타 테이블(External)에 저장
-- MAGIC 
-- MAGIC resultDF.write.format("delta").mode("overwrite").saveAsTable("detailedTableInfoList")

-- COMMAND ----------

select * from detailedTableInfoList where format ='delta'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### 데이터 탭에서 테이블 표시
-- MAGIC 
-- MAGIC Click on the Data tab.
-- MAGIC <img src="https://docs.databricks.com/_images/data-icon.png" /></a>
-- MAGIC 
-- MAGIC You can see your database.table in the data tab
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="https://docs.databricks.com/_images/default-database.png"/></a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DELETE, UPDATE, UPSERT
-- MAGIC 
-- MAGIC 파케이(parquet)는 전통적으로 데이터 레이크의 주요 데이터 스토리지 계층입니다. 하지만 몇 가지 심각한 단점이 있습니다.
-- MAGIC 파케이는 완전한 SQL DML을 지원하지 않습니다. 파케이는 변경이 불가능하며, Append만을 지원합니다. 하나의 행을 삭제하기 위해서는 전체 파일을 다시 생성해야 합니다.
-- MAGIC 
-- MAGIC Delta는 데이터 레이크 상에서 SQL DML 전체를 사용할 수 있습니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DELETE
-- MAGIC 
-- MAGIC Delta는 SQL DML Delete를 지원하여 GDPR 및 CCPA 활용 사례에 사용할 수 있습니다.
-- MAGIC 
-- MAGIC 현재 데이터에 2명의 25세 사용자가 등록되어 있고 방금 이중에서 사용자 아이디 21번을 삭제해야 한다는 요청을 받았습니다(right to be forgotten)

-- COMMAND ----------

SELECT * FROM user_data_bronze_delta WHERE age = 25

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 실습(삭제)

-- COMMAND ----------

-- userid = 21 정보를 user_data_bronze_delta 델타테이블에서 삭제합니다.
-- 삭제 조건 where userid = 21

DELETE FROM user_data_bronze_delta where where userid = 21;

SELECT * FROM user_data_bronze_delta WHERE age = 25

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### UPDATE
-- MAGIC 
-- MAGIC Delta는 간단한 SQL DML로 행을 업데이트 하는 것을 지원합니다.

-- COMMAND ----------

SELECT * FROM user_data_bronze_delta WHERE userid = 1

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC userid 1 사용자의 콜레스테롤 레벨이 정상 수주치로 떨어졌다는 갱신된 정보를 받았습니다.

-- COMMAND ----------

UPDATE user_data_bronze_delta SET chosestlevs = 'Normal' WHERE userid = 1;

SELECT * FROM user_data_bronze_delta where userid = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UPSERT / MERGE
-- MAGIC 
-- MAGIC Delta는 Merge 구문을 사용해서 간단한 upsert를 지원합니다. 34세 사용자의 초기 데이터를 확인해봅시다.

-- COMMAND ----------

SELECT * FROM user_data_bronze_delta WHERE age = 34

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 현재 데이터셋에는 80세 사용자는 없습니다.

-- COMMAND ----------

SELECT * FROM user_data_bronze_delta WHERE age = 80

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 원 데이터셋에 merge 연산을 수행할 신규 데이터를 생성해보시죠. User 2는 기존 정보 중 일부를 업데이트(10 파운드, 약 4.5kg 체중 감소), 신규 사용자(userid=39)는 80세로 기존에는 없는 데이터

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_data_upsert_values
USING DELTA
AS
SELECT 2 AS userid,  "M" AS gender, 34 AS age, 69 AS height, 140 AS weight, "N" AS smoker, "Y" AS familyhistory, "Normal" AS chosestlevs, "Normal" AS bp, -10 AS risk
UNION
SELECT 39 AS userid,  "M" AS gender, 80 AS age, 72 AS height, 155 AS weight, "N" AS smoker, "Y" AS familyhistory, "Normal" AS chosestlevs, "Normal" AS bp, 10 AS risk;

SELECT * FROM user_data_upsert_values;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `user_data_upsert_values`의 정보를 `user_data_bronze_delta` 테이블에 Merge 구문을 실행합니다.
-- MAGIC 실행 후 신규 사용자(userid=39)가 추가되었고, userid 2 사용자는 기존 150에서 140으로 체중이 감소했습니다.

-- COMMAND ----------

MERGE INTO user_data_bronze_delta as target
USING user_data_upsert_values as source
ON target.userid = source.userid
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *;
  
SELECT * FROM user_data_bronze_delta WHERE age = 34 OR age = 80;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 테이블 시간 여행 : History
-- MAGIC 
-- MAGIC Delta는 테이블의 이전 커밋을 트랙킹합니다. history를 볼 수 있고, 이전 상태를 쿼리하거나 롤백하는 것을 지원 합니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 실습(테이블 시간 여행 정보)
-- MAGIC 참고자료 링크  
-- MAGIC https://docs.microsoft.com/ko-kr/azure/databricks/spark/latest/spark-sql/language-manual/sql-ref-syntax-aux-describe-table  
-- MAGIC https://docs.microsoft.com/ko-kr/azure/databricks/spark/latest/spark-sql/language-manual/delta-describe-history

-- COMMAND ----------

-- user_data_bronze_delta 테이블의 정보 확인
-- Describe 사용

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 쿼리 수행 시 가장 최신의 버전이 기본

-- COMMAND ----------

SELECT *
FROM user_data_bronze_delta
WHERE age = 25

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `VERSION AS OF` 명령어를 사용해서 쿼리를 해보겠습니다. 이전에 삭제된 데이터를 볼 수 있습니다. `TIMESTAMP AS OF` 명령어도 있습니다 :
-- MAGIC ```
-- MAGIC SELECT * FROM table_identifier TIMESTAMP AS OF timestamp_expression
-- MAGIC SELECT * FROM table_identifier VERSION AS OF version
-- MAGIC ```

-- COMMAND ----------

SELECT * FROM user_data_bronze_delta
VERSION AS OF 2
WHERE age = 25
--삭제되었던 21번 확인

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### 이전 버전으로 Restore
-- MAGIC `RESTORE` 명령어를 사용해서 이전 상태로 Delta 테이블을 복구할 수 있습니다.
-- MAGIC 
-- MAGIC ⚠️ Databricks Runtime 7.4 and above

-- COMMAND ----------

describe history user_data_bronze_delta

-- COMMAND ----------

RESTORE TABLE user_data_bronze_delta TO VERSION AS OF 8;

SELECT *
FROM user_data_bronze_delta
WHERE age = 25;

-- COMMAND ----------

RESTORE TABLE user_data_bronze_delta TO VERSION AS OF 1;

SELECT *
FROM user_data_bronze_delta
WHERE age = 25;

-- COMMAND ----------

SELECT * FROM device_data_bronze_delta where num_steps > 7000 AND calories_burnt > 500

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

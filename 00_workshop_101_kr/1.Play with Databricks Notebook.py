# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # ![](https://redislabs.com/wp-content/uploads/2016/12/lgo-partners-databricks-125x125.png) Databricks Notebook Quickstart
# MAGIC 데이터브릭스는 Apache Spark™의 제작자가 만든 **통합 분석 플랫폼**으로 데이터 준비, 탐색 및 분석과 머신러닝 어플리케이션의 디플로이까지 전체적인 머신러닝/AI 라이프사이클 구성을 하는데 데이터 엔지니어, 데이터 과학자, 분석가들이 같이 협업할 수 있는 공간을 제공합니다.  
# MAGIC   
# MAGIC <br>  
# MAGIC   
# MAGIC 
# MAGIC <img width="1098" alt="image" src="https://user-images.githubusercontent.com/91228557/168506128-ce86ce8c-5ec3-4a8c-8157-3102a26a37d5.png">
# MAGIC 
# MAGIC 
# MAGIC 노트북은 데이터브릭스에서 코드를 개발하고 수행하는 가장 기본적인 톨입니다. 이 데이터브릭스의 노트북 환경을 통해 다양한 업무들을 협업하는 과정을 알아봅시다!

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1. Multi Language Support with Magic Command!  
# MAGIC 매직컴맨드 **%** 를 사용해서 하나의 노트북에서 다양한 언어를 사용해서 코딩할 수 있습니다. 
# MAGIC 
# MAGIC * **&percnt;python** 
# MAGIC * **&percnt;scala** 
# MAGIC * **&percnt;sql** 
# MAGIC * **&percnt;r** 

# COMMAND ----------

# DBTITLE 1,Markdown
# MAGIC %md
# MAGIC this is text
# MAGIC 
# MAGIC this is `code`
# MAGIC 
# MAGIC # this is a header

# COMMAND ----------

# DBTITLE 1,Python
print("Hello Python!")

# COMMAND ----------

# MAGIC %scala 
# MAGIC println("Hello Scala")

# COMMAND ----------

# MAGIC %r
# MAGIC print("Hello R!", quote=FALSE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 다양한 Utility 지원 

# COMMAND ----------

# DBTITLE 1,%sh 클러스터의 드라이버 노드에서 sh 커맨드를 수행합니다
# MAGIC %sh 
# MAGIC 
# MAGIC ps | grep 'java'

# COMMAND ----------

# DBTITLE 1,%run 다른 노트북을 수행합니다
# MAGIC %run "./Include/Setup"

# COMMAND ----------

# DBTITLE 1,Setup 노트북에서 정의한 변수값을 불러올 수 있습니다
print(databricks_user)

# COMMAND ----------

# MAGIC  
# MAGIC %md 
# MAGIC ### Widget 을 이용한 동적인 변수 활용
# MAGIC 
# MAGIC Databrick utilites (e.g. `dbutils`) 은 notebook에서 유용하게 사용할 수 있는 다양한 기능을 제공합니다: 
# MAGIC https://docs.databricks.com/dev-tools/databricks-utils.html
# MAGIC 
# MAGIC 그중 하나인 **"Widgets"** 기능은 노트북에서 동적인 변수처리를 손쉽게 할 수 있도록 도와줍니다:https://docs.databricks.com/notebooks/widgets.ht

# COMMAND ----------

#Uncomment this to remove all widgets
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("dropdown_widget", "1", [str(x) for x in range(1, 4)])

# COMMAND ----------

print("dropdown_widget 의 현재값은 :", dbutils.widgets.get("dropdown_widget"))

# COMMAND ----------

dbutils.widgets.text("text_widget","Hello World!")

# COMMAND ----------

print(dbutils.widgets.get("text_widget"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks File System - DBFS 을 이용해서 손쉽게 Object Storage 데이터에 접근하기
# MAGIC 또하나의 dbutil중 유용한 기능은 dbutils.fs 를 통해 DBFS를 사용하는 것입니다.  
# MAGIC DBFS 는 cloud-based object storage(S3,ADLS Gen2, GCS등) 에 손쉽게 접근할 수 있는 레이어를 제공합니다.   
# MAGIC 분석이 필요한 데이터가 존재하는 버킷/컨테이너를 DBFS에 마운트하면 접근때마다 권한요청/credential입력등의 번거로운 작업이 필요없습니다. 
# MAGIC 
# MAGIC 기존 Bucket을 마운트 하는 방법(Azure, AWS) [클릭](https://docs.databricks.com/data/databricks-file-system.html#mount-storage).
# MAGIC 
# MAGIC ### Magic Command: &percnt;fs
# MAGIC **&percnt;fs** 매직커맨드는 `dbutils.fs` 에 대한 wrapper 로  ,`dbutils.fs.ls("/databricks-datasets")` 와 `%fs ls /databricks-datasets`는 똑같이 동작합니다.  
# MAGIC 이 노트북에서는 설치시에 기본으로 제공되는 /databricks-datasets 상의 데이터를 사용해 보도록 하겠습니다. 

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/iot-stream/data-device

# COMMAND ----------

# MAGIC %md 
# MAGIC dbfs상의 내용은 메뉴상의 **"Data"** 에서도 탐색할 수 있습니다. 또한 이 파일들을 기반으로 뒤의 실습에서 만들 테이블 metadata들도 마찬가지로 **"Data"** 부분에서 확인할 수 있습니다. 
# MAGIC <img width="808" alt="image" src="https://user-images.githubusercontent.com/91228557/168516585-c6c9b05f-2a95-4526-bcf5-20e90318deee.png">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 3. 데이터 가지고 놀기

# COMMAND ----------

# DBTITLE 1,pyspark dataframe으로 csv파일 읽기
datapath = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"
diamondsDF = spark.read.format("csv")\
              .option("header","true")\
              .option("inferschema","true")\
              .load(datapath)

# COMMAND ----------

from pyspark.sql.functions import avg
# groupBy 를 사용해서 color별 평균 가격을 구해보자
display(diamondsDF.select("color","price").groupBy("color").agg(avg("price")).sort("color"))


# COMMAND ----------

# DBTITLE 1,SQL 을 사용한 diamonds csv 데이터셋 탐색
# MAGIC %sql 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS diamonds;
# MAGIC -- table로 생성한 내용은 왼쪽 Data 메뉴에서 확인이 가능합니다. 
# MAGIC CREATE TABLE diamonds
# MAGIC USING csv
# MAGIC OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true");
# MAGIC 
# MAGIC 
# MAGIC SELECT * FROM diamonds;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 4. 다양한 협업 기능  
# MAGIC   
# MAGIC   
# MAGIC     
# MAGIC 데이터브릭스 노트북은 데이터에 대한 인사이트를 찾아가는 과정을 기업내 다양한 데이터팀이 함께 협업을 통해서 가속화할 수 있도록 다양한 협업 기능과 동시에 Enterprise급 권한 관리 기능을 제공합니다. 
# MAGIC - Comments
# MAGIC - Revision history
# MAGIC - Coediting like Google Docs
# MAGIC - Fine Grain Access Controls for Workspace, Notebook, Clusters, Jobs, and Tables

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

# COMMAND ----------



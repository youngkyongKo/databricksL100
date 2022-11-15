# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC 
# MAGIC # Databricks SQL Demo 
# MAGIC 
# MAGIC 1.SQL Endpoint startup
# MAGIC * Size/Scaling , Autostop , Spot Instance 
# MAGIC * Connection Detail 
# MAGIC * Monitoring 
# MAGIC 2.SQL Editor 
# MAGIC ```
# MAGIC CREATE DATABASE IF NOT EXISTS dbacademy_demo;
# MAGIC USE dbacademy_demo;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS customers USING csv OPTIONS (
# MAGIC   path "/databricks-datasets/retail-org/customers",
# MAGIC   header "true",
# MAGIC   inferSchema "true"
# MAGIC );
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS loyalty_segments USING csv OPTIONS (
# MAGIC   path "/databricks-datasets/retail-org/loyalty_segments",
# MAGIC   header "true",
# MAGIC   inferSchema "true"
# MAGIC );
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS sales_gold USING delta LOCATION "/databricks-datasets/retail-org/solutions/gold/sales";
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver_promo_prices 
# MAGIC USING delta LOCATION "/databricks-datasets/retail-org/solutions/silver/promo_prices";
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver_purchase_orders 
# MAGIC USING delta LOCATION "/databricks-datasets/retail-org/solutions/silver/purchase_orders.delta";
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver_sales_orders 
# MAGIC USING delta LOCATION "/databricks-datasets/retail-org/solutions/silver/sales_orders";
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS source_silver_suppliers 
# MAGIC USING delta LOCATION "/databricks-datasets/retail-org/solutions/silver/suppliers";
# MAGIC 
# MAGIC GRANT USAGE, CREATE, MODIFY, SELECT, READ_METADATA ON DATABASE dbacademy_demo to `users`;
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC * 쿼리 수행   --> bar chart 
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC SELECT product_category, SUM(total_price) total_sales
# MAGIC FROM sales_gold
# MAGIC GROUP BY product_category
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC   
# MAGIC * 쿼리수행 - Chloropleth
# MAGIC 
# MAGIC ```
# MAGIC SELECT
# MAGIC   customers.state,
# MAGIC   COUNT(sales_gold.customer_id) AS cust_count,
# MAGIC   SUM(sales_gold.total_price) sales_revenue
# MAGIC FROM
# MAGIC   sales_gold
# MAGIC   JOIN customers ON customers.customer_id = sales_gold.customer_id
# MAGIC GROUP BY
# MAGIC   (customers.state)
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC General:
# MAGIC Visualization Type: Map (Chloropleth)
# MAGIC Visualization Name: No. of Customers by State
# MAGIC Map: USA
# MAGIC Key Column: state
# MAGIC Target Field: USPS Abbreviation
# MAGIC Value Column: cust_count

# COMMAND ----------



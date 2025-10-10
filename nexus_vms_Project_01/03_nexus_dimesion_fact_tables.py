# Databricks notebook source
# MAGIC %run  /Workspace/nexus_vms_Project_01/00_vms_utilities

# COMMAND ----------

catalog = "vms"
schema_bronze = "schema_bronze"
schema_silver = "schema_silver"
tables = ['vendor_daily_updated']


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vms.schema_silver.dim_vendor
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY VENDOR_ID) AS VENDOR_SK,   
# MAGIC     VENDOR_ID,VENDOR_NAME,CONTACT_NAME,CONTACT_EMAIL,PHONE
# MAGIC FROM vms.schema_silver.vendor_daily_updated
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vms.schema_silver.dim_vendor

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vms.schema_silver.dim_location
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY CITY, STATE, COUNTRY) AS LOCATION_SK,  
# MAGIC     CITY, STATE, COUNTRY
# MAGIC FROM vms.schema_silver.vendor_daily_updated
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  vms.schema_silver.dim_location

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vms.schema_silver.dim_dates
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY CONTRACT_START, CONTRACT_END) AS DATE_SK,
# MAGIC     CONTRACT_START, CONTRACT_END
# MAGIC FROM vms.schema_silver.vendor_daily_updated

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vms.schema_gold.tbl_vendor_detail
# MAGIC AS
# MAGIC SELECT
# MAGIC     f.FACT_SK,
# MAGIC     f.VENDOR_ID,
# MAGIC     v.VENDOR_NAME,
# MAGIC     v.CONTACT_NAME,
# MAGIC     v.CONTACT_EMAIL,
# MAGIC     v.PHONE,
# MAGIC     l.CITY,
# MAGIC     l.STATE,
# MAGIC     l.COUNTRY,
# MAGIC     d.CONTRACT_START,
# MAGIC     d.CONTRACT_END,
# MAGIC     f.TOTAL_ORDERS,
# MAGIC     f.PROJECTED_AMOUNT,
# MAGIC     f.RATING
# MAGIC FROM vms.schema_silver.fact_vendor f
# MAGIC JOIN vms.schema_silver.dim_vendor v
# MAGIC     ON f.VENDOR_SK = v.VENDOR_SK
# MAGIC LEFT JOIN vms.schema_silver.dim_location l
# MAGIC     ON f.LOCATION_SK = l.LOCATION_SK
# MAGIC LEFT JOIN vms.schema_silver.dim_dates d
# MAGIC     ON f.DATE_SK = d.DATE_SK;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SElect * from vms.schema_silver.fact_vendor
# MAGIC limit 10
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vms.schema_gold.tbl_vendor_summary
# MAGIC AS
# MAGIC SELECT
# MAGIC     f.VENDOR_ID,
# MAGIC     v.VENDOR_NAME,
# MAGIC     SUM(f.TOTAL_ORDERS) AS TOTAL_ORDERS,
# MAGIC     SUM(f.PROJECTED_AMOUNT) AS TOTAL_PROJECTED_AMOUNT,
# MAGIC     ROUND(AVG(f.RATING),2) AS AVG_RATING
# MAGIC FROM vms.schema_silver.fact_vendor f
# MAGIC JOIN vms.schema_silver.dim_vendor v
# MAGIC     ON f.VENDOR_SK = v.VENDOR_SK
# MAGIC GROUP BY f.VENDOR_ID, v.VENDOR_NAME;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vms.schema_gold.tbl_vendor_summary
# MAGIC order by VENDOR_ID
# MAGIC limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vms.schema_gold.tbl_vendor_location_summary
# MAGIC AS
# MAGIC SELECT
# MAGIC     l.CITY,
# MAGIC     l.STATE,
# MAGIC     l.COUNTRY,
# MAGIC     COUNT(DISTINCT f.VENDOR_ID) AS TOTAL_VENDORS,
# MAGIC     SUM(f.TOTAL_ORDERS) AS TOTAL_ORDERS,
# MAGIC     SUM(f.PROJECTED_AMOUNT) AS TOTAL_PROJECTED_AMOUNT,
# MAGIC     ROUND(AVG(f.RATING),2) AS AVG_RATING
# MAGIC FROM vms.schema_silver.fact_vendor f
# MAGIC LEFT JOIN vms.schema_silver.dim_location l
# MAGIC     ON f.LOCATION_SK = l.LOCATION_SK
# MAGIC GROUP BY l.CITY, l.STATE, l.COUNTRY;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vms.schema_gold.tbl_vendor_location_summary
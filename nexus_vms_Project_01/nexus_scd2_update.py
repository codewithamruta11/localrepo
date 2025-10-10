# Databricks notebook source
# Set catalog & schema names
catalog = "vms"
schema_bronze = "schema_bronze"
schema_silver = "schema_silver"
table = "vendors_raw_data","vendors_daily_ingest_data","vendor_daily_updated"

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table vms.schema_silver.vendor_daily_updated

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE  or replace TABLE vms.schema_silver.vendor_daily_updated
# MAGIC (
# MAGIC     VENDOR_SK BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), 
# MAGIC     VENDOR_ID INT,
# MAGIC     VENDOR_NAME STRING,
# MAGIC     CONTACT_NAME STRING,
# MAGIC     CONTACT_EMAIL STRING,
# MAGIC     PHONE STRING,
# MAGIC     CITY STRING,
# MAGIC     STATE STRING,
# MAGIC     COUNTRY STRING,
# MAGIC     CONTRACT_START DATE,
# MAGIC     CONTRACT_END DATE,
# MAGIC     TOTAL_ORDERS INT,
# MAGIC     PROJECTED_AMOUNT DOUBLE,
# MAGIC     RATING DOUBLE,
# MAGIC     START_DATE DATE,
# MAGIC     END_DATE DATE,
# MAGIC     IS_CURRENT INT
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO vms.schema_silver.vendor_daily_updated
# MAGIC (
# MAGIC     VENDOR_ID, VENDOR_NAME, CONTACT_NAME, CONTACT_EMAIL, PHONE,
# MAGIC     CITY, STATE, COUNTRY, CONTRACT_START, CONTRACT_END,
# MAGIC     TOTAL_ORDERS, PROJECTED_AMOUNT, RATING,
# MAGIC     START_DATE, END_DATE, IS_CURRENT
# MAGIC )
# MAGIC SELECT
# MAGIC     VENDOR_ID, VENDOR_NAME, CONTACT_NAME, CONTACT_EMAIL, PHONE,
# MAGIC     CITY, STATE, COUNTRY, CONTRACT_START, CONTRACT_END,
# MAGIC     TOTAL_ORDERS, PROJECTED_AMOUNT, RATING,
# MAGIC     DATE('1999-12-09') AS START_DATE,
# MAGIC     DATE('9999-12-31') AS END_DATE,
# MAGIC     1 AS IS_CURRENT
# MAGIC FROM vms.schema_silver.vendors_raw_data;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from vms.schema_silver.vendor_daily_updated
# MAGIC order by vendor_id
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO vms.schema_silver.vendor_daily_updated AS tgt
# MAGIC USING vms.schema_silver.vendors_daily_ingest_data AS src
# MAGIC ON tgt.VENDOR_ID = src.VENDOR_ID
# MAGIC    AND tgt.IS_CURRENT = 1
# MAGIC    AND (
# MAGIC        tgt.VENDOR_NAME    <> src.VENDOR_NAME   OR
# MAGIC        tgt.CONTACT_NAME   <> src.CONTACT_NAME  OR
# MAGIC        tgt.CONTACT_EMAIL  <> src.CONTACT_EMAIL OR
# MAGIC        tgt.PHONE          <> src.PHONE         OR
# MAGIC        tgt.CITY           <> src.CITY          OR
# MAGIC        tgt.STATE          <> src.STATE         OR
# MAGIC        tgt.COUNTRY        <> src.COUNTRY
# MAGIC    )
# MAGIC WHEN MATCHED THEN UPDATE
# MAGIC   SET END_DATE   = current_date() - INTERVAL 1 DAY,
# MAGIC       IS_CURRENT = 0;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO vms.schema_silver.vendor_daily_updated AS tgt
# MAGIC USING vms.schema_silver.vendors_daily_ingest_data AS src
# MAGIC ON tgt.VENDOR_ID = src.VENDOR_ID
# MAGIC    AND tgt.IS_CURRENT = 1
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     VENDOR_ID, VENDOR_NAME, CONTACT_NAME, CONTACT_EMAIL, PHONE,
# MAGIC     CITY, STATE, COUNTRY, CONTRACT_START, CONTRACT_END,
# MAGIC     TOTAL_ORDERS, PROJECTED_AMOUNT, RATING,
# MAGIC     START_DATE, END_DATE, IS_CURRENT
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.VENDOR_ID, src.VENDOR_NAME, src.CONTACT_NAME, src.CONTACT_EMAIL, src.PHONE,
# MAGIC     src.CITY, src.STATE, src.COUNTRY, src.CONTRACT_START, src.CONTRACT_END,
# MAGIC     src.TOTAL_ORDERS, src.PROJECTED_AMOUNT, src.RATING,
# MAGIC     current_date(), DATE('9999-12-31'), 1
# MAGIC   );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vms.schema_silver.vendor_daily_updated
# MAGIC order by VENDOR_ID

# COMMAND ----------

# MAGIC %sql
# MAGIC select VENDOR_ID,CONTACT_NAME,IS_CURRENT,CITY from vms.schema_silver.vendor_daily_updated
# MAGIC order by vendor_id
# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC ### TC_001 - Bronze Layer Data Ingestion
# MAGIC Verify CSV file can be read from Unity Catalog Volumes path with proper schema inference
# MAGIC

# COMMAND ----------

base_path = "/Volumes/vms/schema_bronze/vendors"
bronze_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/*.csv")


# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

display(bronze_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### TC_002 - Check For Duplicate Vendor_id
# MAGIC Verify duplicate data can be remove in data cleaning.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   vendor_id, 
# MAGIC   COUNT(*) 
# MAGIC FROM vms.schema_silver.vendors_raw_data
# MAGIC GROUP BY vendor_id
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY vendor_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### TC_003 - Validate row counts between Bronze and Silver
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS bronze_count FROM vms.schema_bronze.vendors_source_data_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS silver_count FROM vms.schema_bronze.vendors_raw_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ### TC_OO4 -  Validate Schema of Silver Vendor Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT column_name, data_type
# MAGIC FROM system.information_schema.columns
# MAGIC WHERE table_schema = 'schema_silver'
# MAGIC   AND table_name = 'vendors_raw_data'
# MAGIC ORDER BY ordinal_position;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### TC_005 - Phone Number Transformation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     VENDOR_ID,
# MAGIC     PHONE AS original_phone,
# MAGIC     regexp_replace(PHONE, '[^0-9]', '') AS cleaned_phone
# MAGIC FROM vms.schema_silver.vendors_raw_data
# MAGIC WHERE PHONE IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ### TC_006 - Email Address Cleaning

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     VENDOR_ID,
# MAGIC     CONTACT_EMAIL AS original_email,
# MAGIC     LOWER(TRIM(REGEXP_REPLACE(CONTACT_EMAIL, '\\[at\\]', '@'))) AS cleaned_email
# MAGIC FROM vms.schema_bronze.vendors_raw_data
# MAGIC where vendor_id = 6
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### TC_007 -  Vendor Name Uppercase Transformation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     VENDOR_ID,
# MAGIC     VENDOR_NAME AS original_name,
# MAGIC     UPPER(VENDOR_NAME) AS standardized_name
# MAGIC FROM vms.schema_bronze.vendors_source_data_bronze
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### TC_008 - Date Format Conversion (CONTRACT_START),(CONTACT_END)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     VENDOR_ID,
# MAGIC     CONTRACT_START,
# MAGIC     typeof(CONTRACT_START) AS data_type
# MAGIC FROM vms.schema_silver.vendors_raw_data
# MAGIC WHERE CONTRACT_START IS NOT NULL
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     VENDOR_ID,
# MAGIC     CONTRACT_END,
# MAGIC     typeof(CONTRACT_END) AS data_type
# MAGIC FROM vms.schema_silver.vendors_raw_data
# MAGIC WHERE CONTRACT_END IS NOT NULL
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### TC_009 - Validate end date is after start date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     VENDOR_ID,
# MAGIC     CONTRACT_START,
# MAGIC     CONTRACT_END,
# MAGIC     DATEDIFF(CONTRACT_END, CONTRACT_START) AS contract_days,
# MAGIC     CASE 
# MAGIC         WHEN CONTRACT_END > CONTRACT_START THEN 'Valid'
# MAGIC         ELSE 'Invalid'
# MAGIC     END AS validity_check
# MAGIC FROM vms.schema_silver.vendors_raw_data
# MAGIC WHERE CONTRACT_START IS NOT NULL AND CONTRACT_END IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ### TC_010 Data Type Casting to Correct Types

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE vms.schema_bronze.vendors_source_data_bronze;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE vms.schema_silver.vendors_raw_data;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 
# MAGIC ### TC_011 Column Names to Uppercase

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH bronze_cols AS (
# MAGIC     SELECT column_name
# MAGIC     FROM system.information_schema.columns
# MAGIC     WHERE table_schema = 'schema_bronze'
# MAGIC       AND table_name = 'vendors_source_data_bronze'
# MAGIC ),
# MAGIC silver_cols AS (
# MAGIC     SELECT column_name
# MAGIC     FROM system.information_schema.columns
# MAGIC     WHERE table_schema = 'schema_silver'
# MAGIC       AND table_name = 'vendors_raw_data'
# MAGIC )
# MAGIC SELECT 
# MAGIC     b.column_name AS bronze_column,
# MAGIC     s.column_name AS silver_column,
# MAGIC     CASE 
# MAGIC         WHEN s.column_name = UPPER(b.column_name) THEN 'UPPERCASE_OK'
# MAGIC         ELSE 'NOT_UPPERCASE'
# MAGIC     END AS validation_result
# MAGIC FROM bronze_cols b
# MAGIC JOIN silver_cols s 
# MAGIC     ON LOWER(b.column_name) = LOWER(s.column_name);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### TC_012 - Null Value Validation in VENDOR_ID

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_records,
# MAGIC     COUNT(VENDOR_ID) AS non_null_vendor_ids,
# MAGIC     COUNT(*) - COUNT(VENDOR_ID) AS null_count
# MAGIC FROM vms.schema_silver.vendors_raw_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_records,
# MAGIC     COUNT(VENDOR_ID) AS non_null_vendor_ids,
# MAGIC     COUNT(*) - COUNT(VENDOR_ID) AS null_count
# MAGIC FROM vms.schema_silver.vendor_daily_updated;

# COMMAND ----------

# MAGIC %md
# MAGIC ### TC_013 Validate Silver receives incremental records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM vms.schema_silver.vendors_raw_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM vms.schema_silver.vendor_daily_updated
# MAGIC where IS_CURRENT=1

# COMMAND ----------

# MAGIC %md
# MAGIC ### TC_014 - Validate Vendor Location Change in SCD2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     v1.VENDOR_ID,v1.VENDOR_NAME,
# MAGIC     v1.CITY AS OLD_CITY,
# MAGIC     v1.START_DATE AS OLD_START,
# MAGIC     v1.END_DATE AS OLD_END,
# MAGIC     v2.CITY AS NEW_CITY,
# MAGIC     v2.START_DATE AS NEW_START,
# MAGIC     v2.END_DATE AS NEW_END,v2.IS_CURRENT
# MAGIC FROM vms.schema_silver.vendor_daily_updated v1
# MAGIC JOIN vms.schema_silver.vendor_daily_updated v2
# MAGIC   ON v1.VENDOR_ID = v2.VENDOR_ID
# MAGIC  AND v1.CITY <> v2.CITY
# MAGIC  AND v1.IS_CURRENT = 0
# MAGIC  AND v2.IS_CURRENT = 1
# MAGIC ORDER BY v1.VENDOR_ID, v1.END_DATE;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### TC_015 1. Validate Current Record

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT VENDOR_ID
# MAGIC FROM vms.schema_silver.vendor_daily_updated
# MAGIC GROUP BY VENDOR_ID
# MAGIC HAVING SUM(CASE WHEN IS_CURRENT = 1 THEN 1 ELSE 0 END) <> 1;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### TC_016 - Validate Historical Records Ended Properly

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT VENDOR_ID, CITY, END_DATE
# MAGIC FROM vms.schema_silver.vendor_daily_updated
# MAGIC WHERE IS_CURRENT = 0 AND END_DATE = DATE('9999-12-31');
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### TC_017 - Validate no duplicate SCD2 updates on re-run

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS row_count_before 
# MAGIC FROM vms.schema_silver.vendor_daily_updated;
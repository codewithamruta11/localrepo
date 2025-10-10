# Databricks notebook source
# Set catalog & schema names
catalog = "vms"
schema_bronze = "schema_bronze"
schema_silver = "schema_silver"
table = "vendors_raw_data","vendors_daily_ingest_data","vendor_daily_updated"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS TOTAL_RAWS
# MAGIC FROM vms.schema_silver.vendor_daily_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW COLUMNS FROM vms.schema_silver.vendor_daily_updated

# COMMAND ----------

# MAGIC %sql
# MAGIC select VENDOR_ID,CONTACT_NAME,START_DATE,END_DATE,IS_CURRENT from vms.schema_silver.vendor_daily_updated
# MAGIC order by vendor_id
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select VENDOR_ID,CONTACT_NAME,START_DATE,END_DATE,IS_CURRENT from vms.schema_silver.vendor_daily_updated
# MAGIC where IS_CURRENT = 1
# MAGIC order by vendor_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select VENDOR_ID,CONTACT_NAME,START_DATE,END_DATE,IS_CURRENT from vms.schema_silver.vendor_daily_updated
# MAGIC where IS_CURRENT = 0
# MAGIC order by vendor_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS TOTAL_RAWS
# MAGIC FROM vms.schema_silver.dim_vendor;

# COMMAND ----------


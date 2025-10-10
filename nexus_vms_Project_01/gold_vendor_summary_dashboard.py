# Databricks notebook source
# MAGIC %md
# MAGIC #Gold Layer – BI Ready Tables

# COMMAND ----------

catlog = "vms"
schema = "schema_gold","schema_silver"

# COMMAND ----------

# MAGIC %md
# MAGIC Detailed Vendor Table - Full transactional detail for all vendors × locations × contracts

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

# MAGIC %md
# MAGIC  Vendor Summary Table - Aggregated totals per vendor (TOTAL_ORDERS, PROJECTED_AMOUNT, AVG_RATING)

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

# MAGIC %md
# MAGIC Location Summary Table : Aggregated totals per location (TOTAL_ORDERS, TOTAL_PROJECTED_AMOUNT, AVG_RATING)

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
# Databricks notebook source
# MAGIC %md
# MAGIC ### 1.Vendor Performance Summary -
# MAGIC This query provides a high-level overview of vendor performance metrics across the dataset. It aggregates key KPIs (Key Performance Indicators) such as total vendors, active vendors, total orders, projected revenue, and average rating — all in one view.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT VENDOR_ID) AS TOTAL_VENDORS,
# MAGIC     COUNT(DISTINCT CASE WHEN TOTAL_ORDERS > 0 THEN VENDOR_ID END) AS ACTIVE_VENDORS,
# MAGIC     SUM(TOTAL_ORDERS) AS TOTAL_ORDERS,
# MAGIC     SUM(PROJECTED_AMOUNT) AS TOTAL_PROJECTED_AMOUNT,
# MAGIC     ROUND(AVG(RATING),2) AS AVG_RATING
# MAGIC FROM vms.schema_silver.vendor_daily_updated;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.Vendor Order Trend Over Time -
# MAGIC This query analyzes the order volume and projected business value (revenue) over time, based on vendor contract start dates. It helps track how vendor activity and expected revenue have changed across different time periods.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CONTRACT_START AS DATE,
# MAGIC     SUM(TOTAL_ORDERS) AS TOTAL_ORDERS,
# MAGIC     SUM(PROJECTED_AMOUNT) AS TOTAL_PROJECTED_AMOUNT
# MAGIC FROM vms.schema_silver.vendor_daily_updated
# MAGIC GROUP BY CONTRACT_START
# MAGIC ORDER BY CONTRACT_START;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.Vendor Performance by Location -
# MAGIC This query analyzes vendor activity and performance across different locations (city, state, and country). It helps identify regions contributing the most to total orders, projected revenue, and vendor quality (rating).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CITY,
# MAGIC     STATE,
# MAGIC     COUNTRY,
# MAGIC     COUNT(DISTINCT VENDOR_ID) AS TOTAL_VENDORS,
# MAGIC     SUM(TOTAL_ORDERS) AS TOTAL_ORDERS,
# MAGIC     SUM(PROJECTED_AMOUNT) AS TOTAL_PROJECTED_AMOUNT,
# MAGIC     ROUND(AVG(RATING),2) AS AVG_RATING
# MAGIC FROM vms.schema_silver.vendor_daily_updated
# MAGIC GROUP BY CITY, STATE, COUNTRY
# MAGIC ORDER BY TOTAL_ORDERS DESC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CONCAT(CITY, ', ', STATE) AS LOCATION,
# MAGIC   COUNT(DISTINCT VENDOR_ID) AS TOTAL_VENDORS
# MAGIC FROM vms.schema_silver.vendor_daily_updated
# MAGIC GROUP BY
# MAGIC   CITY, STATE,COUNTRY
# MAGIC ORDER BY
# MAGIC   TOTAL_VENDORS DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.Top Vendors by Orders and Performance - 
# MAGIC This query provides a detailed vendor-level performance report, ranking vendors by their total number of orders. It helps business analysts and BI teams identify top-performing vendors, their projected business value, contract periods, locations.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     VENDOR_ID,
# MAGIC     VENDOR_NAME,
# MAGIC     TOTAL_ORDERS,
# MAGIC     PROJECTED_AMOUNT,
# MAGIC     RATING,
# MAGIC     CONTRACT_START,
# MAGIC     CONTRACT_END,
# MAGIC     CITY, STATE, COUNTRY
# MAGIC FROM vms.schema_silver.vendor_daily_updated
# MAGIC ORDER BY TOTAL_ORDERS DESC
# MAGIC limit 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 Vendors by Orders - Highlight top-performing vendors in  charts.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.Vendor Rating Distribution - 
# MAGIC This query provides a summary report on the distribution of vendor quality by counting the number of vendors at each rating level. It helps Quality Assurance and Vendor Management teams quickly assess the overall health and performance profile of the vendor pool.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     RATING,
# MAGIC     COUNT(*) AS NUM_VENDORS
# MAGIC FROM vms.schema_silver.vendor_daily_updated
# MAGIC GROUP BY RATING
# MAGIC ORDER BY RATING DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.Orders vs Projected Amount Scatter - 
# MAGIC  This chart will show the correlation between the number of orders a vendor handles and their projected monetary value. You can easily spot vendors who deliver high order volume but low projected value, or vice-versa.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     TOTAL_ORDERS,
# MAGIC     PROJECTED_AMOUNT,
# MAGIC     VENDOR_ID,
# MAGIC     VENDOR_NAME
# MAGIC FROM vms.schema_silver.vendor_daily_updated
# MAGIC WHERE TOTAL_ORDERS IS NOT NULL
# MAGIC   AND PROJECTED_AMOUNT IS NOT NULL;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Vendors Without Orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     VENDOR_ID,
# MAGIC     VENDOR_NAME,
# MAGIC     TOTAL_ORDERS
# MAGIC FROM vms.schema_silver.vendor_daily_updated
# MAGIC WHERE TOTAL_ORDERS IS NULL OR TOTAL_ORDERS = 0;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.City-Wise Average Rating - 
# MAGIC This query is perfect for generating a City-Wise Average Rating report. The best way to visualize this data is using a Horizontal Bar Chart or a Table with Conditional Formatting.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CITY,
# MAGIC     ROUND(AVG(RATING), 2) AS AVG_RATING
# MAGIC FROM vms.schema_silver.vendor_daily_updated
# MAGIC GROUP BY CITY
# MAGIC ORDER BY AVG_RATING DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.Top 10 Vendors by Total Projected Amount

# COMMAND ----------

# MAGIC %md
# MAGIC ### 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     VENDOR_NAME,
# MAGIC     SUM(PROJECTED_AMOUNT) AS TOTAL_AMOUNT,
# MAGIC     SUM(TOTAL_ORDERS) AS TOTAL_ORDERS
# MAGIC FROM vms.schema_silver.vendor_daily_updated
# MAGIC WHERE VENDOR_NAME IS NOT NULL
# MAGIC GROUP BY VENDOR_NAME
# MAGIC HAVING SUM(TOTAL_ORDERS) > 0
# MAGIC ORDER BY TOTAL_AMOUNT DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.Vendors and orders trend over time

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DATE(CONTRACT_START) AS start_date,
# MAGIC   COUNT(DISTINCT VENDOR_ID) AS total_vendors,
# MAGIC   SUM(TOTAL_ORDERS) AS total_orders,
# MAGIC   ROUND(AVG(RATING),2) AS avg_rating
# MAGIC FROM vms.schema_silver.vendor_daily_updated
# MAGIC where CONTRACT_START IS NOT NULL
# MAGIC GROUP BY DATE(CONTRACT_START)
# MAGIC ORDER BY start_date;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rating distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   RATING,
# MAGIC   COUNT(*) AS cnt
# MAGIC FROM vms.schema_silver.vendor_daily_updated
# MAGIC WHERE RATING IS NOT NULL
# MAGIC GROUP BY RATING
# MAGIC ORDER BY RATING;
# MAGIC
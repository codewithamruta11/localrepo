# Databricks notebook source
# MAGIC %run /Workspace/nexus_vms_Project_01/00_vms_utilities

# COMMAND ----------

# Set catalog & schema names
catalog = "vms"
schema_bronze = "schema_bronze"
schema_silver = "schema_silver"



# COMMAND ----------

# Path in Unity Catalog Volumes (Bronze layer)
base_path = "/Volumes/vms/schema_bronze/vendor_stream"

# Batch-1 file 
vms_batch1 = f"{base_path}/vendors_stream1.csv"

# Load the CSV in  Bronze DataFrame
bronze_df =spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/*.csv")




# COMMAND ----------

# Save as a managed Delta table in Bronze
save_delta(
    bronze_df,
    catalog,
    schema_bronze,
    "vendors_stream_data_bronze"
)

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

display(bronze_df.limit(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vms.schema_bronze.vendors_stream_data_bronze

# COMMAND ----------

silver_df = (
    bronze_df
    # Fix phone numbers
    .transform(lambda df: fix_phone(df, "phone"))

    # Clean email
    .transform(lambda df: clean_email(df, "contact_email"))

    # Uppercase vendor_name
    .transform(lambda df: uppercase_column_data(df, "vendor_name"))

    # Remove duplicates based on vendor_id
    .transform(lambda df: remove_duplicates(df, ["vendor_id"]))

    # Convert date strings
    .withColumn("contract_start", convert_date_format(col("contract_start")))
    .withColumn("contract_end",   convert_date_format(col("contract_end")))

    # Validate: filter rows where vendor_id is not null
    .transform(lambda df: df.filter(col("VENDOR_ID").isNotNull()))
   
    # Cast to correct data types
    .transform(change_column_types)
               
    # Fix date columns
    .transform(fix_date_columns)

    #convert all column names to UPPERCASE
    .transform(uppercase_columns)
)


# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

save_delta(silver_df, catalog, schema_silver, "vendors_daily_ingest_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vms.schema_silver.vendors_daily_ingest_data
# Databricks notebook source
#Import the DataFrame better readability in function .
from pyspark.sql import DataFrame

from pyspark.sql.window import Window

# Import commonly used PySpark SQL functions for data cleaning and transformation.
from pyspark.sql.functions import col, trim, to_date, lit, when, upper, current_timestamp, to_timestamp, monotonically_increasing_id,regexp_replace,coalesce,StringType,date_format,row_number

# Import datetime module for working with dates and times in Python code.
from datetime import datetime

# Regular expressions module for complex pattern matching 
import re

#Import DeltaTable class to perform advanced Delta Lake operations such as MERGE (upsert), UPDATE, DELETE.
from delta.tables import DeltaTable


# Import specific Spark SQL data types for explicit schema definitions.
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType




# COMMAND ----------

#Load a CSV file into a Spark DataFrame.
def load_csv(path: str, header: bool = True, infer_schema: bool = False) :
    df = (spark.read.format("csv")
                 .option("header", str(header).lower())
                 .option("inferSchema", str(infer_schema).lower())
                 .load(path))
    return df

# COMMAND ----------

#Save a DataFrame as a managed Delta table in the format: <catalog>.<schema>.<table_name>.
def save_delta(
    df: DataFrame,
    catalog: str,
    schema: str,
    table_name: str,
    mode: str = "overwrite"
):
    full_table_name = f"{catalog}.{schema}.{table_name}"
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )
    print(f"DataFrame saved as Delta table: {full_table_name}")

# COMMAND ----------

#reads multiple tables from the bronze layer.
def read_bronze_tables(catalog, schema, table_names):
    dfs = {}
    for t in table_names:
        dfs[t] = spark.table(f"{catalog}.{schema}.{t}")
    return dfs

# COMMAND ----------

#Converts all column names to uppercase.
def uppercase_columns(df):
    return df.toDF(*[c.upper() for c in df.columns])

# COMMAND ----------

#Rename all columns to uppercase using withColumnRenamed.
def rename_columns_upper(df: DataFrame) :
    for c in df.columns:
        df = df.withColumnRenamed(c, c.upper())
    return df



# COMMAND ----------

#Rename all columns to uppercase using withColumnRenamed.
def rename_columns_upper(df: DataFrame) :
    for c in df.columns:
        df = df.withColumnRenamed(c, c.upper())
    return df



# COMMAND ----------

#Convert a messy date string into YYYY-MM-DD.
#Returns None if the string is empty or not a valid date.
def convert_to_iso(date_str):
    if not date_str:
        return None
     #Remove anything except digits, slash or hyphen
    s = re.sub(r"[^0-9/\-]", "", date_str.strip())
    s = s.replace("-", "/")
    for fmt in ("%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None
convert_date_format = udf(convert_to_iso, StringType())


# COMMAND ----------

#validate Data -Capture invalid rows 
def validate_vendors(df: DataFrame) :
    return df.filter(col("df").isNull())

# COMMAND ----------

#  Drop duplicate rows based on subset of columns.
def remove_duplicates(df, subset_cols=["vedor_id"]):
    return df.dropDuplicates(subset=subset_cols)



# COMMAND ----------

#Convert all values in a column to uppercase.
def uppercase_column_data(df, col_name):
    if col_name in df.columns:
        df = df.withColumn(col_name, upper(col(col_name)))
    return df




# COMMAND ----------


#  Clean email: replace '[at]' with '@'
def clean_email(df: DataFrame, email_col: str = "contact_email"):
    return df.withColumn(
        email_col,
        regexp_replace(col(email_col), r"\[at\]", "@"))

# COMMAND ----------

#fix phone
def fix_phone(df, phone_col="phone"):
    return df.withColumn(
        phone_col,
        when(
            trim(col(phone_col)).rlike("^[0-9]{10,}$"),
            trim(col(phone_col))
        ).otherwise(lit(None))
    )



# COMMAND ----------

def change_column_types(df):
    return (
        df
        .withColumn("vendor_id", col("vendor_id").cast("int"))
        .withColumn("vendor_name", col("vendor_name").cast("string"))
        .withColumn("contact_name", col("contact_name").cast("string"))
        .withColumn("contact_email", col("contact_email").cast("string"))
        .withColumn("phone", col("phone").cast("string"))
        .withColumn("city", col("city").cast("string"))
        .withColumn("state", col("state").cast("string"))
        .withColumn("country", col("country").cast("string"))
        .withColumn("contract_start", col("contract_start").cast("string"))
        .withColumn("contract_end", col("contract_end").cast("string"))
        .withColumn("total_orders", col("total_orders").cast("int"))
        .withColumn("projected_amount", col("projected_amount").cast("double"))
        .withColumn("rating", col("rating").cast("double"))
    )

# COMMAND ----------

#
def fix_date_columns(df):
    return (
        df
        .withColumn("contract_start",
            regexp_replace(col("contract_start"), "/", "-").cast("date"))
        .withColumn("contract_end",
            regexp_replace(col("contract_end"), "/", "-").cast("date"))
    )



# COMMAND ----------

#create function for dimension
def create_dimension(
    df: DataFrame,
    dim_name: str,
    catalog: str,
    schema: str,
    natural_cols: list,
    sk_col: str = "surrogate_sk"   
) :
    # Remove duplicates
    dim_df = df.select(natural_cols).dropDuplicates()

    # Add auto-incrementing surrogate key
    window_spec = Window.orderBy(lit(1))  # dummy ordering
    dim_df = dim_df.withColumn(sk_col, row_number().over(window_spec))

    # Reorder columns to have surrogate key first
    dim_df = dim_df.select([sk_col] + natural_cols)

    # Full table name
    full_table_name = f"{catalog}.{schema}.{dim_name}"

    # Save as Delta table
    dim_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)

    print(f"Dimension table saved: {full_table_name}")
    return dim_df


# COMMAND ----------

#create function for fact
def create_fact_table(
    df: DataFrame,
    fact_name: str,
    catalog: str,
    schema: str,
    fact_cols: list,
    sk_col: str = "fact_sk"
):
    # Select only fact columns and remove duplicates
    fact_df = df.select(fact_cols).dropDuplicates()

    # Add auto-incrementing surrogate key
    window_spec = Window.orderBy(lit(1))  # dummy ordering
    fact_df = fact_df.withColumn(sk_col, row_number().over(window_spec))

    # Reorder columns to have surrogate key first
    fact_df = fact_df.select([sk_col] + fact_cols)

    # Full table name
    full_table_name = f"{catalog}.{schema}.{fact_name}"

    # Save as Delta table
    fact_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)

    print(f"Fact table saved: {full_table_name}")
    return fact_df

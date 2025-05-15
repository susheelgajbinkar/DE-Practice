# Databricks notebook source
# MAGIC %md
# MAGIC Define the Table List

# COMMAND ----------

# List of tables and their primary key columns
tables = [
    {"name": "media_customer_reviews", "key": "franchiseID", "path": "/FileStore/tables/media_customer_reviews.parquet"},
    {"name": "media_gold_reviews_chunked", "key": "franchiseID", "path": "/FileStore/tables/media_gold_reviews_chunked.parquet"},
    {"name": "sales_customers", "key": "customerID", "path": "/FileStore/tables/sales_customers.parquet"},
    {"name": "sales_franchises", "key": "franchiseID", "path": "/FileStore/tables/sales_franchises.parquet"},
    {"name": "sales_suppliers", "key": "supplierID", "path": "/FileStore/tables/sales_suppliers.parquet"},
    {"name": "sales_transactions", "key": "transactionID", "path": "/FileStore/tables/sales_transactions.parquet"}
]

# COMMAND ----------

# MAGIC %md
# MAGIC Create Schemas - Bronze, Silver & Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic Bronze -> Silver -> Gold ETL Function

# COMMAND ----------

def run_etl_pipeline(table_name: str, key: str, file_path: str):
    bronze = f"bronze.{table_name}"
    silver = f"silver.{table_name}"
    
    # Step 1: Read raw file
    df = spark.read.parquet(file_path)
    
    # Step 2: Write as bronze table
    df.write.format("delta").mode("overwrite").saveAsTable(bronze)
    
    # Step 3: Create/overwrite silver table
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver)
    
    # Step 4: Generate SCD Type 1 MERGE SQL
    columns = df.columns
    update_expr = ", ".join([f"target.{c} = source.{c}" for c in columns])
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join([f"source.{c}" for c in columns])
    
    merge_sql = f"""
    MERGE INTO {silver} AS target
    USING {bronze} AS source
    ON target.{key} = source.{key}
    WHEN MATCHED THEN UPDATE SET {update_expr}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    
    # Step 5: Execute merge
    spark.sql(merge_sql)
    
    # Step 6: Create a gold table with sample aggregation
    if "name" in columns:
        gold_sql = f"""
        CREATE OR REPLACE TABLE gold.{table_name}_record_count AS
        SELECT name, COUNT(*) AS total_records
        FROM {silver}
        GROUP BY name
        """
        spark.sql(gold_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC Loop through All Tables

# COMMAND ----------

for t in tables:
    run_etl_pipeline(t["name"], t["key"], t["path"])

# COMMAND ----------

# MAGIC %md
# MAGIC Query the Gold Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.media_customer_reviews;
# Databricks notebook source
# MAGIC %md
# MAGIC # Chicago Crime Reference Data DLT Pipeline
# MAGIC 
# MAGIC Delta Live Tables for reference/dimension data:
# MAGIC - Community Areas
# MAGIC - IUCR Codes
# MAGIC - Wards

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, lpad, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# 从 Pipeline 配置获取参数
env = spark.conf.get("env", "dev")
storage_account = spark.conf.get("storage_account", "kevintestdatabricks")

# Reference data 路径
reference_path = f"abfss://reference@{storage_account}.dfs.core.windows.net/ref/"

print(f"Environment: {env}")
print(f"Reference Path: {reference_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference Tables

# COMMAND ----------

@dlt.table(
    name="ref_community_areas",
    comment="Chicago community areas reference data",
    table_properties={
        "quality": "reference",
        "pipelines.autoOptimize.managed": "true"
    }
)
def ref_community_areas():
    """
    Load community areas reference data from CSV.
    """
    return (spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{reference_path}community_areas.csv")
        .withColumn("_loaded_at", current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="ref_iucr_codes",
    comment="Illinois Uniform Crime Reporting codes",
    table_properties={
        "quality": "reference",
        "pipelines.autoOptimize.managed": "true"
    }
)
def ref_iucr_codes():
    """
    Load IUCR codes reference data from JSON.
    - IUCR codes are padded to 4 characters (e.g., 281 -> 0281)
    """
    return (spark.read
        .format("json")
        .option("multiLine", "true")
        .option("primitivesAsString", "true")  # Keep IUCR as string
        .load(f"{reference_path}iucr_codes.json")
        # Ensure IUCR is 4 characters with leading zeros
        .withColumn("iucr", lpad(col("iucr"), 4, "0"))
        .withColumn("_loaded_at", current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="ref_wards",
    comment="Chicago wards reference data",
    table_properties={
        "quality": "reference",
        "pipelines.autoOptimize.managed": "true"
    }
)
def ref_wards():
    """
    Load wards reference data from JSON.
    """
    return (spark.read
        .format("json")
        .option("multiLine", "true")
        .load(f"{reference_path}wards_current.json")
        .withColumn("_loaded_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enriched Silver Table (Join with Reference Data)

# COMMAND ----------

@dlt.table(
    name="silver_crime_enriched",
    comment="Crime data enriched with reference data (community area names, IUCR descriptions)",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def silver_crime_enriched():
    """
    Join silver crime data with reference tables for enriched analytics.
    """
    # Read silver crime data (from main pipeline, need to reference the table)
    # Note: This assumes the main crime pipeline runs first
    silver_crime = spark.table(f"{env}_catalog.crime_data_{env}.silver_crime")
    ref_iucr = dlt.read("ref_iucr_codes")
    ref_community = dlt.read("ref_community_areas")
    
    return (silver_crime
        # Join with IUCR codes
        .join(
            ref_iucr.select(
                col("iucr"),
                col("primary_description").alias("iucr_primary_desc"),
                col("secondary_description").alias("iucr_secondary_desc")
            ),
            on="iucr",
            how="left"
        )
        # Join with community areas
        .join(
            ref_community.select(
                col("community_area").cast("int").alias("community_area"),
                col("community_name")
            ),
            on="community_area",
            how="left"
        )
    )

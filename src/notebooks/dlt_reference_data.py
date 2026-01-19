# Databricks notebook source
# MAGIC %md
# MAGIC # Chicago Crime Reference Data DLT Pipeline

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, lpad, current_timestamp

# COMMAND ----------

# 配置
env = spark.conf.get("env", "dev")
storage_account = spark.conf.get("storage_account", "kevintestdatabricks")
reference_path = f"abfss://reference@{storage_account}.dfs.core.windows.net/ref/"

# COMMAND ----------

@dlt.table(
    name="ref_community_areas",
    comment="Chicago community areas reference data"
)
def ref_community_areas():
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
    comment="Illinois Uniform Crime Reporting codes"
)
def ref_iucr_codes():
    return (spark.read
        .format("json")
        .option("multiLine", "true")
        .option("primitivesAsString", "true")
        .load(f"{reference_path}iucr_codes.json")
        .withColumn("iucr", lpad(col("iucr"), 4, "0"))
        .withColumn("_loaded_at", current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="ref_wards",
    comment="Chicago wards reference data"
)
def ref_wards():
    return (spark.read
        .format("json")
        .option("multiLine", "true")
        .load(f"{reference_path}wards_current.json")
        .withColumn("_loaded_at", current_timestamp())
    )

# COMMAND ---------
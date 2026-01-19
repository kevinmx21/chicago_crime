# Databricks notebook source
# MAGIC %md
# MAGIC # Chicago Crime DLT Pipeline
# MAGIC 
# MAGIC Delta Live Tables pipeline for Chicago crime data processing:
# MAGIC - **Bronze**: Raw data ingestion with AutoLoader
# MAGIC - **Silver**: Cleaned, deduplicated, typed data
# MAGIC - **Gold**: Aggregated analytics tables

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, current_timestamp, lit, count, sum as spark_sum,
    when, year, month, dayofweek, hour, to_timestamp,
    avg, round as spark_round, countDistinct  # ← 添加这个
)
from pyspark.sql.types import FloatType, IntegerType, BooleanType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# 从 Pipeline 配置获取参数
env = spark.conf.get("env", "dev")
storage_account = spark.conf.get("storage_account", "kevintestdatabricks")

# 源数据路径
source_path = f"abfss://data@{storage_account}.dfs.core.windows.net/"
checkpoint_path = f"abfss://{env}@{storage_account}.dfs.core.windows.net/_checkpoints/dlt/"

print(f"Environment: {env}")
print(f"Source Path: {source_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Raw Data Ingestion

# COMMAND ----------

@dlt.table(
    name="bronze_crime",
    comment="Raw Chicago crime data ingested via AutoLoader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.reset.allowed": "false"  # ← 禁止重置
    }
)
def bronze_crime():
    """
    Ingest raw crime data from Azure Storage using AutoLoader.
    - Supports incremental loading
    - Schema evolution enabled
    """
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}bronze/schema/")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load(source_path)
        .withColumn("_ingestion_time", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_env", lit(env))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Cleaned & Transformed Data

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Enriched Data (with Reference)

# COMMAND ----------

@dlt.table(
    name="silver_crime_enriched",
    comment="Crime data enriched with reference data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.reset.allowed": "false"  # ← 禁止重置
    }
)
def silver_crime_enriched():
    """
    Join silver crime data with reference tables.
    Note: Reference tables are read from catalog (managed by separate pipeline)
    """
    silver_crime = dlt.read("silver_crime")
    
    # 从 catalog 读取 reference 表（另一个 pipeline 管理）
    ref_iucr = spark.table(f"{spark.conf.get('env', 'dev')}_catalog.crime_data_{spark.conf.get('env', 'dev')}.ref_iucr_codes")
    ref_community = spark.table(f"{spark.conf.get('env', 'dev')}_catalog.crime_data_{spark.conf.get('env', 'dev')}.ref_community_areas")
    
    # 准备 lookup 表
    community_lookup = ref_community.select(
        col("AREA_NUMBE").cast("string").alias("area_num"),
        col("COMMUNITY").alias("community_name")
    )
    
    iucr_lookup = ref_iucr.select(
        col("iucr"),
        col("primary_description").alias("iucr_primary_desc"),
        col("secondary_description").alias("iucr_secondary_desc")
    )
    
    return (silver_crime
        .join(iucr_lookup, on="iucr", how="left")
        .join(
            community_lookup,
            silver_crime["community_area"].cast("string") == community_lookup["area_num"],
            how="left"
        )
        .drop("area_num")
    )

@dlt.table(
    name="silver_crime",
    comment="Cleaned, deduplicated, and typed crime data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.reset.allowed": "false"  # ← 禁止重置
    }
)
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
@dlt.expect_or_drop("valid_case_number", "case_number IS NOT NULL")
@dlt.expect("valid_coordinates", "latitude IS NOT NULL AND longitude IS NOT NULL")
@dlt.expect("valid_year", "year >= 2000 AND year <= 2030")
def silver_crime():
    """
    Clean and transform crime data:
    - Cast coordinates to float
    - Deduplicate by ID
    - Parse dates
    - Data quality expectations
    """
    return (dlt.read_stream("bronze_crime")
        # Type conversions
        .withColumn("id", col("id").cast(IntegerType()))
        .withColumn("latitude", col("latitude").cast(FloatType()))
        .withColumn("longitude", col("longitude").cast(FloatType()))
        .withColumn("x_coordinate", col("x_coordinate").cast(FloatType()))
        .withColumn("y_coordinate", col("y_coordinate").cast(FloatType()))
        .withColumn("year", col("year").cast(IntegerType()))
        .withColumn("beat", col("beat").cast(IntegerType()))
        .withColumn("district", col("district").cast(IntegerType()))
        .withColumn("ward", col("ward").cast(IntegerType()))
        .withColumn("community_area", col("community_area").cast(IntegerType()))
        
        # Parse crime date
        .withColumn("crime_datetime", to_timestamp(col("date")))
        .withColumn("crime_year", year(col("crime_datetime")))
        .withColumn("crime_month", month(col("crime_datetime")))
        .withColumn("crime_day_of_week", dayofweek(col("crime_datetime")))
        .withColumn("crime_hour", hour(col("crime_datetime")))
        
        # Boolean conversions
        .withColumn("arrest", col("arrest").cast(BooleanType()))
        .withColumn("domestic", col("domestic").cast(BooleanType()))
        
        # Add processing metadata
        .withColumn("_silver_processed_time", current_timestamp())
        
        # Deduplicate
        .dropDuplicates(["id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Analytics Tables

# COMMAND ----------

@dlt.table(
    name="gold_crime_by_type",
    comment="Crime statistics aggregated by crime type and year",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.reset.allowed": "false"  # ← 禁止重置
    }
)
def gold_crime_by_type():
    """
    Aggregate crime statistics by primary type and year.
    """
    return (dlt.read("silver_crime")
        .groupBy("primary_type", "crime_year")
        .agg(
            count("*").alias("total_crimes"),
            spark_sum(when(col("arrest") == True, 1).otherwise(0)).alias("total_arrests"),
            spark_sum(when(col("domestic") == True, 1).otherwise(0)).alias("domestic_crimes"),
            spark_round(
                spark_sum(when(col("arrest") == True, 1).otherwise(0)) / count("*") * 100, 2
            ).alias("arrest_rate_pct")
        )
        .orderBy("crime_year", "total_crimes", ascending=[True, False])
    )

# COMMAND ----------

@dlt.table(
    name="gold_crime_by_location",
    comment="Crime statistics aggregated by community area",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.reset.allowed": "false"  # ← 禁止重置
    }
)
def gold_crime_by_location():
    """
    Aggregate crime statistics by community area and district.
    """
    return (dlt.read("silver_crime")
        .filter(col("community_area").isNotNull())
        .groupBy("community_area", "district", "crime_year")
        .agg(
            count("*").alias("total_crimes"),
            spark_sum(when(col("arrest") == True, 1).otherwise(0)).alias("total_arrests"),
            avg("latitude").alias("avg_latitude"),
            avg("longitude").alias("avg_longitude")
        )
        .orderBy("crime_year", "total_crimes", ascending=[True, False])
    )

# COMMAND ----------

@dlt.table(
    name="gold_crime_by_time",
    comment="Crime statistics aggregated by time dimensions",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.reset.allowed": "false"  # ← 禁止重置
    }
)
def gold_crime_by_time():
    """
    Aggregate crime statistics by time dimensions (year, month, day of week, hour).
    """
    return (dlt.read("silver_crime")
        .groupBy("crime_year", "crime_month", "crime_day_of_week", "crime_hour")
        .agg(
            count("*").alias("total_crimes"),
            spark_sum(when(col("arrest") == True, 1).otherwise(0)).alias("total_arrests")
        )
        .orderBy("crime_year", "crime_month", "crime_day_of_week", "crime_hour")
    )

# COMMAND ----------

@dlt.table(
    name="gold_crime_summary",
    comment="Overall crime summary dashboard table",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.reset.allowed": "false"  # ← 禁止重置
    }
)
def gold_crime_summary():
    """
    High-level crime summary for dashboards.
    """
    return (dlt.read("silver_crime")
        .groupBy("crime_year")
        .agg(
            count("*").alias("total_crimes"),
            spark_sum(when(col("arrest") == True, 1).otherwise(0)).alias("total_arrests"),
            spark_sum(when(col("domestic") == True, 1).otherwise(0)).alias("domestic_crimes"),
            countDistinct("primary_type").alias("crime_types"),
            countDistinct("community_area").alias("affected_areas"),
            spark_round(
                spark_sum(when(col("arrest") == True, 1).otherwise(0)) / count("*") * 100, 2
            ).alias("arrest_rate_pct")
        )
        .orderBy("crime_year")
    )

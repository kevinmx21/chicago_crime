# Databricks notebook source
# MAGIC %md
# MAGIC # Chicago Crime Data Ingestion Pipeline
# MAGIC 
# MAGIC This notebook uses AutoLoader to incrementally ingest Chicago crime data from Azure Storage.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup - Get Parameters

# COMMAND ----------

# 获取从 Databricks Asset Bundle 传入的参数
dbutils.widgets.text("catalog", "dev_catalog")
dbutils.widgets.text("schema", "chicago_crime")
dbutils.widgets.text("container", "dev")
dbutils.widgets.text("storage_account", "kevintestdatabricks")
dbutils.widgets.text("env_name", "dev")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
container = dbutils.widgets.get("container")
storage_account = dbutils.widgets.get("storage_account")
env_name = dbutils.widgets.get("env_name")

print(f"""
========================================
Environment Configuration
========================================
Environment: {env_name}
Catalog: {catalog}
Schema: {schema}
Container: {container}
Storage Account: {storage_account}
========================================
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Define Paths

# COMMAND ----------

# 源数据路径
source_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/chicago_crime/raw/"

# Checkpoint 路径（用于增量处理）
checkpoint_base = f"abfss://{container}@{storage_account}.dfs.core.windows.net/_checkpoints/chicago_crime/"
schema_checkpoint = f"{checkpoint_base}schema/"
data_checkpoint = f"{checkpoint_base}data/"

# 目标表
target_table = f"{catalog}.{schema}.crime_data"

print(f"Source Path: {source_path}")
print(f"Target Table: {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Schema if Not Exists

# COMMAND ----------

# 创建 catalog（如果不存在）
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

# 创建 schema（如果不存在）
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

print(f"✓ Catalog and Schema ready: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Define Schema (Optional - for better performance)

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType, DateType
)

# Chicago Crime Data Schema
# 可以让 AutoLoader 自动推断，或者手动指定以提高性能
crime_schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Case Number", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Block", StringType(), True),
    StructField("IUCR", StringType(), True),
    StructField("Primary Type", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Location Description", StringType(), True),
    StructField("Arrest", BooleanType(), True),
    StructField("Domestic", BooleanType(), True),
    StructField("Beat", IntegerType(), True),
    StructField("District", IntegerType(), True),
    StructField("Ward", IntegerType(), True),
    StructField("Community Area", IntegerType(), True),
    StructField("FBI Code", StringType(), True),
    StructField("X Coordinate", DoubleType(), True),
    StructField("Y Coordinate", DoubleType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Updated On", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Location", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. AutoLoader - Read Stream

# COMMAND ----------

# 使用 AutoLoader 读取数据
df_raw = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_checkpoint)
    .option("cloudFiles.inferColumnTypes", "true")  # 自动推断列类型
    .option("cloudFiles.schemaHints", "ID INT, Year INT, Beat INT, District INT")  # Schema hints
    .option("header", "true")
    .option("multiLine", "true")  # 处理多行记录
    .option("escape", '"')
    .load(source_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Transformation

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp, lit, 
    regexp_replace, trim, when
)

# 数据清洗和转换
df_transformed = (df_raw
    # 重命名列（去掉空格）
    .withColumnRenamed("Case Number", "case_number")
    .withColumnRenamed("Primary Type", "primary_type")
    .withColumnRenamed("Location Description", "location_description")
    .withColumnRenamed("Community Area", "community_area")
    .withColumnRenamed("FBI Code", "fbi_code")
    .withColumnRenamed("X Coordinate", "x_coordinate")
    .withColumnRenamed("Y Coordinate", "y_coordinate")
    .withColumnRenamed("Updated On", "updated_on")
    .withColumnRenamed("ID", "id")
    .withColumnRenamed("Date", "crime_date")
    .withColumnRenamed("Block", "block")
    .withColumnRenamed("IUCR", "iucr")
    .withColumnRenamed("Description", "description")
    .withColumnRenamed("Arrest", "arrest")
    .withColumnRenamed("Domestic", "domestic")
    .withColumnRenamed("Beat", "beat")
    .withColumnRenamed("District", "district")
    .withColumnRenamed("Ward", "ward")
    .withColumnRenamed("Year", "year")
    .withColumnRenamed("Latitude", "latitude")
    .withColumnRenamed("Longitude", "longitude")
    .withColumnRenamed("Location", "location")
    
    # 添加元数据列
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_environment", lit(env_name))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write to Delta Table

# COMMAND ----------

# 写入 Delta Table
query = (df_transformed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", data_checkpoint)
    .option("mergeSchema", "true")  # 允许 schema 演化
    .trigger(availableNow=True)  # 处理所有可用数据后停止
    .toTable(target_table)
)

# 等待完成
query.awaitTermination()

print(f"✓ Data successfully written to {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verify Results

# COMMAND ----------

# 检查数据
row_count = spark.table(target_table).count()
print(f"Total rows in {target_table}: {row_count:,}")

# 显示最新数据
display(spark.table(target_table).orderBy(col("_ingestion_timestamp").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Data Quality Checks (Optional)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum as spark_sum, avg

# 基本统计
stats = spark.table(target_table).select(
    count("*").alias("total_records"),
    countDistinct("case_number").alias("unique_cases"),
    countDistinct("primary_type").alias("crime_types"),
    spark_sum(when(col("arrest") == True, 1).otherwise(0)).alias("total_arrests"),
    avg("latitude").alias("avg_latitude"),
    avg("longitude").alias("avg_longitude")
)

display(stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Cleanup (Optional)

# COMMAND ----------

# 清理 widgets
# dbutils.widgets.removeAll()

print("Pipeline completed successfully! ✓")

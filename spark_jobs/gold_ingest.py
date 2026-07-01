import os
import sys
import config
import pandas as pd
import boto3
import io
import importlib.util
import pymysql
from datetime import *
import config
import glob
import tempfile
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ.setdefault("KPLUS_RUNTIME", "host")

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "config")
sys.path.insert(0, CONFIG_DIR)


EXEC_DATE = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
spark = (
    SparkSession.builder
    .appName(f"bronze_ingest_{EXEC_DATE}")
    .config("spark.driver.memory", "8g")
    
    # Iceberg extension
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    
    # Catalog kplus → Hive
    .config("spark.sql.catalog.kplus",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.kplus.type", "hive")
    .config("spark.sql.catalog.kplus.uri", config.HIVE_METASTORE_URI)
    .config("spark.sql.catalog.kplus.warehouse", config.ICEBERG_WAREHOUSE)
    
    # S3A
    .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

def read_latest_silver():
    df = spark.table("kplus.silver.events")
    print(f"  → Rows: {df.count()}")
    return df


def most_watch(df):
    category_cols = [c for c in df.columns if c in 
                     ["Truyen_Hinh", "Phim_Truyen", "Giai_Tri", "Thieu_Nhi", "The_Thao"]]
    
    df = df.withColumn("MostWatch", greatest(*[col(c) for c in category_cols]))
    
    expr = when(col("MostWatch") == col(category_cols[0]), lit(category_cols[0]))
    for c in category_cols[1:]:
        expr = expr.when(col("MostWatch") == col(c), lit(c))
    
    df = df.withColumn("MostWatch", expr)
    return df

def customer_taste(df):
    category_cols = [c for c in df.columns if c in 
                     ["Truyen_Hinh", "Phim_Truyen", "Giai_Tri", "Thieu_Nhi", "The_Thao"]]
    
    df = df.withColumn("Taste", concat_ws("-", *[
        when(col(c) > 0, lit(c)) for c in category_cols
    ]))
    return df

def find_active(df):
    # Đếm số ngày active per Contract
    active_count = df.groupBy("Contract") \
                     .agg(countDistinct("execution_day").alias("active_days"))
    
    active_count = active_count.withColumn(
        "Active",
        when(col("active_days") > 4, "High").otherwise("Low")
    ).drop("active_days")
    
    return active_count

def save_to_gold(df):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS kplus.gold")
    
    df = df.withColumn("execution_day", lit(datetime.now().strftime("%Y-%m-%d")))
    
    try:
        # Lấy columns hiện tại trong table
        existing_cols = [f.name for f in spark.table("kplus.gold.app_usage").schema.fields]
        
        # Detect columns mới
        new_cols = [c for c in df.columns if c not in existing_cols]
        
        # ALTER TABLE thêm columns mới
        for c in new_cols:
            print(f"  → Adding new column: {c}")
            spark.sql(f"ALTER TABLE kplus.gold.app_usage ADD COLUMN {c} BIGINT")
        
        df.writeTo("kplus.gold.app_usage") \
          .option("merge-schema", "true") \
          .append()
        print("  ✅ Appended to kplus.gold.app_usage")
        
    except Exception:
        df.writeTo("kplus.gold.app_usage") \
          .using("iceberg") \
          .partitionedBy("execution_day") \
          .tableProperty("location", "s3a://lakehouse/gold/app_usage/") \
          .create()
        print("  ✅ Created kplus.gold.app_usage")
    
def main():
    print('------------- Reading silver --------------')
    df = read_latest_silver()
    
    print('------------- Most watch --------------')
    df = most_watch(df)
    
    print('------------- Customer taste --------------')
    df = customer_taste(df)
    
    print('------------- Find active --------------')
    active = find_active(df)
    
    print('------------- Join result --------------')
    result = df.join(active, "Contract", "inner")
    result.show(5)

    print('------------- Saving to Gold --------------')
    save_to_gold(result)

if __name__ == "__main__":
    main()
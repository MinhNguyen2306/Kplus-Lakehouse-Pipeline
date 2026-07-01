import os
import sys
import config
import pandas as pd
import boto3
import io
import importlib.util
import pymysql
from datetime import *
import glob
import config
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

def read_latest_bronze():
    df = spark.table("kplus.bronze.events")
    print(f"  → Rows: {df.count()}")
    return df

def calculate_devices(df):
    total_devices = df.select("Contract","Mac").groupBy("Contract").count()
    total_devices = total_devices.withColumnRenamed('count','TotalDevices')
    return total_devices

def transform_category(df):
	df = df.withColumn("Type",
		   when((col("AppName") == 'CHANNEL') | (col("AppName") =='DSHD')| (col("AppName") =='KPLUS')| (col("AppName") =='KPlus'), "Truyen_Hinh")
		  .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS_RES')| (col("AppName") =='BHD_RES')| 
				 (col("AppName") =='VOD_RES')| (col("AppName") =='FIMS')| (col("AppName") =='BHD')| (col("AppName") =='DANET'), "Phim_Truyen")
		  .when((col("AppName") == 'RELAX'), "Giai_Tri")
		  .when((col("AppName") == 'CHILD'), "Thieu_Nhi")
		  .when((col("AppName") == 'SPORT'), "The_Thao")
		  .otherwise("Error"))
	return df 

def calculate_statistics(df): 
	statistics = df.select('Contract','TotalDuration','Type').groupBy('Contract','Type').sum()
	statistics = statistics.withColumnRenamed('sum(TotalDuration)','TotalDuration')
	statistics = statistics.groupBy('Contract').pivot('Type').sum('TotalDuration').na.fill(0)
	return statistics 
	
def finalize_result(statistics, total_devices, df_bronze):
    result = statistics.join(total_devices, 'Contract', 'inner')
    source = df_bronze.select("Contract", "source_file").distinct()
    result = result.join(source, 'Contract', 'left')
    return result

def save_to_silver(df):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS kplus.silver")
    df = df.withColumn("execution_day", lit(datetime.now().strftime("%Y-%m-%d")))
    
    try:
        existing_cols = [f.name for f in spark.table("kplus.silver.events").schema.fields]
        new_cols = [c for c in df.columns if c not in existing_cols]
        for c in new_cols:
            print(f"  → Adding new column: {c}")
            spark.sql(f"ALTER TABLE kplus.silver.events ADD COLUMN {c} BIGINT")
        df.writeTo("kplus.silver.events") \
          .option("merge-schema", "true") \
          .append()
        print("  ✅ Appended to kplus.silver.events")
    except Exception:
        df.writeTo("kplus.silver.events") \
          .using("iceberg") \
          .partitionedBy("execution_day") \
          .create()
        print("  ✅ Created kplus.silver.events")


def main():
    print('------------- Reading bronze from MinIO --------------')
    df = read_latest_bronze()
    
    print('------------- Transforming category --------------')
    trans = transform_category(df)
    
    print('------------- Calculating devices --------------')
    total_devices = calculate_devices(df)
    
    print('------------- Calculating statistics --------------')
    statistics = calculate_statistics(trans)
    
    print('------------- Finalizing result --------------')
    result = finalize_result(statistics, total_devices, df)
    result.show(5)
    
    print('------------- Saving to Silver --------------')
    save_to_silver(result)

    return print('Task finished')         	
	
if __name__ == "__main__":
    main()

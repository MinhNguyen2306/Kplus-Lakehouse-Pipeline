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
import tempfile
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ.setdefault("KPLUS_RUNTIME", "host")

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "config")
sys.path.insert(0, CONFIG_DIR)
import config

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

def export_to_mysql(df):
    latest_day = df.agg({"execution_day": "max"}).collect()[0][0]
    print(f"  → Exporting execution_day: {latest_day}")

    # 1. Lấy thứ tự columns từ MySQL
    conn = pymysql.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DB,
        auth_plugin_map={"caching_sha2_password": "mysql_native_password"}
    )
    cursor = conn.cursor()
    cursor.execute("SHOW COLUMNS FROM summary_behavior_data")
    mysql_cols = [row[0] for row in cursor.fetchall()]
    conn.close()

    # 2. Filter + thêm columns thiếu với giá trị 0
    df_latest = df.filter(col("execution_day") == latest_day).fillna(0)

    # Thêm columns có trong MySQL nhưng chưa có trong df
    for c in mysql_cols:
        if c not in df_latest.columns:
            df_latest = df_latest.withColumn(c, lit(0))

    # Reorder theo MySQL
    df_latest = df_latest.select(mysql_cols)
    print(f"  → Rows: {df_latest.count()}")

    # 3. Write vào temp table
    df_latest.write \
      .format("jdbc") \
      .option("url", config.MYSQL_JDBC_URL) \
      .option("driver", config.MYSQL_JDBC_DRIVER) \
      .option("dbtable", "summary_behavior_data_tmp") \
      .option("user", config.MYSQL_USER) \
      .option("password", config.MYSQL_PASSWORD) \
      .option("batchsize", "50000") \
      .option("numPartitions", "4") \
      .mode("overwrite") \
      .save()

    # 4. UPSERT
    conn = pymysql.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DB,
        auth_plugin_map={"caching_sha2_password": "mysql_native_password"}
    )
    cursor = conn.cursor()
    primary_keys = ["Contract", "execution_day", "source_file"]
    update_cols = [c for c in mysql_cols if c not in primary_keys]
    update_clause = ", ".join([f"{c}=VALUES({c})" for c in update_cols])

    cursor.execute(f"""
        INSERT INTO summary_behavior_data
        SELECT * FROM summary_behavior_data_tmp
        ON DUPLICATE KEY UPDATE {update_clause}
    """)
    conn.commit()
    cursor.execute("DROP TABLE IF EXISTS summary_behavior_data_tmp")
    conn.commit()
    conn.close()
    print("  ✅ Upserted to MySQL")

def main():
    print('------------- Reading Gold --------------')
    df = spark.table("kplus.gold.app_usage")
    print(f"  → Rows: {df.count()}")
    df.show(5)
    
    print('------------- Export to MySQL --------------')
    print('  → Using UPSERT via pymysql')  # ← thêm dòng này
    export_to_mysql(df)
    
    return print('✅ Task finished')

if __name__ == "__main__":
    main()
import os
import sys
import pandas as pd
import boto3
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
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.local.dir", "C:/tmp/spark")

    # Iceberg
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.kplus",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.kplus.type", "hive")
    .config("spark.sql.catalog.kplus.uri", config.HIVE_METASTORE_URI)
    .config("spark.sql.catalog.kplus.warehouse", f"s3a://{config.BUCKET_WAREHOUSE}/")

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


def get_checkpoint(file_name: str) -> dict:
    conn = pymysql.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DB,
        auth_plugin_map={"caching_sha2_password": "mysql_native_password"}
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT last_offset, rows_per_run
        FROM cdc_checkpoint
        WHERE file_name = %s
        ORDER BY updated_at DESC
        LIMIT 1
    """, (file_name,))
    row = cursor.fetchone()
    conn.close()
    if row is None:
        return {"last_offset": 0, "rows_per_run": 0}
    return {"last_offset": row[0], "rows_per_run": row[1]}


def read_n_rows(file_key: str, offset: int, n: int):
    s3 = boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
    )
    response = s3.get_object(Bucket=config.BUCKET_RAW, Key=file_key)
    pdf = pd.read_json(response["Body"], lines=True)
    pdf_slice = pdf.iloc[offset : offset + n]
    pdf_source = pd.json_normalize(pdf_slice["_source"])
    pdf_source["execution_date"] = str(date.today())
    df = spark.createDataFrame(pdf_source)
    return df


def save_checkpoint(file_name: str, new_offset: int, n: int):
    conn = pymysql.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DB,
        auth_plugin_map={"caching_sha2_password": "mysql_native_password"}
    )
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO cdc_checkpoint (file_name, last_offset, rows_per_run, execution_date)
        VALUES (%s, %s, %s, %s)
    """, (file_name, new_offset, n, date.today()))
    conn.commit()
    conn.close()
    print(f"  ✅ Checkpoint saved: offset={new_offset}")


def save_to_bronze(df, execution_date: str, source_file: str):
    df = df.withColumn("source_file", lit(source_file))
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS kplus.bronze")
    
    # Tạo table với LOCATION rõ ràng trỏ về lakehouse
    spark.sql("""
        CREATE TABLE IF NOT EXISTS kplus.bronze.events (
            Contract        STRING,
            Mac             STRING,
            TotalDuration   BIGINT,
            AppName         STRING,
            execution_date  STRING,
            source_file     STRING
        )
        USING iceberg
        PARTITIONED BY (execution_date)
        LOCATION 's3a://lakehouse/bronze/events/'
    """)
    
    # Detect và thêm columns mới nếu có
    existing_cols = [f.name for f in spark.table("kplus.bronze.events").schema.fields]
    new_cols = [c for c in df.columns if c not in existing_cols]
    for c in new_cols:
        print(f"  → Adding new column: {c}")
        spark.sql(f"ALTER TABLE kplus.bronze.events ADD COLUMN {c} BIGINT")
    
    df.writeTo("kplus.bronze.events") \
      .option("merge-schema", "true") \
      .append()
    print("  ✅ Saved to kplus.bronze.events")


def main():
    print('------------- Enter date --------------')
    date_input = input("Nhập ngày (YYYYMMDD): ")
    file_key  = f"kplus/{date_input}.json"
    file_name = f"{date_input}.json"
    print(f"  → File: {file_key}")

    print('------------- Entering the number --------------')
    idx = int(input("Enter the # rows: "))

    print('------------- Getting checkpoint --------------')
    checkpoint = get_checkpoint(file_name)
    offset = checkpoint["last_offset"]
    print(f"  → offset: {offset}, reading {idx} rows")

    print('------------- Reading from MinIO --------------')
    df = read_n_rows(file_key, offset, idx)
    df.show(5)

    print('------------- Saving checkpoint --------------')
    save_checkpoint(file_name, offset + idx, idx)

    print('------------- Saving to Bronze --------------')
    save_to_bronze(df, EXEC_DATE, date_input)

    return print('✅ Task finished')


if __name__ == "__main__":
    main()
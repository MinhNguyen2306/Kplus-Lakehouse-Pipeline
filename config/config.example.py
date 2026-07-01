"""
config.py — Central connection config for the KPLUS Lakehouse project.

Mounted into:
  - Spark containers at /opt/config
  - Airflow containers at /opt/airflow/config (on PYTHONPATH)

Host vs container networking
----------------------------
Inside the docker-compose network, services talk to each other by their
service name (e.g. "minio:9000"). From your laptop (host) you reach the same
services via "localhost:<published_port>".

Set RUNTIME=host when running a script from your machine (e.g. the upload
helper). Leave it unset / "container" for anything running inside compose.
"""

import os

# "container" (default) -> use docker service names
# "host"                -> use localhost + published ports
RUNTIME = os.getenv("KPLUS_RUNTIME", "container").lower()
_HOST = RUNTIME == "host"


def _h(service_name: str, localhost_alias: str = "localhost") -> str:
    """Return the right hostname depending on where the code runs."""
    return localhost_alias if _HOST else service_name


# ---------------------------------------------------------------------------
# MinIO / S3
# ---------------------------------------------------------------------------
MINIO_ENDPOINT_HOST = _h("minio")
MINIO_PORT = 9000
# boto3 / mc style endpoint (with scheme)
MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT_HOST}:{MINIO_PORT}"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_REGION = "us-east-1"  # MinIO ignores it but S3A wants a value

# Buckets (created by minio-init service)
BUCKET_RAW = "raw"              # landing zone for source files
BUCKET_LAKEHOUSE = "lakehouse"  # bronze/silver/gold iceberg data
BUCKET_WAREHOUSE = "warehouse"  # hive/iceberg warehouse root

# Source file
SOURCE_FILE = "20220401.json"
RAW_OBJECT_KEY = f"kplus/{SOURCE_FILE}"
RAW_S3_URI = f"s3a://{BUCKET_RAW}/{RAW_OBJECT_KEY}"

BRONZE_FILE = ""

# S3A config map — spread into SparkSession builder in the spark jobs.
# Note S3A uses the host WITHOUT scheme for fs.s3a.endpoint.
S3A_CONF = {
    "fs.s3a.endpoint": f"{MINIO_ENDPOINT_HOST}:{MINIO_PORT}",
    "fs.s3a.access.key": MINIO_ACCESS_KEY,
    "fs.s3a.secret.key": MINIO_SECRET_KEY,
    "fs.s3a.path.style.access": "true",
    "fs.s3a.connection.ssl.enabled": "false",
    "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "fs.s3a.aws.credentials.provider":
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}

# ---------------------------------------------------------------------------
# Hive Metastore (Iceberg catalog backend)
# ---------------------------------------------------------------------------
HIVE_METASTORE_HOST = _h("hive-metastore")
HIVE_METASTORE_PORT = 9083
HIVE_METASTORE_URI = f"thrift://{HIVE_METASTORE_HOST}:{HIVE_METASTORE_PORT}"

# ---------------------------------------------------------------------------
# Iceberg catalog
# ---------------------------------------------------------------------------
ICEBERG_CATALOG = "kplus"
ICEBERG_WAREHOUSE = f"s3a://{BUCKET_WAREHOUSE}/"

# Namespaces (databases) per medallion layer
DB_BRONZE = "bronze"
DB_SILVER = "silver"
DB_GOLD = "gold"

# Fully-qualified table names
TBL_BRONZE = f"{ICEBERG_CATALOG}.{DB_BRONZE}.events"
TBL_SILVER = f"{ICEBERG_CATALOG}.{DB_SILVER}.events"
TBL_GOLD_APP = f"{ICEBERG_CATALOG}.{DB_GOLD}.app_usage"

# Iceberg + Spark catalog config — spread into SparkSession builder.
ICEBERG_CONF = {
    "spark.sql.extensions":
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    f"spark.sql.catalog.{ICEBERG_CATALOG}":
        "org.apache.iceberg.spark.SparkCatalog",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.type": "hive",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.uri": HIVE_METASTORE_URI,
    f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse": ICEBERG_WAREHOUSE,
    f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl":
        "org.apache.iceberg.aws.s3.S3FileIO",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.endpoint": MINIO_ENDPOINT,
    f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.path-style-access": "true",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.access-key-id": MINIO_ACCESS_KEY,
    f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.secret-access-key": MINIO_SECRET_KEY,
}

# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------
SPARK_MASTER_HOST = _h("spark-master")
SPARK_MASTER_URL = f"spark://{SPARK_MASTER_HOST}:7077"

# Maven coordinates pulled via --packages (match Spark 3.5 / Scala 2.12)
SPARK_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.iceberg:iceberg-aws-bundle:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])

# ---------------------------------------------------------------------------
# MySQL (serving layer)
# ---------------------------------------------------------------------------
MYSQL_HOST = _h("mysql")
MYSQL_PORT = 3306
MYSQL_DB = "serving"
MYSQL_USER = os.getenv("MYSQL_USER", "kplus")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "kplus")

# JDBC URL for Spark write (serving_export.py)
MYSQL_JDBC_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
)
MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
MYSQL_JDBC_PACKAGE = "com.mysql:mysql-connector-j:8.4.0"

# SQLAlchemy URL (Airflow / python clients)
MYSQL_SQLALCHEMY_URL = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@"
    f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
)

# ---------------------------------------------------------------------------
# Postgres (Hive metastore backend) — usually not touched directly
# ---------------------------------------------------------------------------
PG_HIVE_HOST = _h("postgres-hive")
PG_HIVE_PORT = 5432
PG_HIVE_DB = "metastore"
PG_HIVE_USER = "hive"
PG_HIVE_PASSWORD = "hive"

# ---------------------------------------------------------------------------
# Grafana
# ---------------------------------------------------------------------------
GRAFANA_HOST = _h("grafana")
GRAFANA_PORT = 3000
GRAFANA_URL = f"http://{GRAFANA_HOST}:{GRAFANA_PORT}"

# ---------------------------------------------------------------------------
# Demo / pipeline params
# ---------------------------------------------------------------------------
DEMO_ROW_LIMIT = 1000  # only load 1000 rows for the demo


if __name__ == "__main__":
    # Quick sanity dump
    print(f"RUNTIME            = {RUNTIME}")
    print(f"MINIO_ENDPOINT     = {MINIO_ENDPOINT}")
    print(f"RAW_S3_URI         = {RAW_S3_URI}")
    print(f"HIVE_METASTORE_URI = {HIVE_METASTORE_URI}")
    print(f"ICEBERG_WAREHOUSE  = {ICEBERG_WAREHOUSE}")
    print(f"SPARK_MASTER_URL   = {SPARK_MASTER_URL}")
    print(f"MYSQL_JDBC_URL     = {MYSQL_JDBC_URL}")
    print(f"MYSQL_SQLALCHEMY   = {MYSQL_SQLALCHEMY_URL}")

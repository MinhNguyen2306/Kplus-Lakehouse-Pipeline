<div align="center">

# KPLUS Lakehouse — Medallion Data Pipeline

An end-to-end **lakehouse pipeline** that turns ~1.6M raw KPLUS viewing-log records into clean, aggregated tables ready for dashboarding — built solo, from infrastructure to visualization.

Data flows through a **medallion architecture** (Bronze → Silver → Gold) using Apache Iceberg tables on a Hive Metastore catalog.

<br>

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1E90FF?style=for-the-badge&logo=apache&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-C72E49?style=for-the-badge&logo=minio&logoColor=white)
![Apache Hive](https://img.shields.io/badge/Apache%20Hive-FDEE21?style=for-the-badge&logo=apachehive&logoColor=black)
![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

</div>

---

---

## Stack

| Layer | Technology |
|---|---|
| Object storage | MinIO (S3-compatible) |
| Table format | Apache Iceberg |
| Catalog | Hive Metastore (PostgreSQL backend) |
| Processing | PySpark |
| Serving | MySQL |
| Dashboard | Grafana |
| Containerization | Docker Compose |

---

## Architecture
<img width="1326" height="663" alt="image" src="https://github.com/user-attachments/assets/3fe6875a-db08-4db4-a28e-61a079f1c7bf" />

Key point: **Hive Metastore is not in the data path** — it is the *catalog* that tracks where each Iceberg table lives and its schema. The table data itself sits on MinIO.

---

## Medallion layers

**Bronze — raw capture (`spark_jobs/bronze_ingest.py`)**
Reads N rows from a source JSON file on MinIO (incrementally, via CDC checkpoint), extracts the `_source.*` fields, adds `execution_date` / `source_file`, and appends to the Iceberg table `kplus.bronze.events`. Supports schema evolution (auto `ALTER TABLE ADD COLUMN`).

**Silver — clean & standardize (`spark_jobs/silver_ingest.py`)**
Reads bronze, then maps `AppName` to 5 content types (Truyền Hình, Phim Truyện, Giải Trí, Thiếu Nhi, Thể Thao), counts devices per contract, does groupBy contract + pivot type to sum `TotalDuration`, and joins statistics with device counts. Writes to `kplus.silver.events`.

**Gold — enrich & aggregate (`spark_jobs/gold_ingest.py`)**
Reads silver and computes per-contract behavior features: `MostWatch` (most-watched content type), `Taste` (combination of content types consumed), and `Active` (High/Low segment based on distinct active days). Writes to `kplus.gold.app_usage`.

**Serving export (`spark_jobs/export_data.py`)**
Exports the latest gold snapshot into the MySQL table `summary_behavior_data` via a JDBC temp-table + `INSERT … ON DUPLICATE KEY UPDATE` upsert, so re-runs are idempotent.

---

## Incremental loading (CDC checkpoint)

Bronze processes the source file in batches rather than all at once. The `cdc_checkpoint` table in MySQL stores the last processed **offset** per file. Each run reads the checkpoint, ingests the next `rows_per_run` rows, then advances the offset — making ingestion resumable and avoiding reprocessing.

> **Note:** the checkpoint and the ingested data must stay in sync. Truncating the checkpoint without clearing the corresponding data can cause duplicate rows in bronze.

---

## Data quality note

The source contains a null-bucket row with `Contract = '0'` holding anomalously large values (e.g. ~44.7M duration, 1011 devices). This is filtered out in the serving views before aggregation, otherwise it dominates every chart.

---

## Project structure

```
Kplus-Lakehouse-Pipeline/
├── config/
│   ├── config.py            # connection strings (host/container aware via KPLUS_RUNTIME)
│   └── jars_config.py       # Spark JAR / package configuration
├── hive-conf/
│   └── metastore-site.xml   # Hive Metastore + S3A→MinIO config
├── spark_jobs/
│   ├── bronze_ingest.py     # raw JSON → Iceberg bronze (CDC incremental)
│   ├── silver_ingest.py     # clean + standardize → Iceberg silver
│   ├── gold_ingest.py       # enrich + aggregate → Iceberg gold
│   └── export_data.py       # gold → MySQL serving (upsert)
├── docker-compose.yml       # MinIO, Hive, Postgres, Spark, MySQL, Grafana
├── run_pipeline.py          # runs the stages sequentially
├── upload_to_minio.py       # upload source file to MinIO raw bucket
├── requirements.txt
├── README.md
└── LICENSE
```

---

## Quick start

**1. Download required JARs into `jars/`**
```bash
bash download_jars.sh
# postgresql-42.7.3.jar, hadoop-aws-3.3.4.jar, aws-java-sdk-bundle-1.12.262.jar
```

**2. Start the stack**
```bash
docker compose up -d
docker compose ps          # wait until services are healthy
```

**3. Upload a source file to MinIO**
```bash
pip install -r requirements.txt
KPLUS_RUNTIME=host python upload_to_minio.py path/to/20220401.json
```

**4. Run the pipeline (sequential)**
```bash
python spark_jobs/bronze_ingest.py    # reads N rows, writes bronze
python spark_jobs/silver_ingest.py    # cleans + standardizes → silver
python spark_jobs/gold_ingest.py      # enriches + aggregates → gold
python spark_jobs/export_data.py      # exports gold → MySQL
```

**5. Create serving views + open Grafana**
```sql
-- run the CREATE VIEW statements (v_kpi, v_content, v_segment, v_top …) in MySQL
```
Grafana: http://localhost:3000 (admin/admin) → add MySQL data source (`mysql:3306`, db `serving`).

---

## Service endpoints

| Service | URL | Login |
|---|---|---|
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Spark master UI | http://localhost:8080 | — |
| MySQL | localhost:3306 | kplus / kplus (db: serving) |
| Grafana | http://localhost:3000 | admin / admin |
| Hive Metastore | thrift://localhost:9083 | — |

---

## Design decisions & trade-offs

This is a learning/portfolio build; several choices favor simplicity for a single-node demo,
with a clear upgrade path to production:

| Decision | Rationale | Production upgrade |
|---|---|---|
| MinIO instead of AWS S3 | Free, local, same S3 API | S3 / MinIO cluster |
| boto3 + pandas to read source | Avoids S3A JAR version issues | Spark reads `s3a://` directly (distributed) |
| PySpark local mode | Fast startup, easy debug | Spark cluster + submit |
| MySQL serving | Fast dashboard queries | (keep — correct pattern) |
| Sequential script runs | Simple, proves pipeline correctness | Airflow DAG orchestration |

---

## Roadmap / future work

- **Orchestration** — wrap the stages in an Airflow DAG (`bronze → silver → gold → export`).
- **Distributed reads** — switch source ingestion from boto3/pandas to native Spark `s3a://`.
- **Dimensional modeling** — evolve the gold layer into a star schema (fact + dimensions).
- **Cloud & IaC** — migrate to S3 + Redshift + Glue, provisioned with Terraform, transformed with dbt.

---

## Notes

Built end-to-end by a single developer to understand the full data lifecycle —
from container infrastructure through catalog, table format, incremental processing,
and dashboarding. The stack is intentionally lean to master each component rather than
wire together tools superficially.

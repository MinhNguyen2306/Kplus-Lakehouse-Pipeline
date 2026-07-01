# KPLUS Lakehouse — Medallion Data Pipeline

An end-to-end **lakehouse pipeline** that turns ~1.6M raw KPLUS viewing-log records into
clean, aggregated tables ready for dashboarding — built solo, from infrastructure to visualization.

Data flows through a **medallion architecture** (Bronze → Silver → Gold) using
**Apache Iceberg** tables on a **Hive Metastore** catalog, with **MinIO** as S3-compatible
storage, **PySpark** for processing, **MySQL** as the serving layer, and **Grafana** for dashboards —
all orchestrated with **Docker Compose**.

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

```

Key point: **Hive Metastore is not in the data path** — it is the *catalog* that tracks
where each Iceberg table lives and its schema. Table data itself sits on MinIO.

---

## Medallion layers

**Bronze — raw capture (`bronze_ingest.py`)**
Reads N rows from a source JSON file on MinIO (incrementally, via CDC checkpoint),
extracts the `_source.*` fields, adds `execution_date` / `source_file`, and appends to
the Iceberg table `kplus.bronze.events`. Supports schema evolution (auto `ALTER TABLE ADD COLUMN`).

**Silver — clean & standardize (`silver_ingest.py`)**
Reads bronze, then:
- maps `AppName` → 5 content types (Truyền Hình, Phim Truyện, Giải Trí, Thiếu Nhi, Thể Thao)
- counts devices per contract
- groupBy contract + pivot type → sum of `TotalDuration`
- joins statistics with device counts

Writes to `kplus.silver.events`.

**Gold — enrich & aggregate (`gold_ingest.py`)**
Reads silver, computes per-contract behavior features:
- `MostWatch` — the most-watched content type
- `Taste` — combination of content types consumed
- `Active` — activity segment (High/Low) based on distinct active days

Writes to `kplus.gold.app_usage`.

**Serving export (`export_data.py`)**
Exports the latest gold snapshot into MySQL table `summary_behavior_data` via a
JDBC temp-table + `INSERT … ON DUPLICATE KEY UPDATE` upsert, so re-runs are idempotent.

---

## Incremental loading (CDC checkpoint)

Bronze processes the source file in batches rather than all at once. The
`cdc_checkpoint` table in MySQL stores the last processed **offset** per file:

| file_name | last_offset | rows_per_run | execution_date |
|---|---|---|---|
| 20220401.json | 70 | 10 | 2026-06-25 |

Each run reads the checkpoint, ingests the next `rows_per_run` rows, then advances the offset.
This makes ingestion resumable and avoids reprocessing.

> **Note:** the checkpoint and the ingested data must stay in sync. Truncating the checkpoint
> without clearing the corresponding data can cause duplicate rows in bronze.

---

## Data quality note

The source contains a null-bucket row with `Contract = '0'` holding anomalously large values
(e.g. ~44.7M duration, 1011 devices). This is filtered out in the serving views before
aggregation, otherwise it dominates every chart.

---

## Project structure

```
kplus_lakehouse/
├── docker-compose.yml          # 9 services: MinIO, Hive, 2×Postgres, Spark, MySQL, Grafana …
├── config/
│   └── config.py               # connection strings (host/container aware via KPLUS_RUNTIME)
├── hive-conf/
│   └── metastore-site.xml       # Hive Metastore + S3A→MinIO config
├── jars/                       # hadoop-aws, aws-java-sdk-bundle, postgresql, iceberg runtime
├── spark_jobs/
│   ├── bronze_ingest.py
│   ├── silver_ingest.py
│   ├── gold_ingest.py
│   └── export_data.py
└── README.md
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

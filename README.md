# GCS → BigQuery Pipeline — Developer Reference

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Repository Structure](#2-repository-structure)
3. [Source Data](#3-source-data)
4. [BigQuery Tables & Views](#4-bigquery-tables--views)
5. [Pipeline Components](#5-pipeline-components)
6. [Deduplication Strategy](#6-deduplication-strategy)
7. [Environment Configuration](#7-environment-configuration)
8. [Deployment](#8-deployment)
9. [Adding a New Source Folder](#9-adding-a-new-source-folder)
10. [Operational Notes](#10-operational-notes)

---

## 1. Architecture Overview

```
GCS Bucket  (demo_data_output_dev / prod)
  ├── Order_Events/*.json.gz
  └── Notification_Events/*.json.gz
        │
        │ OBJECT_FINALIZE event
        ▼
    Pub/Sub Topic: demo-ingest-topic
        │
        │ pull subscription: demo-ingest-topic-sub
        ▼
    Dataflow Streaming Job  (dataflow/pipeline.py)
        │  routes by folder name → different BQ stage tables
        ├──────────────────────────────────────────────────────┐
        ▼                                                      ▼
STG_Order_Events                                STG_Notification_Events
(explicit columns, 7-day expiry)                (payload JSON, 7-day expiry)
        │                                                      │
        │ BQ Scheduled Query — every hour                      │
        │ (03_merge_stage_to_gold.sql)                         │
        ▼                                                      ▼
Order_Events                                    Notification_Events
(gold, explicit columns, record_id PK)          (gold, payload JSON)
        │                                                      │
        ▼                                                      ▼
Order_Events_vw                        Notification_Events_unified_vw
(deduped UNION stage + gold)           (deduped UNION stage + gold)

Airflow DAG — daily 12 PM UTC  (airflow/dag_daily_reprocessing.py)
  └─ Cloud Run Job: demo-bq-reprocessing-job
       1. Reprocesses last 2 days of GCS files → stage tables
       2. MERGE stage → gold  (catches anything missed by streaming)
```

Two parallel pipelines share the same GCS bucket and Pub/Sub topic but write to separate stage/gold table pairs because the two source folders have different schemas.

---

## 2. Repository Structure

```
gcs_bq_pipeline/
├── README.md                        ← this file
├── SETUP_GUIDE.md                   ← step-by-step GCP Web UI setup
├── pyrightconfig.json               ← suppresses IDE import warnings (not installed locally)
├── sql/
│   ├── 01_create_tables.sql         ← DDL for all stage + gold tables
│   ├── 02_create_view.sql           ← unified views (stage + gold union)
│   └── 03_merge_stage_to_gold.sql   ← hourly MERGE scheduled queries
├── dataflow/
│   ├── pipeline.py                  ← Apache Beam streaming job
│   ├── Dockerfile                   ← custom SDK container (Python 3.12 + Beam 2.69.0)
│   ├── requirements.txt             ← Beam dependencies
│   └── gcp_deployment.txt           ← step-by-step Cloud Build + Dataflow deploy guide
└── airflow/
    ├── cloud_run_job/
    │   ├── Dockerfile               ← Cloud Run Job container (Python 3.12-slim)
    │   ├── main.py                  ← entrypoint: load stage + MERGE to gold
    │   ├── requirements.txt         ← GCP client libraries (no Beam)
    │   └── deployment_script.txt    ← Cloud Build + Cloud Run deploy commands
    ├── python_scripts/
    │   ├── __init__.py
    │   └── gcs_bq_pipeline/
    │       ├── __init__.py
    │       ├── config.py            ← env vars, folder/table mapping, column lists
    │       ├── gcs_loader.py        ← GCS → stage rows logic
    │       ├── bq_storage_write.py  ← Storage Write API helper
    │       ├── dlq.py               ← Pub/Sub DLQ publisher
    │       └── merge_sql.py         ← MERGE SQL builder
    └── dag_daily_reprocessing.py    ← Cloud Composer DAG (pure orchestration)
```

---

## 3. Source Data

| Property | Value |
|---|---|
| GCS bucket (dev) | `demo_data_output_dev` |
| GCS bucket (prod) | `demo_data_output_prod` |
| File format | `.json.gz` (gzip-compressed JSON) |
| JSON structures supported | JSON array `[{...}, {...}]` and newline-delimited JSON `{...}\n{...}` |
| GCS auto-delete | 90 days (lifecycle rule on bucket) |

### Source folders

| Folder | BQ schema style | Stage table | Gold table |
|---|---|---|---|
| `Order_Events/` | Explicit columns | `STG_Order_Events` | `Order_Events` |
| `Notification_Events/` | `payload JSON` | `STG_Notification_Events` | `Notification_Events` |

Each `Order_Events` JSON record is expected to carry a `record_id` field — the primary key used to deduplicate and upsert into the gold table. Records missing `record_id` are still ingested into stage, but cannot be promoted to gold (the gold `record_id` column is `NOT NULL`).

---

## 4. BigQuery Tables & Views

**Project:** `demo-datalake-12345`  **Dataset:** `Demo_Output_GCS`

### 4.1 Order_Events — Stage & Gold

Both stage and gold share the same business columns. Gold adds a `NOT NULL` primary key constraint on `record_id`; stage has a 7-day partition expiry, gold does not.

| Column | Type | Notes |
|---|---|---|
| `record_id` | STRING | **Primary key** (gold: `NOT NULL`, `PRIMARY KEY … NOT ENFORCED`) |
| `order_date` | DATE | From source JSON |
| `ship_date` | DATE | From source JSON |
| `order_ref` | STRING | |
| `source_code` | STRING | |
| `category` | STRING | |
| `status` | STRING | |
| `channel` | STRING | |
| `event_timestamp` | TIMESTAMP | |
| `customer_name` | STRING | |
| `region_code` | STRING | |
| `contact_email` | STRING | |
| `source_file` | STRING | `gs://bucket/folder/file.json.gz` |
| `ingestion_timestamp` | TIMESTAMP | UTC time the row was written |

- **Partitioned by:** `DATE(ingestion_timestamp)`
- **Clustered by:** `record_id` — speeds up the MERGE join on the primary key
- **Stage expiry:** 7 days — rows are automatically deleted after the MERGE has promoted them to gold
- **Primary key:** `record_id` (declared `NOT ENFORCED`; uniqueness is guaranteed by the MERGE, not by BigQuery)

### 4.2 Notification_Events — Stage & Gold

| Column | Type | Notes |
|---|---|---|
| `payload` | JSON NOT NULL | Full raw record |
| `source_file` | STRING | `gs://bucket/folder/file.json.gz` |
| `ingestion_timestamp` | TIMESTAMP | UTC time the row was written |
| `_partition_date` | DATE | Stage only; partition column |
| `_last_updated` | TIMESTAMP | Gold only; set by MERGE |

- **Stage partitioned by:** `_partition_date`, 7-day expiry
- **Gold:** no partitioning, no expiry (permanent)
- **Uniqueness:** `TO_JSON_STRING(payload)` — the entire JSON object is the identity

### 4.3 Views

| View | Purpose | Dedup key |
|---|---|---|
| `Order_Events_vw` | Deduped UNION of stage (last 2 days) + gold | `record_id` (latest `ingestion_timestamp` wins) |
| `Notification_Events_unified_vw` | CTE dedup union of stage (last 1 day) + gold | `TO_JSON_STRING(payload)` |

Use these views when you need the freshest data before the hourly MERGE has run.
Query explicit fields from `Order_Events_vw` directly. For `Notification_Events_unified_vw`, access nested fields with:

```sql
SELECT JSON_VALUE(payload, '$.your_field') FROM `demo-datalake-12345.Demo_Output_GCS.Notification_Events_unified_vw`;
```

---

## 5. Pipeline Components

### 5.1 Dataflow Streaming Job (`dataflow/pipeline.py`)

**Trigger:** GCS `OBJECT_FINALIZE` → Pub/Sub → Dataflow (always running)

**Flow:**
1. Read Pub/Sub messages with attributes (`with_attributes=True`)
2. Filter for `eventType == OBJECT_FINALIZE` and `.json.gz` suffix
3. Download and `gzip.decompress` the file from GCS
4. Parse JSON (array or NDJSON)
5. Route by folder name using `beam.pvalue.TaggedOutput`:
   - `Order_Events` → map to explicit column dict → `STG_Order_Events`
   - `Notification_Events` → wrap as `payload JSON` dict → `STG_Notification_Events`
6. Write to BigQuery via `WriteToBigQuery` (Storage Write API, at-least-once); BQ insert failures routed to DLQ via `FailedRowsWithErrors` side output

**CLI args:**

```bash
python pipeline.py \
  --runner=DataflowRunner \
  --project=demo-orchestration \
  --region=us-west1 \
  --temp_location=gs://demo_dataflow_staging/dataflow/tmp \
  --staging_location=gs://demo_dataflow_staging/dataflow/staging \
  --sdk_container_image=us-west1-docker.pkg.dev/demo-orchestration/demo-pipeline-containers/demo-bq-pipeline:2.69.0 \
  --sdk_location=container \
  --subnetwork=regions/us-west1/subnetworks/demo-orchestration-subnet-us-west1 \
  --no_use_public_ips \
  --input_subscription=projects/demo-ingest/subscriptions/demo-ingest-topic-sub \
  --order_events_table=demo-datalake-12345:Demo_Output_GCS.STG_Order_Events \
  --dlq_topic=projects/demo-ingest/topics/demo_output_gcs_DEADLETTER \
  --streaming \
  --enable_streaming_engine
# --notification_events_table=...   ← NOTIFICATION_EVENTS: not yet active
```

**Key implementation notes:**
- `ReadAndParseGCSFile.setup()` initialises the GCS client once per worker (not per element)
- Files from unknown folders are silently dropped (no `TaggedOutput` emitted)
- Both JSON array and NDJSON formats are handled in the same code path
- DLQ message attributes: `{"source": "Dataflow Streaming", "table": "<stage_table_name>"}` — the `source` field lets consumers distinguish Dataflow vs. Airflow failures
- Custom SDK container required: Python 3.12 + Beam 2.69.0 image built via Cloud Build and pushed to Artifact Registry (`demo-pipeline-containers` repo)

---

### 5.2 Airflow Daily Reprocessing DAG + Cloud Run Job

**Schedule:** `0 12 * * *` (12 PM UTC daily)
**Purpose:** Catch files that Dataflow may have missed, and reprocess edits/late arrivals

**Task graph:**

```
reprocess_and_merge   (CloudRunExecuteJobOperator)
```

The DAG is **pure orchestration** — it has no imports from `python_scripts` and no direct GCP API calls. It triggers the Cloud Run Job and waits for it to complete. All compute runs inside the job.

**What the Cloud Run Job does (`airflow/cloud_run_job/main.py`):**
1. List GCS blobs under `{folder}/` where `blob.updated >= execution_date - 2 days`
2. Decompress and parse each `.json.gz` file
3. Build row dicts (schema-specific per folder)
4. Write to the stage table via **BigQuery Storage Write API** (`_default` COMMITTED stream, 500-row batches)
5. Run MERGE SQL for each folder (stage → gold), same logic as the BQ Scheduled Query

**Runtime values the DAG passes to the job (as env var overrides):**

| Env var | Source | Example |
|---|---|---|
| `EXECUTION_DATE` | `{{ ts }}` — Jinja-templated by Airflow | `2025-05-29T12:00:00+00:00` |
| `ENV` | `{{ var.value.env }}` — Airflow Variable `env` | `dev` |

**Key implementation notes:**

- `blob.updated` is always UTC-aware; `cutoff_dt` is forced UTC with `.replace(tzinfo=timezone.utc)`
- `schema_type: "explicit"` folders use a per-folder proto descriptor (`OrderEventRow`); `schema_type: "payload"` folders use `NotificationEventRow`
- All GCP imports are deferred inside functions in `python_scripts/` — Cloud Run imports them at runtime only
- `config.py` resolves `ENV` from `airflow.models.Variable` when Airflow is present, or from the `ENV` OS env var when running in Cloud Run
- DLQ message attributes: `{"source": "Airflow Daily Reprocessing", "table": "<stage_table_name>"}` — the `source` field lets consumers distinguish Airflow vs. Dataflow failures

**Airflow Variable required:**

| Variable | Values | Default |
|---|---|---|
| `env` | `dev`, `prod` | `dev` |

---

### 5.3 BigQuery Scheduled Queries — Hourly MERGE (`sql/03_merge_stage_to_gold.sql`)

Create **two separate** scheduled queries in BigQuery (one per block in the file).

| Query | Frequency | Stage scan window |
|---|---|---|
| Order_Events | Every hour | `DATE(ingestion_timestamp) >= CURRENT_DATE - 1` |
| Notification_Events | Every hour | `_partition_date >= CURRENT_DATE - 1` |

**MERGE behaviour (Order_Events):**
- Stage is deduplicated to the latest row per `record_id` inside the `USING` subquery before the match
- `WHEN MATCHED AND S.ingestion_timestamp > T.ingestion_timestamp THEN UPDATE` — a re-sent / edited record updates the gold row in place
- `WHEN NOT MATCHED THEN INSERT` — first time we see this `record_id`
- Idempotent: running multiple times produces the same result

---

## 6. Deduplication Strategy

Uniqueness is defined per schema:

| Folder | Unique row identity | Where applied |
|---|---|---|
| `Order_Events` | `record_id` (the primary key) | MERGE ON clause, view PARTITION BY, Airflow MERGE SQL |
| `Notification_Events` | `TO_JSON_STRING(payload)` — the full JSON object serialised to a string | MERGE ON clause, view PARTITION BY, Airflow MERGE SQL |

For `Order_Events`, the gold table declares `PRIMARY KEY (record_id) NOT ENFORCED`. BigQuery does **not** enforce the constraint at write time — it is an optimizer hint. Uniqueness is actually guaranteed by the MERGE: stage rows are deduped to one row per `record_id` (latest `ingestion_timestamp` wins) before the `ON T.record_id = S.record_id` match.

**Why stage can have duplicates:**
Both Dataflow (streaming, event-driven) and the Airflow DAG (daily batch) write to the same stage table. A file that arrives while Dataflow is momentarily restarting will be picked up by Airflow the next morning, producing a duplicate row in stage. The MERGE handles this — duplicates in stage are deduplicated in the `USING` subquery before the upsert into gold.

---

## 7. Environment Configuration

All environment-specific values live in `airflow/python_scripts/gcs_bq_pipeline/config.py`:

```python
BQ_PROJECT_ID  = "demo-datalake-12345"
GCP_PROJECT_ID = "demo-orchestration"
DATASET_ID     = "Demo_Output_GCS"

BUCKET = {
    "dev":  "demo_data_output_dev",
    "prod": "demo_data_output_prod",
}[ENV]
```

`ENV` is resolved in order:
1. **In Airflow** (DAG parse-time): `airflow.models.Variable.get("env", default_var="dev")`
2. **In Cloud Run** (runtime): `os.environ.get("ENV", "dev")` — injected per-execution by the DAG operator

The DAG itself contains no project IDs or table names — just the Cloud Run job name, region, and the two runtime env var overrides.

To add a new environment, add an entry to the `BUCKET` dict in `config.py` and set the Airflow Variable accordingly.

---

## 8. Deployment

See [SETUP_GUIDE.md](SETUP_GUIDE.md) for the full step-by-step walkthrough. The high-level order is:

1. **GCS bucket** — create with 90-day lifecycle delete rule (`demo-ingest`)
2. **Pub/Sub topic** — `demo-ingest-topic` (`demo-ingest`)
3. **GCS → Pub/Sub notification** — `gsutil notification create` (Cloud Shell)
4. **Pub/Sub pull subscription** — `demo-ingest-topic-sub`, 7-day retention, 600s ack deadline
5. **BigQuery dataset** — `Demo_Output_GCS` (`demo-datalake-12345`)
6. **BigQuery tables** — run `sql/01_create_tables.sql`
7. **BigQuery views** — run `sql/02_create_view.sql`
8. **Dataflow streaming job** — build image via Cloud Build, deploy with `DataflowRunner` (see `dataflow/gcp_deployment.txt`)
9. **BigQuery Scheduled Queries** — create one query from `sql/03_merge_stage_to_gold.sql`
10. **Cloud Run Job** — build image via Cloud Build, deploy job (see `airflow/cloud_run_job/deployment_script.txt`)
11. **Cloud Composer DAG** — upload `airflow/dag_daily_reprocessing.py`, set Airflow Variable `env`

---

## 9. Adding a New Source Folder

To onboard a third GCS folder (e.g., `Loyalty_Events`):

### Step 1 — Decide schema style

- **Explicit columns** (like Order_Events): preferred when the schema is stable and you need efficient clustering/filtering on specific fields and a clean primary key
- **payload JSON** (like Notification_Events): preferred when the schema is variable or unknown

### Step 2 — Add DDL (`sql/01_create_tables.sql`)

Add `CREATE TABLE` statements for the new stage and gold tables. For explicit columns, add `PARTITION BY` and `CLUSTER BY` clauses matching your query patterns, plus a `PRIMARY KEY (…) NOT ENFORCED` on the gold table.

### Step 3 — Add to `FOLDER_CONFIG` in `python_scripts/gcs_bq_pipeline/config.py`

```python
FOLDER_CONFIG = {
    ...
    "Loyalty_Events": {
        "stage_table": f"{BQ_PROJECT_ID}.{DATASET_ID}.STG_Loyalty_Events",
        "gold_table":  f"{BQ_PROJECT_ID}.{DATASET_ID}.Loyalty_Events",
        "schema_type": "explicit",   # or "payload"
    },
}
```

For `schema_type: "explicit"`, also add the column list constant (e.g., `LOYALTY_EVENTS_COLUMNS`, its primary key, and update-column list) and add the `elif folder == "Loyalty_Events":` branch in `gcs_loader.py`.

### Step 4 — Add routing in `dataflow/pipeline.py`

1. Add `--loyalty_events_table` CLI arg to `GCSIngestionOptions`
2. Add `LOYALTY_EVENTS_TAG` to `ReadAndParseGCSFile` and emit the tagged output in `process()`
3. Add a `WriteToBigQuery` sink for the new tag

### Step 5 — Add row builder in `cloud_run_job/main.py`

In `gcs_loader.py`, add the folder branch inside `reprocess_gcs_files()` — same pattern as the `Order_Events` block. The merge step in `main.py` picks it up automatically from `FOLDER_CONFIG`.

### Step 6 — Add MERGE block (`sql/03_merge_stage_to_gold.sql`)

Copy the appropriate block (Order_Events for explicit, Notification_Events for payload) and update table names, the primary key, and column lists.

### Step 7 — Add view (`sql/02_create_view.sql`)

Add a new `CREATE OR REPLACE VIEW` following the pattern for the chosen schema style.

---

## 10. Operational Notes

| Concern | Detail |
|---|---|
| Stage duplicates | Expected and intentional — both Dataflow and Airflow write to stage; the MERGE deduplicates on `record_id` before promoting to gold |
| MERGE window | Scheduled query scans 1 day of stage; Airflow MERGE scans 2 days — the wider window catches late-arriving files |
| Stage auto-expiry | Stage partitions expire after 7 days; by that time they will have been merged into gold at least 168 times |
| Streaming vs batch gap | The view (`*_vw`) bridges the up-to-1-hour lag between Dataflow writing to stage and the next MERGE run |
| Primary key | `record_id` is declared `NOT ENFORCED` — BigQuery never rejects a duplicate; the MERGE is what guarantees one gold row per `record_id` |
| Schema changes | Update `01_create_tables.sql` (use `ALTER TABLE` or recreate), then update the column constant in `config.py` and the field mapping in both `pipeline.py` and `gcs_loader.py` |
| Dataflow restart | The `_default` COMMITTED stream in the Storage Write API does not guarantee exactly-once; duplicate stage rows are handled by the MERGE |
| proto field names | BigQuery Storage Write API maps proto field names to BQ column names by exact string match — column name case must match exactly between the proto descriptor and the BQ table schema |

# GCS → BigQuery Pipeline — GCP Setup Guide

## Architecture Overview

```
GCS Bucket: demo_data_output_dev  (project: demo-ingest)
  ├── Order_Events/*.json.gz
  └── Notification_Events/*.json.gz
        │ OBJECT_FINALIZE event
        ▼
  Pub/Sub Topic: demo-ingest-topic  (demo-ingest)
        │ pull subscription: demo-ingest-topic-sub
        ▼
  Dataflow Streaming Job  (demo-orchestration, us-west1)
        │ routes by folder → BQ stage tables
        ▼
  BigQuery: Demo_Output_GCS dataset  (demo-datalake-12345)
    STG_Order_Events  (7-day partition expiry)
        │ BQ Scheduled Query — every hour
        ▼
    Order_Events  (gold, permanent, record_id PK)
        ▼
    Order_Events_vw  (deduped UNION stage + gold)

  Airflow DAG — daily 12 PM UTC  (Cloud Composer, demo-orchestration)
    └─ Cloud Run Job: demo-bq-reprocessing-job
         • Reprocesses last 2 days of GCS files → stage
         • Runs MERGE stage → gold  (same SQL as scheduled query)
```

**Three GCP projects:**

| Project | Owns |
|---|---|
| `demo-ingest` | GCS source bucket, Pub/Sub topic/subscription, DLQ topic |
| `demo-orchestration` | Dataflow job, Cloud Run Job, Composer, Dataflow staging bucket |
| `demo-datalake-12345` | BigQuery dataset and tables |

---

## Step 1 — Pub/Sub Subscription (demo-ingest)

The Pub/Sub topic `demo-ingest-topic` and GCS → Pub/Sub notification already exist.
Create the pull subscription if it doesn't exist:

1. Go to **Pub/Sub > Topics** in project `demo-ingest`.
2. Click `demo-ingest-topic` → **Subscriptions** tab → **Create Subscription**.
3. Subscription ID: `demo-ingest-topic-sub`
4. Delivery type: **Pull**
5. Message retention: `7 days`
6. Acknowledgement deadline: `600` seconds
7. Click **Create**.

---

## Step 2 — BigQuery Dataset and Tables (demo-datalake-12345)

1. Go to **BigQuery** in project `demo-datalake-12345`.
2. Create dataset `Demo_Output_GCS` (region: `us-west1`).
3. Open the **Query editor** and run `sql/01_create_tables.sql`.
4. Run `sql/02_create_view.sql` to create the unified views.

---

## Step 3 — BigQuery Scheduled Query — Hourly MERGE

1. Go to **BigQuery > Scheduled Queries > Create Scheduled Query** in `demo-datalake-12345`.
2. Paste the `Order_Events` block from `sql/03_merge_stage_to_gold.sql`.
3. Schedule: every **1 hour**.
4. Leave destination dataset/table blank (the MERGE writes directly to the gold table).
5. Click **Save**.

> The MERGE is idempotent — running it multiple times produces the same result.
> It deduplicates stage on `record_id`, then upserts into gold (insert new records,
> update an existing record when a newer `ingestion_timestamp` arrives).

---

## Step 4 — Dataflow Staging Bucket (demo-orchestration)

The dedicated staging bucket must be in the same project as the Dataflow job.

```bash
gcloud storage buckets create gs://demo_dataflow_staging \
  --project=demo-orchestration \
  --location=us-west1 \
  --uniform-bucket-level-access
```

---

## Step 5 — Artifact Registry Repos (demo-orchestration)

Two repos — one for the Dataflow SDK container, one for the Cloud Run Job image:

```bash
# Dataflow SDK container
gcloud artifacts repositories create demo-pipeline-containers \
  --repository-format=docker \
  --location=us-west1 \
  --project=demo-orchestration

# Cloud Run Job image
gcloud artifacts repositories create demo-bq-reprocessing \
  --repository-format=docker \
  --location=us-west1 \
  --project=demo-orchestration
```

---

## Step 6 — IAM Permissions

The Dataflow and Cloud Run workers share the default Compute Engine service account
`000000000000-compute@developer.gserviceaccount.com`. Grant cross-project roles once:

```bash
SA="000000000000-compute@developer.gserviceaccount.com"

# Read source GCS files (demo-ingest)
gcloud projects add-iam-policy-binding demo-ingest \
  --member="serviceAccount:${SA}" --role="roles/storage.objectViewer"

# Publish to DLQ topic (demo-ingest)
gcloud projects add-iam-policy-binding demo-ingest \
  --member="serviceAccount:${SA}" --role="roles/pubsub.publisher"

# Subscribe to ingest topic (demo-ingest)
gcloud projects add-iam-policy-binding demo-ingest \
  --member="serviceAccount:${SA}" --role="roles/pubsub.subscriber"

# Write to BigQuery stage tables (demo-datalake-12345)
gcloud projects add-iam-policy-binding demo-datalake-12345 \
  --member="serviceAccount:${SA}" --role="roles/bigquery.dataEditor"

# Run BigQuery jobs (demo-datalake-12345)
gcloud projects add-iam-policy-binding demo-datalake-12345 \
  --member="serviceAccount:${SA}" --role="roles/bigquery.jobUser"

# Invoke Cloud Run Jobs (demo-orchestration)
gcloud projects add-iam-policy-binding demo-orchestration \
  --member="serviceAccount:${SA}" --role="roles/run.invoker"
```

---

## Step 7 — Deploy the Dataflow Streaming Job

Follow `dataflow/gcp_deployment.txt` (run from the `dataflow/` directory in Cloud Shell).
High-level steps:

1. Upload the `dataflow/` folder to Cloud Shell and extract it.
2. Build and push the SDK container image via Cloud Build:
   ```bash
   IMAGE="us-west1-docker.pkg.dev/demo-orchestration/demo-pipeline-containers/demo-bq-pipeline:2.69.0"
   gcloud builds submit . --tag="$IMAGE" --machine-type=e2-highcpu-8 --timeout=15m \
     --project=demo-orchestration
   ```
3. Install Beam in Cloud Shell: `pip install apache-beam[gcp]==2.69.0`
4. Submit the streaming job:
   ```bash
   python pipeline.py \
     --runner=DataflowRunner \
     --project=demo-orchestration \
     --region=us-west1 \
     --job_name=demo-bq-ingestion \
     --temp_location=gs://demo_dataflow_staging/dataflow/tmp \
     --staging_location=gs://demo_dataflow_staging/dataflow/staging \
     --sdk_container_image=$IMAGE \
     --sdk_location=container \
     --subnetwork=regions/us-west1/subnetworks/demo-orchestration-subnet-us-west1 \
     --no_use_public_ips \
     --input_subscription=projects/demo-ingest/subscriptions/demo-ingest-topic-sub \
     --order_events_table=demo-datalake-12345:Demo_Output_GCS.STG_Order_Events \
     --dlq_topic=projects/demo-ingest/topics/demo_output_gcs_DEADLETTER \
     --streaming \
     --enable_streaming_engine
   ```
5. Verify in **Dataflow > Jobs** — status should be **Running**.

---

## Step 8 — Deploy the Cloud Run Job

Follow `airflow/cloud_run_job/deployment_script.txt` (run from the `cloud_run_job/` directory in Cloud Shell).
High-level steps:

1. Upload the `airflow/` folder to Cloud Shell and extract it.
2. Copy `python_scripts/` into the build context, build and push the image:
   ```bash
   cp -r ../python_scripts .
   gcloud builds submit \
     --tag "us-west1-docker.pkg.dev/demo-orchestration/demo-bq-reprocessing/demo-bq-reprocessing:latest"
   rm -rf python_scripts
   ```
3. Create the Cloud Run Job:
   ```bash
   gcloud run jobs create demo-bq-reprocessing-job \
     --image us-west1-docker.pkg.dev/demo-orchestration/demo-bq-reprocessing/demo-bq-reprocessing:latest \
     --parallelism 2 --cpu 1 --memory 1Gi --task-timeout 10m \
     --region us-west1 \
     --set-env-vars="ENV=dev"
   ```
4. Test manually:
   ```bash
   gcloud run jobs execute demo-bq-reprocessing-job \
     --region=us-west1 \
     --update-env-vars="EXECUTION_DATE=$(date -u +%Y-%m-%dT%H:%M:%S+00:00),ENV=dev" \
     --wait
   ```
5. Verify rows appear in `STG_Order_Events` and then in `Order_Events` after the merge runs.

---

## Step 9 — Cloud Composer (Airflow) DAG

### 9a — Set the Airflow Variable

In the Airflow web UI: **Admin > Variables > +**

| Key | Value |
|---|---|
| `env` | `dev` (or `prod` for production) |

### 9b — Install the Google Cloud Run provider

In your Composer environment → **PyPI Packages** tab, add:

```
apache-airflow-providers-google>=8.8.0
```

> `CloudRunExecuteJobOperator` was added in providers-google 8.8.0.

### 9c — Upload the DAG

```bash
gsutil cp airflow/dag_daily_reprocessing.py gs://<your-composer-dags-bucket>/dags/
```

In the Airflow UI, confirm `demo_bq_daily_reprocessing` appears and toggle it **On**.

### 9d — Verify

Trigger a manual run in the Airflow UI. The single task `reprocess_and_merge` should:
1. Start a Cloud Run Job execution
2. Poll until it completes
3. Turn green

---

## Redeployment (after code changes)

| What changed | Action |
|---|---|
| `dataflow/pipeline.py` | Rebuild Dataflow image (Steps 7.1–7.2), drain old job, resubmit (Step 7.4) |
| `airflow/cloud_run_job/` or `python_scripts/` | Rebuild Cloud Run image (Step 8.1–8.2), update job (see `deployment_script.txt`) |
| `airflow/dag_daily_reprocessing.py` | Re-upload DAG file to Composer bucket (Step 9c) |
| `sql/*.sql` | Re-run the relevant SQL in BigQuery |

---

## DLQ (Dead-Letter Queue)

Both Dataflow and the Cloud Run Job publish failed messages to:
`projects/demo-ingest/topics/demo_output_gcs_DEADLETTER`

**Message format:**

| Field | Description |
|---|---|
| `data` (JSON) | `{"source_file": "gs://...", "error": "..."}` for file failures; `{"row": {...}, "error": "..."}` for BQ insert failures |
| attribute `source` | `"Dataflow Streaming"` or `"Airflow Daily Reprocessing"` |
| attribute `table` | Stage table name, e.g. `STG_Order_Events` |

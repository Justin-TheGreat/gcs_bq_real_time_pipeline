import os

BQ_PROJECT_ID  = "demo-datalake-12345"      # BigQuery target project
GCP_PROJECT_ID = "demo-orchestration"       # Dataflow / Pub/Sub / Composer
DATASET_ID     = "Demo_Output_GCS"

# In Airflow, ENV comes from the Airflow Variable "env".
# In Cloud Run, it comes from the ENV environment variable set at deploy time.
try:
    from airflow.models import Variable
    ENV = Variable.get("env", default_var="dev")
except ImportError:
    ENV = os.environ.get("ENV", "dev")

BUCKET = {
    "dev":  "demo_data_output_dev",
    "prod": "demo_data_output_prod",
}[ENV]

DLQ_TOPIC = "projects/demo-ingest/topics/demo_output_gcs_DEADLETTER"

# schema_type "explicit" → maps JSON keys to named BQ columns
# schema_type "payload"  → stores full record as payload JSON column
FOLDER_CONFIG = {
    "Order_Events": {
        "stage_table": f"{BQ_PROJECT_ID}.{DATASET_ID}.STG_Order_Events",
        "gold_table":  f"{BQ_PROJECT_ID}.{DATASET_ID}.Order_Events",
        "schema_type": "explicit",
    },
    # NOTIFICATION_EVENTS: not yet active
    # "Notification_Events": {
    #     "stage_table": f"{BQ_PROJECT_ID}.{DATASET_ID}.STG_Notification_Events",
    #     "gold_table":  f"{BQ_PROJECT_ID}.{DATASET_ID}.Notification_Events",
    #     "schema_type": "payload",
    # },
}

# Ordered columns for Order_Events (must match BQ table definition).
# Primary key first, then business columns, then metadata.
ORDER_EVENTS_COLUMNS = [
    "record_id",
    "order_date", "ship_date", "order_ref", "source_code",
    "category", "status", "channel", "event_timestamp",
    "customer_name", "region_code", "contact_email",
    "source_file", "ingestion_timestamp",
]

# Primary key column for Order_Events (gold dedup + MERGE ON clause).
ORDER_EVENTS_PK = "record_id"

# Non-key columns the MERGE updates when a newer row arrives for the same PK.
ORDER_EVENTS_UPDATE_COLS = [c for c in ORDER_EVENTS_COLUMNS if c != ORDER_EVENTS_PK]

REPROCESS_DAYS = 2

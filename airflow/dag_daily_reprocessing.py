"""
Airflow DAG: daily GCS → stage → gold reprocessing

Orchestration only. All compute (GCS download, BQ stage writes, MERGE to gold)
runs inside the Cloud Run Job "demo-bq-reprocessing-job".

The DAG passes two runtime values via env var overrides:
  EXECUTION_DATE — ISO 8601 timestamp of this DAG run ({{ ts }})
  ENV            — "dev" or "prod", from Airflow Variable "env"
"""

from datetime import timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="demo_bq_daily_reprocessing",
    default_args={
        "owner":           "airflow",
        "depends_on_past": False,
        # "email_on_failure": True,
        # "email":           ["your-team@example.com"],
        "retries":         2,
        "retry_delay":     timedelta(minutes=5),
    },
    description="Daily GCS .json.gz → stage → gold reprocessing",
    schedule_interval="0 12 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["gcs", "bigquery", "reprocessing"],
) as dag:

    CloudRunExecuteJobOperator(
        task_id="reprocess_and_merge",
        project_id="demo-orchestration",
        region="us-west1",
        job_name="demo-bq-reprocessing-job",
        overrides={
            "container_overrides": [
                {
                    "env": [
                        {"name": "EXECUTION_DATE", "value": "{{ ts }}"},
                        {"name": "ENV",            "value": "{{ var.value.env }}"},
                    ]
                }
            ]
        },
        deferrable=False,
        gcp_conn_id="google_cloud_default",
    )

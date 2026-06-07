"""
Cloud Run Job entrypoint — GCS → BigQuery stage reprocessing + merge to gold.

Steps (run sequentially):
  1. Download and parse .json.gz files from GCS modified within the lookback window
  2. Write rows to BigQuery stage tables (Storage Write API)
  3. Run MERGE from stage → gold for each configured folder

Environment variables (injected per-execution by the Airflow DAG via overrides):
  EXECUTION_DATE   ISO 8601 datetime; defaults to now() UTC when run ad-hoc.
  ENV              "dev" or "prod"; controls which GCS bucket is scanned.
"""

import logging
import os
import sys
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(message)s",
)


def _run_merges():
    from google.cloud import bigquery
    from python_scripts.gcs_bq_pipeline.config import FOLDER_CONFIG, BQ_PROJECT_ID
    from python_scripts.gcs_bq_pipeline.merge_sql import build_merge_sql

    client = bigquery.Client(project=BQ_PROJECT_ID)
    for folder, cfg in FOLDER_CONFIG.items():
        sql = build_merge_sql(cfg["schema_type"], cfg["stage_table"], cfg["gold_table"])
        logging.info("MERGE start: %s → %s", cfg["stage_table"], cfg["gold_table"])
        job = client.query(sql)
        job.result()
        logging.info("MERGE done:  %s", folder)


def main():
    execution_date_str = os.environ.get("EXECUTION_DATE", "")
    if execution_date_str:
        try:
            execution_date = datetime.fromisoformat(
                execution_date_str.replace("Z", "+00:00")
            )
        except ValueError:
            logging.error("Invalid EXECUTION_DATE: %s", execution_date_str)
            sys.exit(1)
    else:
        execution_date = datetime.now(timezone.utc)

    logging.info("execution_date=%s  env=%s", execution_date.isoformat(), os.environ.get("ENV", "dev"))

    from python_scripts.gcs_bq_pipeline.gcs_loader import reprocess_gcs_files
    reprocess_gcs_files(execution_date=execution_date)

    _run_merges()

    logging.info("All done.")


if __name__ == "__main__":
    main()

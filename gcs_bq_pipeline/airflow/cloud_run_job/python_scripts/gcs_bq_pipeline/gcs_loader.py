import gzip
import json
import logging
from datetime import datetime, timedelta, timezone

from python_scripts.gcs_bq_pipeline.bq_storage_write import append_rows_to_bq
from python_scripts.gcs_bq_pipeline.config import (
    BUCKET,
    BQ_PROJECT_ID,
    ORDER_EVENTS_COLUMNS,
    FOLDER_CONFIG,
    GCP_PROJECT_ID,
    REPROCESS_DAYS,
)
from python_scripts.gcs_bq_pipeline.dlq import publish_to_dlq


def reprocess_gcs_files(execution_date=None):
    from google.cloud import storage

    if execution_date is None:
        execution_date = datetime.now(timezone.utc)
    if execution_date.tzinfo is None:
        execution_date = execution_date.replace(tzinfo=timezone.utc)

    gcs_client = storage.Client(project=GCP_PROJECT_ID)

    cutoff_dt = (execution_date - timedelta(days=REPROCESS_DAYS)).replace(
        tzinfo=timezone.utc
    )

    for folder, cfg in FOLDER_CONFIG.items():
        stage_table = cfg["stage_table"]
        schema_type = cfg["schema_type"]
        _, dataset, table_id = stage_table.split(".")

        logging.info("Processing folder: %s → %s", folder, stage_table)

        blobs = [
            b
            for b in gcs_client.bucket(BUCKET).list_blobs(prefix=f"{folder}/")
            if b.name.endswith(".json.gz") and b.updated >= cutoff_dt
        ]
        logging.info(
            "  %d blob(s) modified since %s in gs://%s/%s/",
            len(blobs), cutoff_dt.isoformat(), BUCKET, folder,
        )

        rows         = []
        source_files = []

        for blob in blobs:
            source_file  = f"gs://{BUCKET}/{blob.name}"
            ingestion_ts = datetime.now(timezone.utc).isoformat()

            try:
                raw = gzip.decompress(blob.download_as_bytes()).decode("utf-8").strip()
                if not raw:
                    continue

                records = (
                    json.loads(raw)
                    if raw.startswith("[")
                    else [json.loads(line) for line in raw.splitlines() if line.strip()]
                )
            except Exception as exc:
                logging.error("Failed to read/parse %s: %s", source_file, exc)
                publish_to_dlq(
                    [{"source_file": source_file, "error": str(exc)}],
                    table_id,
                )
                continue

            source_files.append(source_file)

            for record in records:
                if schema_type == "explicit":
                    rows.append({
                        "record_id":            str(record.get("record_id") or ""),
                        "order_date":           str(record.get("order_date") or ""),
                        "ship_date":            str(record.get("ship_date") or ""),
                        "order_ref":            str(record.get("order_ref") or ""),
                        "source_code":          str(record.get("source_code") or ""),
                        "category":             str(record.get("category") or ""),
                        "status":               str(record.get("status") or ""),
                        "channel":              str(record.get("channel") or ""),
                        "event_timestamp":      str(record.get("event_timestamp") or ""),
                        "customer_name":        str(record.get("customer_name") or ""),
                        "region_code":          str(record.get("region_code") or ""),
                        "contact_email":        str(record.get("contact_email") or ""),
                        "source_file":         source_file,
                        "ingestion_timestamp": ingestion_ts,
                    })
                # NOTIFICATION_EVENTS: not yet active
                # else:
                #     rows.append({
                #         "payload":               json.dumps(record),
                #         "source_file":          source_file,
                #         "ingestion_timestamp":  ingestion_ts,
                #         "_partition_date":       str(blob.updated.date()),
                #     })

        logging.info("  Writing %d rows to %s", len(rows), stage_table)

        try:
            if schema_type == "explicit":
                append_rows_to_bq(
                    BQ_PROJECT_ID, dataset, table_id, rows,
                    columns=ORDER_EVENTS_COLUMNS,
                    proto_name="OrderEventRow",
                )
            # NOTIFICATION_EVENTS: not yet active
            # else:
            #     append_rows_to_bq(
            #         BQ_PROJECT_ID, dataset, table_id, rows,
            #         columns=["payload", "source_file", "ingestion_timestamp", "_partition_date"],
            #         proto_name="NotificationEventRow",
            #     )
        except Exception as exc:
            logging.error("BQ write failed for %s: %s", stage_table, exc)
            publish_to_dlq(
                [{"source_file": sf, "error": str(exc)} for sf in source_files],
                table_id,
            )
            raise

        logging.info("  Done: %s", folder)

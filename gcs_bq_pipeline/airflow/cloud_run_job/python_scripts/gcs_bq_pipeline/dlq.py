import json
import logging

from python_scripts.gcs_bq_pipeline.config import DLQ_TOPIC


def publish_to_dlq(messages: list, table_name: str) -> None:
    """
    Batch-publish failed messages to the dead-letter Pub/Sub topic.
    Each message carries {"source_file": ..., "error": ...} as data
    and {"table": table_name} as an attribute.
    DLQ publish errors are logged but never re-raised so they don't mask
    the underlying failure.
    """
    from google.cloud import pubsub_v1

    if not messages:
        return

    publisher = pubsub_v1.PublisherClient()
    futures = [
        publisher.publish(
            DLQ_TOPIC,
            data=json.dumps(msg).encode("utf-8"),
            table=table_name,
            source='Airflow Daily Reprocessing',
        )
        for msg in messages
    ]
    for future in futures:
        try:
            future.result()
        except Exception as exc:
            logging.error("DLQ publish failed (table=%s): %s", table_name, exc)

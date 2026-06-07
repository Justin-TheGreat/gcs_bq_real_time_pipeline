"""
Streaming Dataflow pipeline: GCS (via Pub/Sub) → BigQuery stage tables

Flow:
  Pub/Sub subscription
    → parse GCS OBJECT_FINALIZE notification (attributes)
    → download and decompress .json.gz file from GCS
    → route by folder name:
        Order_Events → explicit column rows → STG_Order_Events
    → failures → Pub/Sub DLQ topic (demo_output_gcs_DEADLETTER)

DLQ message format:
  data       : JSON — { source_file, error } for file failures;
                      { row, error } for BQ insert failures
  attributes : { "source": "Dataflow Streaming", "table": "<stage_table_name>" }

Schema note:
  Metadata columns use plain names (source_file, ingestion_timestamp) rather
  than underscore-prefixed names. This is required for the Storage Write API,
  which internally creates Python NamedTuples — and NamedTuple field names
  cannot start with an underscore.

  event_timestamp and ingestion_timestamp are TIMESTAMP in BigQuery (both tables
  are partitioned by DATE(ingestion_timestamp), which requires the column to be
  a date/timestamp type). For Storage Write API, TIMESTAMP columns must receive
  apache_beam.utils.timestamp.Timestamp objects, not ISO strings. Malformed
  upstream values become NULL via safe parsing in _to_beam_timestamp.

Worker-side name resolution:
  Names referenced inside DoFn methods are accessed via `self`, imported INSIDE
  the method body, or stored as class attributes. The DoFn class itself is NOT
  referenced by name from within its own methods, because module-level class
  names are not reliably available on Dataflow workers — use self instead.

Requires:
  Apache Beam >= 2.50 (for Storage Write API at-least-once with FailedRowsWithErrors).
  Tested on Beam 2.60.
"""

import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.io.gcp.pubsub import PubsubMessage, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Module-level imports used by the driver code (pipeline construction) only.
import json
import logging


# ── Folder / table routing ────────────────────────────────────────────────────

FOLDER_ORDER_EVENTS = "Order_Events"

FOLDER_TO_STAGE_TABLE = {
    FOLDER_ORDER_EVENTS: "STG_Order_Events",
}

# ── BigQuery schemas ──────────────────────────────────────────────────────────

ORDER_EVENTS_SCHEMA = {
    "fields": [
        {"name": "record_id",           "type": "STRING",    "mode": "NULLABLE"},
        {"name": "order_date",          "type": "STRING",    "mode": "NULLABLE"},
        {"name": "ship_date",           "type": "STRING",    "mode": "NULLABLE"},
        {"name": "order_ref",           "type": "STRING",    "mode": "NULLABLE"},
        {"name": "source_code",         "type": "STRING",    "mode": "NULLABLE"},
        {"name": "category",            "type": "STRING",    "mode": "NULLABLE"},
        {"name": "status",              "type": "STRING",    "mode": "NULLABLE"},
        {"name": "channel",             "type": "STRING",    "mode": "NULLABLE"},
        {"name": "event_timestamp",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "customer_name",       "type": "STRING",    "mode": "NULLABLE"},
        {"name": "region_code",         "type": "STRING",    "mode": "NULLABLE"},
        {"name": "contact_email",       "type": "STRING",    "mode": "NULLABLE"},
        {"name": "source_file",         "type": "STRING",    "mode": "NULLABLE"},
        {"name": "ingestion_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}

# ── Storage Write API tuning ──────────────────────────────────────────────────

BQ_TRIGGERING_FREQUENCY_SEC = 30


# ── Pipeline options ──────────────────────────────────────────────────────────

class GCSIngestionOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_subscription",
            required=True,
            help="projects/<project>/subscriptions/<subscription>",
        )
        parser.add_argument(
            "--order_events_table",
            required=True,
            help="project:dataset.STG_Order_Events",
        )
        parser.add_argument(
            "--dlq_topic",
            required=True,
            help="projects/<project>/topics/demo_output_gcs_DEADLETTER",
        )


# ── DLQ helpers (used inside DoFns and on the driver) ─────────────────────────

def _make_file_dlq_msg(row):
    """Format a file-level failure dict as a Pub/Sub DLQ message."""
    import json as _json
    from apache_beam.io.gcp.pubsub import PubsubMessage as _PubsubMessage

    table   = row.get("table", "unknown")
    payload = {k: v for k, v in row.items() if k != "table"}
    return _PubsubMessage(
        data=_json.dumps(payload).encode("utf-8"),
        attributes={"source": "Dataflow Streaming", "table": table},
    )


def _make_bq_dlq_msg(failed, table):
    """
    Format a failed BQ row for the DLQ.

    Storage Write API at-least-once emits failed rows in slightly different
    shapes across Beam versions (dict vs. tuple). Handle both defensively.
    """
    import json as _json
    from apache_beam.io.gcp.pubsub import PubsubMessage as _PubsubMessage

    if isinstance(failed, dict):
        row    = failed.get("row", failed.get("Row", {}))
        errors = failed.get("errors", failed.get("error_message", "BQ insert failure"))
    elif isinstance(failed, (tuple, list)) and len(failed) >= 3:
        # (destination, row, errors)
        row, errors = failed[1], failed[2]
    elif isinstance(failed, (tuple, list)) and len(failed) == 2:
        # (row, errors)
        row, errors = failed[0], failed[1]
    else:
        row, errors = failed, "BQ insert failure (unknown shape)"

    payload = {
        "row":   row,
        "error": str(errors),
    }
    return _PubsubMessage(
        data=_json.dumps(payload, default=str).encode("utf-8"),
        attributes={"source": "Dataflow Streaming", "table": table},
    )


# ── Beam DoFns ────────────────────────────────────────────────────────────────
# IMPORTANT:
#   - Every name used inside a DoFn method is imported INSIDE that method.
#   - The DoFn class is never referenced by name from inside its own methods.
#     Use `self` for instance methods and class-attribute lookups.
#   - Constants are class attributes (pickled with the DoFn), not module globals.

class ExtractGCSPath(beam.DoFn):
    """
    Parse a Pub/Sub GCS notification and yield (bucket, object).
    Silently drops non-OBJECT_FINALIZE events and non-.json.gz files.
    Routes malformed messages to DEAD_LETTER_TAG.
    """

    MAIN_TAG        = "gcs_path"
    DEAD_LETTER_TAG = "dead_letter"

    def process(self, pubsub_message):
        import logging
        import apache_beam as beam

        try:
            attrs      = pubsub_message.attributes or {}
            event_type = attrs.get("eventType")
            object_id  = attrs.get("objectId", "")
            bucket_id  = attrs.get("bucketId")

            if event_type != "OBJECT_FINALIZE":
                return
            if not object_id.endswith(".json.gz"):
                return
            if not bucket_id:
                raise ValueError(f"Missing bucketId; attrs={dict(attrs)}")

            yield beam.pvalue.TaggedOutput(self.MAIN_TAG, (bucket_id, object_id))

        except Exception as exc:
            logging.error("Failed to parse Pub/Sub attributes: %s", exc)
            yield beam.pvalue.TaggedOutput(self.DEAD_LETTER_TAG, {
                "table":       "unknown",
                "source_file": "unknown",
                "error":       f"PubSub parse failure: {exc}",
            })


class ReadAndParseGCSFile(beam.DoFn):
    """
    Download and decompress a .json.gz GCS file, emit rows tagged by target table.

    Tags:
      ORDER_EVENTS_TAG — explicit-column rows for STG_Order_Events
      DEAD_LETTER_TAG  — file-level failures (download / decompress / parse / unknown folder)
    """

    ORDER_EVENTS_TAG = "order_events"
    DEAD_LETTER_TAG  = "dead_letter"

    # Worker-safe constants — re-declared here, do not rely on module globals.
    FOLDER_ORDER_EVENTS   = "Order_Events"
    FOLDER_TO_STAGE_TABLE = {
        "Order_Events": "STG_Order_Events",
    }

    def setup(self):
        from google.cloud import storage
        self._gcs = storage.Client()
        # Build folder → (output tag, row builder) dispatch table on the worker.
        # Bind instance methods so workers don't have to resolve them by class name.
        self._folder_handlers = {
            self.FOLDER_ORDER_EVENTS: (self.ORDER_EVENTS_TAG, self._build_order_events_row),
        }

    def _to_beam_timestamp(self, value):
        """
        Safely convert a value (string, datetime, or None) to a Beam Timestamp.
        Returns None on parse failure so the BQ column receives NULL instead of
        failing the whole batch.

        Accepts ISO 8601 strings (including the trailing 'Z' for UTC) and
        timezone-naive strings (assumed UTC).
        """
        from datetime import datetime, timezone
        from apache_beam.utils.timestamp import Timestamp

        if value is None or value == "":
            return None
        if isinstance(value, datetime):
            dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
            return Timestamp.from_utc_datetime(dt.astimezone(timezone.utc))
        if isinstance(value, str):
            try:
                # fromisoformat handles 'Z' in 3.11+; replace defensively for older runtimes.
                s = value.replace("Z", "+00:00")
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return Timestamp.from_utc_datetime(dt.astimezone(timezone.utc))
            except (ValueError, TypeError):
                return None
        return None

    def _build_order_events_row(self, record, source_file, now):
        return {
            "record_id":           record.get("record_id"),
            "order_date":          record.get("order_date"),
            "ship_date":           record.get("ship_date"),
            "order_ref":           record.get("order_ref"),
            "source_code":         record.get("source_code"),
            "category":            record.get("category"),
            "status":              record.get("status"),
            "channel":             record.get("channel"),
            "event_timestamp":     self._to_beam_timestamp(record.get("event_timestamp")),
            "customer_name":       record.get("customer_name"),
            "region_code":         record.get("region_code"),
            "contact_email":       record.get("contact_email"),
            "source_file":         source_file,
            "ingestion_timestamp": self._to_beam_timestamp(now),
        }

    def process(self, element):
        import gzip
        import json
        import logging
        from datetime import datetime, timezone
        import apache_beam as beam

        bucket_name, blob_name = element
        folder      = blob_name.split("/")[0]
        source_file = f"gs://{bucket_name}/{blob_name}"
        table       = self.FOLDER_TO_STAGE_TABLE.get(folder, "unknown")
        now         = datetime.now(timezone.utc)

        handler = self._folder_handlers.get(folder)
        if handler is None:
            yield beam.pvalue.TaggedOutput(self.DEAD_LETTER_TAG, {
                "table":       "unknown",
                "source_file": source_file,
                "error":       f"Unrecognised folder: {folder}",
            })
            return

        output_tag, row_builder = handler

        try:
            blob = self._gcs.bucket(bucket_name).blob(blob_name)
            raw  = gzip.decompress(blob.download_as_bytes()).decode("utf-8").strip()
            if not raw:
                logging.warning("Empty file skipped: %s", source_file)
                return

            records = (
                json.loads(raw)
                if raw.startswith("[")
                else [json.loads(line) for line in raw.splitlines() if line.strip()]
            )
        except Exception as exc:
            logging.error("Error processing %s: %s", source_file, exc)
            yield beam.pvalue.TaggedOutput(self.DEAD_LETTER_TAG, {
                "table":       table,
                "source_file": source_file,
                "error":       str(exc),
            })
            return

        for record in records:
            yield beam.pvalue.TaggedOutput(
                output_tag,
                row_builder(record, source_file, now),
            )


# ── BQ write + DLQ helper ─────────────────────────────────────────────────────

def write_to_bq_with_dlq(rows, table, schema, dlq_topic, stage_table_name, step_prefix):
    """
    Write rows to BQ via Storage Write API at-least-once, route failures to DLQ.

    AT_LEAST_ONCE is chosen because the hourly gold MERGE deduplicates anyway —
    it's cheaper than exactly-once and supports the FailedRowsWithErrors side output.

    Stream count is controlled by with_auto_sharding=True so Beam scales it
    automatically based on backlog. To pin a fixed count instead, set
    with_auto_sharding=False and pass --num_storage_write_api_streams=N
    on the command line.
    """
    result = (
        rows
        | f"{step_prefix}_Write" >> WriteToBigQuery(
            table,
            schema=schema,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            method=WriteToBigQuery.Method.STORAGE_WRITE_API,
            use_at_least_once=True,
            triggering_frequency=BQ_TRIGGERING_FREQUENCY_SEC,
            with_auto_sharding=True,
        )
    )

    # Storage Write API at-least-once exposes failed rows via the
    # "FailedRowsWithErrors" string key on the result PCollectionTuple.
    (
        result["FailedRowsWithErrors"]
        | f"{step_prefix}_FormatDLQ"  >> beam.Map(
            lambda x: _make_bq_dlq_msg(x, stage_table_name)
        )
        | f"{step_prefix}_PublishDLQ" >> WriteToPubSub(dlq_topic, with_attributes=True)
    )


# ── Pipeline ──────────────────────────────────────────────────────────────────

def run(argv=None):
    options = GCSIngestionOptions(argv)
    options.view_as(StandardOptions).streaming = True
    custom  = options.view_as(GCSIngestionOptions)

    with beam.Pipeline(options=options) as p:

        # Pub/Sub → (bucket, object)
        pubsub_parsed = (
            p
            | "ReadPubSub" >> ReadFromPubSub(
                subscription=custom.input_subscription,
                with_attributes=True,
            )
            | "ExtractGCSPath" >> beam.ParDo(ExtractGCSPath()).with_outputs(
                ExtractGCSPath.MAIN_TAG,
                ExtractGCSPath.DEAD_LETTER_TAG,
            )
        )

        # (bucket, object) → routed rows
        parsed = (
            pubsub_parsed[ExtractGCSPath.MAIN_TAG]
            | "ReadGCSFiles" >> beam.ParDo(ReadAndParseGCSFile()).with_outputs(
                ReadAndParseGCSFile.ORDER_EVENTS_TAG,
                ReadAndParseGCSFile.DEAD_LETTER_TAG,
            )
        )

        # File-level and PubSub-level failures → DLQ
        (
            (
                pubsub_parsed[ExtractGCSPath.DEAD_LETTER_TAG],
                parsed[ReadAndParseGCSFile.DEAD_LETTER_TAG],
            )
            | "FlattenFileDLQ" >> beam.Flatten()
            | "FormatFileDLQ"  >> beam.Map(_make_file_dlq_msg)
            | "PublishFileDLQ" >> WriteToPubSub(custom.dlq_topic, with_attributes=True)
        )

        # Order_Events → BQ + BQ failure DLQ
        write_to_bq_with_dlq(
            rows             = parsed[ReadAndParseGCSFile.ORDER_EVENTS_TAG],
            table            = custom.order_events_table,
            schema           = ORDER_EVENTS_SCHEMA,
            dlq_topic        = custom.dlq_topic,
            stage_table_name = "STG_Order_Events",
            step_prefix      = "OrderEvents",
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
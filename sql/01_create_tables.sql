-- =============================================================================
-- 01_create_tables.sql
-- Creates one stage table and one gold table for each GCS source folder.
-- Run once per environment; replace `demo-datalake-12345.Demo_Output_GCS`
-- with your own project/dataset.
--
-- Stage tables:
--   • Append-only; both Dataflow (streaming) and Airflow (batch) write here.
--   • Partitioned by DATE(ingestion_timestamp) with 7-day auto-expiration.
--   • Explicit columns (Order_Events) or payload JSON (Notification_Events).
--
-- Gold tables:
--   • Deduplicated authoritative store, updated by the hourly scheduled MERGE.
--   • No partition expiration (permanent).
--   • Order_Events declares a NOT ENFORCED PRIMARY KEY on record_id and is
--     clustered on it so the MERGE join on record_id stays cheap.
--   • Notification_Events (payload JSON) has no clustering key because
--     uniqueness is the full payload (TO_JSON_STRING).
-- =============================================================================


-- ============================================================
-- Order_Events  (explicit columns, record_id primary key)
-- ============================================================

CREATE OR REPLACE TABLE `demo-datalake-12345.Demo_Output_GCS.STG_Order_Events`
(
  record_id          STRING,
  order_date         DATE,
  ship_date          DATE,
  order_ref          STRING,
  source_code        STRING,
  category           STRING,
  status             STRING,
  channel            STRING,
  event_timestamp    TIMESTAMP,
  customer_name      STRING,
  region_code        STRING,
  contact_email      STRING,
  source_file          STRING,
  ingestion_timestamp  TIMESTAMP
)
PARTITION BY date(ingestion_timestamp)
CLUSTER BY record_id
OPTIONS (
  partition_expiration_days = 7,
  require_partition_filter  = FALSE,
  description = 'Stage for Order_Events GCS folder. Auto-expires after 7 days.'
);

CREATE OR REPLACE TABLE `demo-datalake-12345.Demo_Output_GCS.Order_Events`
(
  record_id          STRING NOT NULL,
  order_date         DATE,
  ship_date          DATE,
  order_ref          STRING,
  source_code        STRING,
  category           STRING,
  status             STRING,
  channel            STRING,
  event_timestamp    TIMESTAMP,
  customer_name      STRING,
  region_code        STRING,
  contact_email      STRING,
  source_file          STRING,
  ingestion_timestamp  TIMESTAMP,
  PRIMARY KEY (record_id) NOT ENFORCED
)
PARTITION BY date(ingestion_timestamp)
CLUSTER BY record_id
OPTIONS (
  description = 'Gold for Order_Events. Deduplicated by record_id; updated hourly by MERGE.'
);



/* NOTIFICATION_EVENTS: not yet active
-- ============================================================
-- Notification_Events  (payload JSON)
-- ============================================================

CREATE TABLE IF NOT EXISTS `demo-datalake-12345.Demo_Output_GCS.STG_Notification_Events`
(
  payload               JSON     NOT NULL,
  source_file          STRING,
  ingestion_timestamp  TIMESTAMP,
  _partition_date       DATE     NOT NULL
)
PARTITION BY _partition_date
OPTIONS (
  partition_expiration_days = 7,
  require_partition_filter  = FALSE,
  description = 'Stage for Notification_Events GCS folder. Auto-expires after 7 days.'
);

CREATE TABLE IF NOT EXISTS `demo-datalake-12345.Demo_Output_GCS.Notification_Events`
(
  payload               JSON     NOT NULL,
  source_file          STRING,
  ingestion_timestamp  TIMESTAMP,
  _last_updated         TIMESTAMP
)
OPTIONS (
  description = 'Gold for Notification_Events. Deduplicated; updated hourly by MERGE.'
);
*/




CREATE OR REPLACE TABLE `demo-datalake-12345.Demo_Output_GCS.demo_output_gcs_DEADLETTER`
(
  subscription_name STRING,
  message_id STRING,
  publish_time TIMESTAMP,
  attributes STRING,
  data JSON
)
PARTITION BY DATE(publish_time)
OPTIONS(
  description="Dead letter pubsub events for demo_output_gcs"
);

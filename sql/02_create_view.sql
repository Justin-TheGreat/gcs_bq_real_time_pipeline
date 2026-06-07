-- =============================================================================
-- 02_create_view.sql
-- One unified view per folder.  Each view unions the stage table (recent
-- partitions only, for cost) with the gold table, then dedups to keep the
-- freshest copy of every unique row at query time.
--
-- Order_Events_vw
--   Dedup key : record_id (matches the gold primary key + MERGE ON clause)
--   Stage filter: DATE(ingestion_timestamp) >= CURRENT_DATE - 2
--
-- Notification_Events_unified_vw
--   Dedup key : TO_JSON_STRING(payload)
--   Stage filter: _partition_date >= CURRENT_DATE - 1
-- =============================================================================


-- ============================================================
-- View 1: Order_Events
-- ============================================================

CREATE OR REPLACE VIEW `demo-datalake-12345.Demo_Output_GCS.Order_Events_vw` AS
WITH combined AS (

  SELECT
    record_id, order_date, ship_date, order_ref, source_code,
    category, status, channel, event_timestamp,
    customer_name, region_code, contact_email,
    source_file, ingestion_timestamp
  FROM `demo-datalake-12345.Demo_Output_GCS.Order_Events`

  UNION ALL

  -- Stage rows not yet merged into gold (last 2 days of partitions only).
  SELECT
    record_id, order_date, ship_date, order_ref, source_code,
    category, status, channel, event_timestamp,
    customer_name, region_code, contact_email,
    source_file, ingestion_timestamp
  FROM `demo-datalake-12345.Demo_Output_GCS.STG_Order_Events`
  WHERE DATE(ingestion_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)

),
deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY record_id
      ORDER BY ingestion_timestamp DESC
    ) AS _rn
  FROM combined
)
SELECT
  record_id, order_date, ship_date, order_ref, source_code,
  category, status, channel, event_timestamp,
  customer_name, region_code, contact_email
FROM deduped
WHERE _rn = 1;

/* NOTIFICATION_EVENTS: not yet active
-- ============================================================
-- View 2: Notification_Events
-- ============================================================

CREATE OR REPLACE VIEW `demo-datalake-12345.Demo_Output_GCS.Notification_Events_unified_vw` AS
WITH combined AS (

  SELECT
    payload,
    source_file,
    ingestion_timestamp
  FROM `demo-datalake-12345.Demo_Output_GCS.Notification_Events`

  UNION ALL

  -- Stage rows not yet merged into gold (last 1 day of partitions only).
  SELECT
    payload,
    source_file,
    ingestion_timestamp
  FROM `demo-datalake-12345.Demo_Output_GCS.STG_Notification_Events`
  WHERE _partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)

),
deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY TO_JSON_STRING(payload)
      ORDER BY ingestion_timestamp DESC
    ) AS _rn
  FROM combined
)
SELECT
  payload,
  source_file,
  ingestion_timestamp
FROM deduped
WHERE _rn = 1;
*/

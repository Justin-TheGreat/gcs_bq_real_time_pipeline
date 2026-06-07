-- =============================================================================
-- 03_merge_stage_to_gold.sql
-- Create TWO separate BigQuery Scheduled Queries (one per folder), each running
-- every hour.  Paste the relevant block into each scheduled query editor.
--
-- Order_Events: dedup + merge on record_id (the gold primary key).
--   Stage is deduped to the latest row per record_id before the match, so a
--   record that was edited and re-sent updates the gold row in place.
--   Partitioned by DATE(ingestion_timestamp); scan window is 1 day.
--
-- Notification_Events: dedup + merge on TO_JSON_STRING(payload).
--   Partitioned by _partition_date; scan window is 1 day.
-- =============================================================================


-- ============================================================
-- Scheduled Query 1: Order_Events  (run every hour)
-- ============================================================

MERGE `demo-datalake-12345.Demo_Output_GCS.Order_Events` AS T
USING (
  SELECT * EXCEPT (_rn)
  FROM (
    SELECT
      record_id,
      order_date,
      ship_date,
      order_ref,
      source_code,
      category,
      status,
      channel,
      event_timestamp,
      customer_name,
      region_code,
      contact_email,
      source_file,
      ingestion_timestamp,
      ROW_NUMBER() OVER (
        PARTITION BY record_id
        ORDER BY ingestion_timestamp DESC
      ) AS _rn
    FROM `demo-datalake-12345.Demo_Output_GCS.STG_Order_Events`
    WHERE DATE(ingestion_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  )
  WHERE _rn = 1
) AS S
ON T.record_id = S.record_id

WHEN MATCHED AND S.ingestion_timestamp > T.ingestion_timestamp THEN
  UPDATE SET
    T.order_date          = S.order_date,
    T.ship_date           = S.ship_date,
    T.order_ref           = S.order_ref,
    T.source_code         = S.source_code,
    T.category            = S.category,
    T.status              = S.status,
    T.channel             = S.channel,
    T.event_timestamp     = S.event_timestamp,
    T.customer_name       = S.customer_name,
    T.region_code         = S.region_code,
    T.contact_email       = S.contact_email,
    T.source_file         = S.source_file,
    T.ingestion_timestamp = S.ingestion_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    record_id, order_date, ship_date, order_ref, source_code,
    category, status, channel, event_timestamp,
    customer_name, region_code, contact_email,
    source_file, ingestion_timestamp
  )
  VALUES (
    S.record_id, S.order_date, S.ship_date, S.order_ref, S.source_code,
    S.category, S.status, S.channel, S.event_timestamp,
    S.customer_name, S.region_code, S.contact_email,
    S.source_file, S.ingestion_timestamp
  );


/* NOTIFICATION_EVENTS: not yet active
-- ============================================================
-- Scheduled Query 2: Notification_Events  (run every hour)
-- ============================================================

MERGE `demo-datalake-12345.Demo_Output_GCS.Notification_Events` AS T
USING (
  SELECT * EXCEPT (_rn)
  FROM (
    SELECT
      payload,
      source_file,
      ingestion_timestamp,
      ROW_NUMBER() OVER (
        PARTITION BY TO_JSON_STRING(payload)
        ORDER BY ingestion_timestamp DESC
      ) AS _rn
    FROM `demo-datalake-12345.Demo_Output_GCS.STG_Notification_Events`
    WHERE _partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  )
  WHERE _rn = 1
) AS S
ON TO_JSON_STRING(T.payload) = TO_JSON_STRING(S.payload)

WHEN NOT MATCHED THEN
  INSERT (payload, source_file, ingestion_timestamp, _last_updated)
  VALUES (S.payload, S.source_file, S.ingestion_timestamp, CURRENT_TIMESTAMP());
*/

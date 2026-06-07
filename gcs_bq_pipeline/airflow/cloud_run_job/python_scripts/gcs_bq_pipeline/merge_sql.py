from python_scripts.gcs_bq_pipeline.config import (
    ORDER_EVENTS_COLUMNS,
    ORDER_EVENTS_PK,
    ORDER_EVENTS_UPDATE_COLS,
)


def _build_merge_sql_order_events(stage_table: str, gold_table: str) -> str:
    select_cols = ",\n      ".join(ORDER_EVENTS_COLUMNS)
    update_set  = ",\n    ".join(f"T.{c} = S.{c}" for c in ORDER_EVENTS_UPDATE_COLS)
    insert_cols = ", ".join(ORDER_EVENTS_COLUMNS)
    insert_vals = ", ".join(f"S.{c}" for c in ORDER_EVENTS_COLUMNS)

    return f"""
MERGE `{gold_table}` AS T
USING (
  SELECT * EXCEPT (_rn)
  FROM (
    SELECT
      {select_cols},
      ROW_NUMBER() OVER (
        PARTITION BY {ORDER_EVENTS_PK}
        ORDER BY ingestion_timestamp DESC
      ) AS _rn
    FROM `{stage_table}`
    WHERE DATE(ingestion_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
  )
  WHERE _rn = 1
) AS S
ON T.{ORDER_EVENTS_PK} = S.{ORDER_EVENTS_PK}
WHEN MATCHED AND S.ingestion_timestamp > T.ingestion_timestamp THEN
  UPDATE SET
    {update_set}
WHEN NOT MATCHED THEN
  INSERT ({insert_cols})
  VALUES ({insert_vals})
"""


# NOTIFICATION_EVENTS: not yet active
# def _build_merge_sql_payload(stage_table: str, gold_table: str) -> str:
#     return f"""
# MERGE `{gold_table}` AS T
# USING (
#   SELECT * EXCEPT (_rn)
#   FROM (
#     SELECT
#       payload,
#       source_file,
#       ingestion_timestamp,
#       ROW_NUMBER() OVER (
#         PARTITION BY TO_JSON_STRING(payload)
#         ORDER BY ingestion_timestamp DESC
#       ) AS _rn
#     FROM `{stage_table}`
#     WHERE _partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
#   )
#   WHERE _rn = 1
# ) AS S
# ON TO_JSON_STRING(T.payload) = TO_JSON_STRING(S.payload)
# WHEN NOT MATCHED THEN
#   INSERT (payload, source_file, ingestion_timestamp, _last_updated)
#   VALUES (S.payload, S.source_file, S.ingestion_timestamp, CURRENT_TIMESTAMP())
# """


def build_merge_sql(schema_type: str, stage_table: str, gold_table: str) -> str:
    if schema_type == "explicit":
        return _build_merge_sql_order_events(stage_table, gold_table)
    # return _build_merge_sql_payload(stage_table, gold_table)  # NOTIFICATION_EVENTS: not yet active
    raise ValueError(f"Unsupported schema_type: {schema_type}")

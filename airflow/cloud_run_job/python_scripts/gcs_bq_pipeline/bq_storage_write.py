def _build_proto_descriptor(columns: list, name: str = "StageRow"):
    """Return a DescriptorProto with one STRING field per column (BQ auto-converts types)."""
    from google.protobuf import descriptor_pb2

    dp = descriptor_pb2.DescriptorProto()
    dp.name = name
    for i, col in enumerate(columns, start=1):
        f        = dp.field.add()
        f.name   = col
        f.number = i
        f.type   = descriptor_pb2.FieldDescriptorProto.TYPE_STRING
        f.label  = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    return dp


def _build_message_class(proto_desc):
    """Register the descriptor and return a Python protobuf message class."""
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    file_proto        = descriptor_pb2.FileDescriptorProto()
    file_proto.name   = f"{proto_desc.name.lower()}.proto"
    file_proto.syntax = "proto3"
    file_proto.message_type.add().CopyFrom(proto_desc)

    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_proto)
    desc = pool.FindMessageTypeByName(proto_desc.name)

    try:
        return message_factory.GetMessageClass(desc)
    except AttributeError:
        return message_factory.MessageFactory(pool=pool).GetPrototype(desc)


def _serialize_row(msg_class, row: dict) -> bytes:
    msg = msg_class()
    for field, value in row.items():
        if value is not None:
            setattr(msg, field, value if isinstance(value, str) else str(value))
    return msg.SerializeToString()


def append_rows_to_bq(
    project: str,
    dataset: str,
    table: str,
    rows: list,
    columns: list,
    proto_name: str = "StageRow",
):
    """Write rows to BigQuery via the Storage Write API (_default COMMITTED stream)."""
    from google.cloud.bigquery_storage_v1 import types, writer
    from google.cloud.bigquery_storage_v1.services.big_query_write import BigQueryWriteClient

    if not rows:
        return

    proto_desc   = _build_proto_descriptor(columns, name=proto_name)
    msg_class    = _build_message_class(proto_desc)
    write_client = BigQueryWriteClient()

    stream_name = f"projects/{project}/datasets/{dataset}/tables/{table}/_default"

    proto_schema = types.ProtoSchema()
    proto_schema.proto_descriptor.CopyFrom(proto_desc)

    request_template              = types.AppendRowsRequest()
    request_template.write_stream = stream_name
    request_template.proto_rows   = types.AppendRowsRequest.ProtoData(
        writer_schema=proto_schema
    )

    append_stream = writer.AppendRowsStream(write_client, request_template)

    BATCH_SIZE = 500
    try:
        for start in range(0, len(rows), BATCH_SIZE):
            batch      = rows[start : start + BATCH_SIZE]
            proto_rows = types.ProtoRows()
            for row in batch:
                proto_rows.serialized_rows.append(_serialize_row(msg_class, row))

            request            = types.AppendRowsRequest()
            request.proto_rows = types.AppendRowsRequest.ProtoData(rows=proto_rows)

            future = append_stream.send(request)
            future.result()
    finally:
        append_stream.close()

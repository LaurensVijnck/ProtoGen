# Proto-to-BigQuery

Proto-to-BigQuery is a Protocol Buffers compiler plugin.

## Automatic schema migration

Proto-to-BQ can be leveraged to create workflows supporting end-to-end automatic schema migration. 

Proto-to-BQ extracts a BigQuery table schema from an annotated Protobuf file. The plugin additionally generates associated mapper functions to convert a Protobuf message to a BigQuery table row. The resulting table row can subsequently be inserted into the BigQuery table as its guaranteed to satisfy the table schema.

## Real-time ingestion

The repository includes a generic pipeline template that uses the Proto-to-BQ to produce a streaming BigQuery ingestion pipeline.

## Infrastructure-as-Code

The repository includes the source code to create the BigQuery tables by Terraform.

## Supported features

### Table-level features

- [x] Table name
- [x] Table description
- [x] Table partitioning
- [x] Clustering

### Attribute-level features

- [x] Aliases
- [x] Field description
- [x] Default values
- [x] Batch attribute*

### Annotating a protocol buffers file

```proto
message Tag {
    string tag_name = 1;
    string tag_code = 2;
    string tag_namespace = 3;
}

message BatchEvent {
    repeated Tag tags = 1 [(description) = "Tags associated with the event"];
}

message Event {
    // Bigquery meta data
    option (bq_root_options).table_name = "event_table";
    option (bq_root_options).table_description = "ProtoToBQ generated table for events";
    option (bq_root_options).time_partitioning = true;
    option (bq_root_options).time_partitioning_expiration_ms = 15552000000;

    // Fields
    Client client = 1  [(required) = false, (description) = "Owner of the event"];
    repeated BatchEvent events = 2 [(required) = true, (batch_attribute) = true];
    optional int64 epoch_timestamp_millis = 3 [(required) = true, (time_partitioning_attribute) = true, (timestamp_attribute) = true, (alias) = "event_time"];
    optional string tenant_id = 4 [(required) = true, (clustering_attribute) = true];
}
```

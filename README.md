# Proto-to-BigQuery

Proto-to-BigQuery is a Protocol Buffers compiler plugin.

## Automatic schema migration

Proto-to-BQ can be leveraged to create workflows supporting end-to-end automatic schema migration. 

Proto-to-BQ extracts a BigQuery table schema from an annotated Protobuf file. The plugin additionally generates associated mapper functions to convert a message to a BigQuery table row. This table row can subsequently be inserted into the BigQuery table as its guaranteed to satisfy the table schema.

## Real-time ingestion

The repository includes a generic pipeline template that uses the Proto-to-BQ to produce a streaming BigQuery ingestion pipeline.

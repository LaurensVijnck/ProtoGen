#!/bin/bash

# Schema and clustering fields are base64 encoded due Terraform not supporting complex JSON objects
export SCHEMA_FIELDS_BASE64=$(jq '.schema_fields' -r < $1 | base64)
export CLUSTERING_FIELDS_BASE64=$(jq '.clustering_fields' -r < $1 | base64)
export EXPIRATION_BASE64=$(jq '.partitioning_expiration' -r < $1 | base64)

# Leverage jq tool to manipulate JSON
cat $1 \
  | jq --arg schema_base64 "$SCHEMA_FIELDS_BASE64" '.schema_fields = $schema_base64' \
  | jq --arg clustering_base64 "$CLUSTERING_FIELDS_BASE64"  '.clustering_fields = $clustering_base64' \
  | jq --arg expiration_base64 "$EXPIRATION_BASE64"  '.partitioning_expiration = $expiration_base64' \
  # | jq -r '.clustering_fields' | base64 --decode
#!/bin/bash

# https://cloud.google.com/bigquery/docs/managing-table-schemas#bq
bq -q update geometric-ocean-284614:dev_eu_lvi.lvi_test scripts/schema_new.json

# Retrieve schema
bq show \
--schema \
--format=prettyjson \
geometric-ocean-284614:dev_eu_lvi.lvi_test
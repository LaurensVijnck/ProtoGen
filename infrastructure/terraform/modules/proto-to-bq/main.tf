locals {

  module_name = "proto-to-bq"

  # Extract datasets and tables
  bigquery_tables = fileset(var.bigquery_schema_repository_path, "schema_*.json")
  tenant_datasets = [for d in var.tenants: "${d["meta"]["region"]}_${d["meta"]["name"]}"]

  # Compute product for table resource creation
  tenant_tables = setproduct(local.tenant_datasets, local.bigquery_tables)

  # Flatten the tenants
  # FUTURE: Check how this can be simplified, assumes that dash is not used in table names
  tenants_tables_flat = toset([for tuple in local.tenant_tables: "${tuple[0]}-${tuple[1]}"])
}

# [Storage] main bucket
resource "google_storage_bucket" "default" {
  name          = "${var.env}-${local.module_name}"
  location      = var.data_location_storage
}


# [External] table schema
data "external" "table_schema" {
  for_each = local.bigquery_tables
  program     = ["sh", "scripts/read_schema_as_base64.sh", "${var.bigquery_schema_repository_path}/${each.value}"]
}

# [BigQuery] root dataset
resource "google_bigquery_dataset" "proto_to_bq_datasets" {

  for_each = var.tenants

  dataset_id                 = "${var.env}_${var.tenants[each.key].meta.region}_${var.tenants[each.key].meta.name}"
  location                   = var.data_location_bigquery
  project                    = var.project
  delete_contents_on_destroy = true

  labels = {
    tenant    = lower(var.tenants[each.key].meta.friendly_name)
    region    = lower(var.tenants[each.key].meta.region)
    tenant_id = lower(var.tenants[each.key].meta.tenant_id)
    module    = "proto-to-bq"
  }
}

# [BigQuery] Tables
resource "google_bigquery_table" "tenant_parsed" {

  for_each = local.tenants_tables_flat

  # Ensure datasets are created before provisioning tables
  depends_on = [google_bigquery_dataset.proto_to_bq_datasets]

  # General
  dataset_id  = "${var.env}_${split("-", each.value)[0]}"
  table_id    = data.external.table_schema[split("-", each.value)[1]].result["table_name"]
  description = data.external.table_schema[split("-", each.value)[1]].result["table_description"]

  # Clustering fields is base64 encoded due to Terraform not supporting complex JSON objects
  clustering  = jsondecode(base64decode(data.external.table_schema[split("-", each.value)[1]].result["clustering_fields"]))

//  time_partitioning {
//    type  = "DAY"
//    field = data.external.bigquery_schema_parsed.result["partitioning_column"]
//  }

  # Biguery schema is base64 encoded due to Terraform not supporting complex JSON objects
  schema = base64decode(data.external.table_schema[split("-", each.value)[1]].result["schema_fields"])
}
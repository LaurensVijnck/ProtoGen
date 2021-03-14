locals {

  # FUTURE: Setup is currently tailored to a multi-tenant
  # context. Expose a 'namespace' abstraction in the future.

  module_name = "proto-to-bq"

  # Extract datasets and tables
  bigquery_tables = fileset(var.bigquery_schema_repository_path, "schema_*.json")
  tenant_datasets = [for d in var.tenants: "${d["meta"]["region"]}_${d["meta"]["name"]}"]

  # Compute product for table resource creation
  # https://www.terraform.io/docs/language/functions/setproduct.html
  tenant_tables = setproduct(local.tenant_datasets, local.bigquery_tables)

  # Flatten the tenants
  # FUTURE: Check how this can be simplified, assumes that dash is not used in table names
  tenants_tables_flat = toset([for tuple in local.tenant_tables: "${tuple[0]}-${tuple[1]}"])
}

######################################
# Cloud Storage
######################################

# [Storage] main bucket
# Main bucket for template/schema storage.
resource "google_storage_bucket" "default" {
  name          = "${var.env}-${local.module_name}"
  location      = var.data_location_storage
}

######################################
# Cloud PubSub
######################################

# [Pub/Sub] topic
# Topic for ingestion of proto-to-bq events
resource "google_pubsub_topic" "proto_to_bq_ingress_events" {

  # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/pubsub_topic
  name    = "${var.env}.${local.module_name}.ingess.v1"
  project = var.project

  # https://cloud.google.com/pubsub/docs/resource-location-restriction#message_storage_policy_in_new_topics
  # https://cloud.google.com/pubsub/docs/resource-location-restriction#performance_and_availability_implications
  message_storage_policy {
    allowed_persistence_regions = [
      "europe-west1"
    ]
  }

  labels = {
    module      = local.module_name
    environment = var.env
  }
}

# [Pub/Sub] subscription
# Topic for ingestion of proto-to-bq events
resource "google_pubsub_subscription" "proto_to_bq_ingress_events_subscription" {

  # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/pubsub_subscription
  project = var.project
  name    = "${var.env}.${local.module_name}.ingess.v1.subscription.dataflow"
  topic   = google_pubsub_topic.proto_to_bq_ingress_events.id

  expiration_policy {
    ttl = "" # Never expires
  }
}


######################################
# BigQuery
######################################

# [External] table schema
# Leverage external provider to read the BigQuery schema for each table.
data "external" "table_schema" {
  for_each    = local.bigquery_tables
  program     = ["sh", "scripts/read_schema_as_base64.sh", "${var.bigquery_schema_repository_path}/${each.value}"]
}

# [BigQuery] root dataset
# Create dataset for each tenant
resource "google_bigquery_dataset" "proto_to_bq_datasets" {

  for_each = var.tenants

  # Dataset specific attributes
  # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
  dataset_id                 = "${var.env}_${var.tenants[each.key].meta.region}_${var.tenants[each.key].meta.name}"
  location                   = var.data_location_bigquery
  project                    = var.project
  delete_contents_on_destroy = true

  labels = {
    tenant      = lower(var.tenants[each.key].meta.friendly_name)
    region      = lower(var.tenants[each.key].meta.region)
    tenant_id   = lower(var.tenants[each.key].meta.tenant_id)
    environment = var.env
    module      = local.module_name
  }
}

# [BigQuery] Tables
# Create BigQuery table for each combination of tenant and schema
resource "google_bigquery_table" "proto_to_bq_tables" {

  # FUTURE: Simplify code whenever Terraform supports unmarshalling complex JSON objects,
  # currently I worked around this by computing a base64 of complex values (pretty ugly).

  for_each = local.tenants_tables_flat

  # Ensure datasets are created before provisioning tables
  # https://www.terraform.io/docs/language/meta-arguments/depends_on.html
  depends_on = [google_bigquery_dataset.proto_to_bq_datasets]

  # Table specific attributes
  # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_table
  dataset_id  = "${var.env}_${split("-", each.value)[0]}"
  table_id    = data.external.table_schema[split("-", each.value)[1]].result["table_name"]
  description = data.external.table_schema[split("-", each.value)[1]].result["table_description"]

  # FUTURE: Use dynamic blocks to create this solely for tables with partitioning enabled.
  # https://www.terraform.io/docs/language/expressions/dynamic-blocks.html
  time_partitioning {
    type  = "DAY" // FUTURE: Support other types of partitioning
    field = data.external.table_schema[split("-", each.value)[1]].result["partitioning_field"]
    expiration_ms = jsondecode(base64decode(data.external.table_schema[split("-", each.value)[1]].result["partitioning_expiration"]))
  }

  # Fields are base64 encoded due to Terraform not supporting complex JSON objects
  clustering  = jsondecode(base64decode(data.external.table_schema[split("-", each.value)[1]].result["clustering_fields"]))
  schema      = base64decode(data.external.table_schema[split("-", each.value)[1]].result["schema_fields"])
}

######################################
# IAM
######################################

# [AIM] Service account
# Service account for the Dataflow job
resource "google_service_account" "sa" {
  project      = var.project
  account_id   = "sa-${local.module_name}-${var.env}"
  display_name = "SA for the ${local.module_name} ingestion pipeline"
}

# [IAM] dataflow worker
# Grant SA the dataflow worker role
resource "google_project_iam_member" "builtin_roles" {
  for_each = toset([
    "roles/dataflow.worker",   # running the job
    "roles/logging.logWriter", # writing logs
  ])

  role    = each.value
  member  = "serviceAccount:${google_service_account.sa.email}"
  project = var.project
}

# [IAM] bucket access
# Grant permission to proto-to-bq bucket
resource "google_storage_bucket_iam_member" "metl_storage_object_admin" {
  bucket = google_storage_bucket.default.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.sa.email}"
}

# [IAM] PubSub access
# Grant pipeline permission to subscribe to the subscription
resource "google_pubsub_subscription_iam_member" "ingess_events_read_permission" {
  project = var.project

  subscription = google_pubsub_subscription.proto_to_bq_ingress_events_subscription.id
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.sa.email}"
}

# [IAM] dataset access
# Grant SA access to generated tables
resource "google_bigquery_dataset_access" "proto_to_bq_dataset_access" {

  for_each = toset(local.tenant_datasets)

  # Ensure datasets are created before provisioning tables
  # https://www.terraform.io/docs/language/meta-arguments/depends_on.html
  depends_on = [google_bigquery_dataset.proto_to_bq_datasets]

  # Access specific attributes
  # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset_access
  dataset_id    = "${var.env}_${each.value}"
  role          = "roles/bigquery.dataOwner" # FUTURE: Revoke permission to delete the dataset
  user_by_email = google_service_account.sa.email
}



######################################
# Dataflow
######################################

# [Dataflow] Job
# Multi-tenant Dataflow job to stream events into generated BigQuery tables
resource "google_dataflow_job" "proto_to_bq_dataflow_job" {

  # Add count to enable toggling the dataflow job
  # https://www.terraform.io/docs/language/meta-arguments/count.html
  count      = var.enable_ingestion_pipeline ? 1 : 0

  # Ensure tables are provisioned before dataflow job is started
  # https://www.terraform.io/docs/language/meta-arguments/depends_on.html
  depends_on = [google_bigquery_table.proto_to_bq_tables]

  # Dataflow specific attributes
  # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job
  name                  = "${local.module_name}-v1"
  project               = var.project
  region                = var.region
  max_workers           = var.max_workers
  machine_type          = var.machine_type
  service_account_email = google_service_account.sa.email

  # Deploy pipeline based on the proto-to-bq template
  # FUTURE: Supply version through a parameter
  template_gcs_path = "gs://dev-lvi-templates/proto-to-bq/v1"
  temp_gcs_location = "gs://${google_storage_bucket.default.name}/app/${local.module_name}/v1"

  # Template parameters
  parameters = {
    environment             = var.env
    pubSubInputSubscription = google_pubsub_subscription.proto_to_bq_ingress_events_subscription.id
  }

  on_delete = "drain"
}


# Test
# [BigQuery] Tables
# Create BigQuery table for each combination of tenant and schema
resource "google_bigquery_table" "test_table" {

  # Table specific attributes
  # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_table
  dataset_id  = "dev_eu_lvi"
  table_id    = "lvi_test"
  description = "test"

  schema = <<EOF
  [
    {
      "mode": "REQUIRED",
      "name": "timestamp",
      "type": "TIMESTAMP"
    },
    {
    "mode": "NULLABLE",
    "name": "payloadString",
    "type": "STRING"
    }
  ]
  EOF
}


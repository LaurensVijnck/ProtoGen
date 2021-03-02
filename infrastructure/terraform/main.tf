module "proto-to-bq" {
  source = "./modules/proto-to-bq"

  project                = var.project
  env                    = var.env
  region                 = var.region
  zone                   = var.zone
  tenants                = var.tenants

  data_location_bigquery = var.data_location_bigquery
  data_location_storage  = var.data_location_storage

  # FUTURE: Point to GCS location
  bigquery_schema_repository_path = "/Users/lvijnck/Desktop/ProtoBQGeneration/ProtoToBQ/output/bigquery"
}
module "proto-to-bq" {
  source = "./modules/proto-to-bq"

  project                = var.project
  region                 = var.region
  zone                   = var.zone
  tenants                = var.tenants

  bigquery_schema_repository_path = "/Users/lvijnck/Desktop/ProtoBQGeneration/ProtoToBQ/output/bigquery"
}
output "tenant_tables" {
  value = local.tenant_tables
}

output "clustering" {
  value = jsondecode(base64decode(data.external.table_schema["schema_event.json"].result["clustering_fields"]))
}
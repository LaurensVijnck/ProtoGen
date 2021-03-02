output "tenant_tables" {
  value = local.tenants_tables_flat
}

output "clustering" {
  value = jsondecode(base64decode(data.external.table_schema["schema_event.json"].result["clustering_fields"]))
}
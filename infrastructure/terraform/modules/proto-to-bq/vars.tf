variable "project" {
  type        = string
  description = "GCP project ID"
}

variable "env" {
  description = "Environment"
  default = "dev"
}

variable "region" {
  description = "Default region for services and compute resources"
}

variable "zone" {
  description = "Default zone for services and compute resources"
}

variable "data_location_bigquery" {
  description = "Default location for BigQuery"
  default     = "EU"
}

variable "data_location_storage" {
  type        = string
  description = "Default location for blob storage"
  default     = "EU"
}

variable "tenants" {
  default = {}
}

variable "bigquery_schema_repository_path" {
  type        = string
  description = "Location of the BigQuery schemas directory"
}

variable "enable_ingestion_pipeline" {
  type        = bool
  description = "Boolean that indicates whether the ingestion pipeline should be created"
  default     = true
}

variable "max_workers" {
  type    = number
  default = 1
}

variable "machine_type" {
  type    = string
  default = "n1-standard-1"
}
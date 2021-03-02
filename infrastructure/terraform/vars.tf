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

variable "enable_active_components" {
  type        = bool
  description = "Boolean that indicates whether active components should be deployed, e.g., Dataflow jobs"
  default     = true
}


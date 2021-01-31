variable "project" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  description = "Default region for services and compute resources"
}

variable "zone" {
  description = "Default zone for services and compute resources"
}

variable "data_location_bigquery" {
  description = "Default location for storage (e.g. EU)"
  default     = "EU"
}
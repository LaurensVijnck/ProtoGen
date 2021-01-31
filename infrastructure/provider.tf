terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "~> 3.5.0"
    }

    null = {
      source  = "hashicorp/null"
      version = "~> 3.0.0"
    }

    external = {
      source  = "hashicorp/external"
      version = "~> 2.0.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone

  credentials = file("account.json")
}

# BigQuery dataset
resource "google_bigquery_dataset" "proto_to_bq_dataset" {
  dataset_id                 = "proto_to_bq_dataset"
  location                   = var.data_location_bigquery
  project                    = var.project
  delete_contents_on_destroy = true
}
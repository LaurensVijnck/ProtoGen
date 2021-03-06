terraform {
  required_providers {
    google = {
      source = "hashicorp/google"

      # NOTE: Newer version does not support in-place BigQuery table schema changes.
      version = "~> 3.52.0"
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
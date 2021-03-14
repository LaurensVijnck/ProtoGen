# Project settings
project                   = "geometric-ocean-284614"
env                       = "dev"
zone                      = "europe-west1-b"
region                    = "europe-west1"
data_location_bigquery    = "EU"
data_location_storage     = "EU"
enable_active_components  = false

# Specify tenants
tenants = {
  eu-lvi = {
    meta = {
      region = "eu"
      name = "lvi"
      friendly_name = "Laurens",
      tenant_id = "3757A461-4F06-4054-BC3D-CC8075DF96CE",
    }
  },

  eu-jda = {
    meta = {
      region = "eu"
      name = "jda"
      friendly_name = "Jonny",
      tenant_id = "3757A461-4F06-4054-BC3D-CC8075DF96CE",
    }
  }
}
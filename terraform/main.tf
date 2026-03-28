terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project     = "fraud-detection-project-491323"
  region      = "us-central1"
  credentials = file("../gcp-key.json")
}

resource "google_storage_bucket" "data_lake" {
  name          = "fraud-detection-bucket-491323"
  location      = "US"
  force_destroy = true
}

resource "google_bigquery_dataset" "data_warehouse" {
  dataset_id = "fraud_detection"
  location   = "US"
}
provider "google" {
  project = var.tgt_project_id
  region  = var.gcp_region
  zone    = var.gcp_rg_zone
}

data "google_project" "tgt_project" {
  project_id = var.tgt_project_id
}
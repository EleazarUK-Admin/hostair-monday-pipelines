variable "gcp_region" {
  description = "The default region where resources are to be created => multiregion US"
  type        = string
  default     = "US"
}

variable "gcp_rg_zone" {
  description = "The default zone, for the default region "
  type        = string
  default     = "us-west4"
}

# ===> ====
variable "repo_branch" {
  description = "Taken from the CICD. The name of the git repository branch"
  type        = string
}

variable "repo_commit" {
  description = "Taken from the CICD. The hash (revision id) of the git commit"
  type        = string
}

# ===> ====
variable "environment" {
  description = "Taken from the CICD. The name of the target environment (dev, test, uat, prod)"
  type        = string
}

variable "tgt_project_id" {
  description = "Taken from the CICD. The ID of the GCP target project (where all resources are managed)"
  type        = string
}

variable "wif_pool_name" {
  description = "The name of the workload identity pool"
  type        = string
}

variable "wif_provider_name" {
  description = "The name of the workload identity provider"
  type        = string
}


variable "cicd_sa_name" {
  description = "Taken from the CICD. The name (prefix) of the CICD service account used for deployment"
  type        = string
}

variable "main_sa_iam_roles_list" {
  description = "A list of IAM roles assigned to the main service account on the project level"
  type        = list(string)
  default = [
    "roles/iam.serviceAccountUser",
    "roles/logging.logWriter",
    "roles/errorreporting.writer",
  ]
}

# ===> ====
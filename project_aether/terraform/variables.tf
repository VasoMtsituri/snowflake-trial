variable "SVC_TERRAFORM_PROD_PUBLIC_KEY" {
  description = "SVC_TERRAFORM_PROD SA public key for authentication"
  type        = string
  default     = ""
}

variable "SVC_WAREHOUSE" {
  description = "Warehouse for service account programmatic tasks"
  type        = string
  default     = "SVC_WH_AUTOMATION"
}

variable "SVC_ROLE" {
  description = "Role with specific permissions for automation tasks"
  type        = string
  default     = "svc_role"
}

variable "SVC_USER" {
  description = "Service User name and login for automation tasks"
  type        = string
  default     = "svc_user"
}

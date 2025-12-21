terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "2.12.0"
    }
  }
}

provider "snowflake" {
  organization_name = ""
  account_name      = ""
  user              = ""
  role              = ""
  password          = ""
}

# 1. Create the Virtual Warehouse
resource "snowflake_warehouse" "svc_warehouse" {
  name           = var.SVC_WAREHOUSE
  comment        = "Warehouse for service account programmatic tasks"
  warehouse_size = "X-SMALL"
  auto_suspend   = 60   # Seconds of inactivity before sleeping
  auto_resume    = true # Wake up when a query is sent
}

# 2. Create a Role for the Service Account
# Best Practice: Don't grant permissions directly to users; use roles.
resource "snowflake_account_role" "svc_role" {
  name    = var.SVC_ROLE
  comment = "Role with specific permissions for automation tasks"
}

# 3. Create the Service User
resource "snowflake_service_user" "svc_user" {
  name             = var.SVC_USER
  login_name       = var.SVC_USER
  default_role     = snowflake_account_role.svc_role.name
  default_warehouse = snowflake_warehouse.svc_warehouse.name

  # For service accounts, RSA Public Keys are preferred over passwords
  rsa_public_key   = var.SVC_TERRAFORM_PROD_PUBLIC_KEY
  disabled         = false
}

# 4. Grant the Role to the Service User
resource "snowflake_grant_account_role" "user_role_assignment" {
  role_name = snowflake_account_role.svc_role.name
  user_name = snowflake_service_user.svc_user.name
}

# 5. Grant Warehouse Usage to the Role
resource "snowflake_grant_privileges_to_account_role" "warehouse_grant" {
  privileges = ["USAGE"]
  account_role_name = snowflake_account_role.svc_role.name
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.svc_warehouse.name
  }
}
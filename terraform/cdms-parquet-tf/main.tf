provider "aws" {
  region = var.region
  shared_credentials_file = var.shared_credentials_file
  profile = var.profile
}

data "aws_caller_identity" "current" {}

locals {
  environment = var.environment
  # Account ID used for getting the ECR host
  account_id = data.aws_caller_identity.current.account_id
  resource_prefix = "${var.project}-${var.environment}"
}
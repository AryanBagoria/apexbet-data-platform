terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }

  # Remote state is intentionally not configured by default so `terraform init`
  # works offline for validation. Configure a backend (S3 + DynamoDB lock) for
  # any real deployment.
  # backend "s3" {
  #   bucket         = "apexbet-tfstate"
  #   key            = "apexbet/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "apexbet-tflock"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = local.common_tags
  }
}

locals {
  common_tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
    DataClass   = "synthetic"
    CostCenter  = "data-platform"
  }

  name_prefix  = "${var.project}-${var.environment}"
  lake_bucket  = "${var.project}-lake-${var.environment}-${var.lake_bucket_suffix}"
  stream_name  = "${var.project}-events-${var.environment}"
  dlq_topic    = "${var.project}-events-dlq-${var.environment}"
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

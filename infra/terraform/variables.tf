variable "region" {
  description = "AWS region for the deployment."
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name used in resource naming."
  type        = string
  default     = "apexbet"
}

variable "environment" {
  description = "Environment name (dev/staging/prod)."
  type        = string
  default     = "dev"
}

variable "kinesis_shard_count" {
  description = "Number of Kinesis shards for the events stream."
  type        = number
  default     = 2
}

variable "kinesis_retention_hours" {
  description = "Kinesis stream retention in hours (24-8760)."
  type        = number
  default     = 48
}

variable "lake_bucket_suffix" {
  description = "Suffix to make the S3 bucket name globally unique."
  type        = string
  default     = "synthetic"
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days."
  type        = number
  default     = 14
}

variable "enable_ecr" {
  description = "Create an ECR repository for container images."
  type        = bool
  default     = true
}

variable "enable_secrets" {
  description = "Create a Secrets Manager secret for the ingestion API tokens."
  type        = bool
  default     = true
}

variable "enable_glue_catalog" {
  description = "Create a Glue catalog database for Athena queries."
  type        = bool
  default     = true
}

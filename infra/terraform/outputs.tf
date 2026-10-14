output "lake_bucket" {
  description = "S3 bucket holding the data lake."
  value       = module.storage.bucket_name
}

output "kinesis_stream_name" {
  description = "Kinesis Data Stream name for events."
  value       = module.streaming.stream_name
}

output "kinesis_stream_arn" {
  description = "Kinesis Data Stream ARN."
  value       = module.streaming.stream_arn
}

output "dlq_queue_url" {
  description = "SQS dead-letter queue URL."
  value       = module.messaging.dlq_queue_url
}

output "alarms_topic_arn" {
  description = "SNS topic ARN for operational alarms."
  value       = module.messaging.alarms_topic_arn
}

output "ecr_repository_url" {
  description = "ECR repository URL for the platform image (if enabled)."
  value       = var.enable_ecr ? module.compute[0].ecr_repository_url : null
}

output "api_secret_arn" {
  description = "Secrets Manager ARN for API tokens (if enabled)."
  value       = var.enable_secrets ? module.security.api_secret_arn : null
}

output "glue_database" {
  description = "Glue catalog database name (if enabled)."
  value       = var.enable_glue_catalog ? module.catalog[0].database_name : null
}

output "region" {
  value = data.aws_region.current.name
}

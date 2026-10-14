terraform {
  required_version = ">= 1.5.0"
}

variable "name_prefix" { type = string }
variable "kms_key_arn" { type = string }

# Dead-letter queue for events that repeatedly fail processing / validation.
resource "aws_sqs_queue" "dlq" {
  name                       = "${var.name_prefix}-events-dlq"
  message_retention_seconds  = 1209600 # 14 days
  visibility_timeout_seconds = 60
  kms_master_key_id          = var.kms_key_arn
  kms_data_key_reuse_period_seconds = 300
}

# Main processing queue (optional fan-out target for Lambda/Fargate consumers).
resource "aws_sqs_queue" "processing" {
  name                       = "${var.name_prefix}-events-processing"
  message_retention_seconds  = 345600
  visibility_timeout_seconds = 120
  kms_master_key_id          = var.kms_key_arn

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 5
  })
}

# SNS topic for operational alarms -> email/PagerDuty/etc. in real setups.
resource "aws_sns_topic" "alarms" {
  name              = "${var.name_prefix}-alarms"
  kms_master_key_id = var.kms_key_arn
}

output "dlq_queue_url" { value = aws_sqs_queue.dlq.id }
output "dlq_queue_arn" { value = aws_sqs_queue.dlq.arn }
output "processing_queue_url" { value = aws_sqs_queue.processing.id }
output "processing_queue_arn" { value = aws_sqs_queue.processing.arn }
output "alarms_topic_arn" { value = aws_sns_topic.alarms.arn }

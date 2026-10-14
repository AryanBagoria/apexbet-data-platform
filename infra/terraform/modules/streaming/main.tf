terraform {
  required_version = ">= 1.5.0"
}

variable "stream_name" { type = string }
variable "shard_count" { type = number }
variable "retention_hours" { type = number }
variable "kms_key_arn" { type = string }

resource "aws_kinesis_stream" "events" {
  name             = var.stream_name
  shard_count      = var.shard_count
  retention_period = var.retention_hours

  encryption_type = "KMS"
  kms_key_id      = var.kms_key_arn

  shard_level_metrics = [
    "IncomingRecords",
    "OutgoingRecords",
    "WriteProvisionedThroughputExceeded",
    "ReadProvisionedThroughputExceeded",
    "IteratorAgeMilliseconds",
  ]

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    Name = var.stream_name
  }
}

output "stream_name" { value = aws_kinesis_stream.events.name }
output "stream_arn" { value = aws_kinesis_stream.events.arn }

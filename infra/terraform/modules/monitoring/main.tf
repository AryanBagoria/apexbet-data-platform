terraform {
  required_version = ">= 1.5.0"
}

variable "name_prefix" { type = string }
variable "stream_name" { type = string }
variable "alarms_topic_arn" { type = string }
variable "log_retention_days" { type = number }

resource "aws_cloudwatch_log_group" "platform" {
  name              = "/aws/apexbet/${var.name_prefix}"
  retention_in_days = var.log_retention_days
}

# Iterator age alarm: a growing age means consumers have stalled.
resource "aws_cloudwatch_metric_alarm" "iterator_age" {
  alarm_name          = "${var.name_prefix}-kinesis-iterator-age"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  namespace           = "AWS/Kinesis"
  period              = 60
  statistic           = "Maximum"
  threshold           = 60000
  alarm_description   = "Kinesis iterator age exceeds 60s (consumers lagging)."
  alarm_actions       = [var.alarms_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    StreamName = var.stream_name
  }
}

resource "aws_cloudwatch_metric_alarm" "write_throttle" {
  alarm_name          = "${var.name_prefix}-kinesis-write-throttle"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "WriteProvisionedThroughputExceeded"
  namespace           = "AWS/Kinesis"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Kinesis write throttling detected."
  alarm_actions       = [var.alarms_topic_arn]

  dimensions = {
    StreamName = var.stream_name
  }
}

output "log_group_name" { value = aws_cloudwatch_log_group.platform.name }

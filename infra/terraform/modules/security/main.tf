terraform {
  required_version = ">= 1.5.0"
}

variable "name_prefix" { type = string }

# Customer-managed KMS key with rotation for the data lake and streams.
resource "aws_kms_key" "platform" {
  description             = "${var.name_prefix} data platform encryption key"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "platform" {
  name          = "alias/${var.name_prefix}-platform"
  target_key_id = aws_kms_key.platform.key_id
}

# Least-privilege IAM role assumed by the ingestion API / consumers.
data "aws_iam_policy_document" "assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com", "ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "platform" {
  name               = "${var.name_prefix}-platform-role"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

data "aws_iam_policy_document" "platform" {
  statement {
    sid    = "KinesisAccess"
    effect = "Allow"
    actions = [
      "kinesis:PutRecord",
      "kinesis:PutRecords",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:DescribeStream",
      "kinesis:ListShards",
      "kinesis:ListStreams",
    ]
    resources = ["arn:aws:kinesis:*:*:stream/${var.name_prefix}*"]
  }

  statement {
    sid    = "S3Lake"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      "arn:aws:s3:::${var.name_prefix}-lake-*",
      "arn:aws:s3:::${var.name_prefix}-lake-*/*",
    ]
  }

  statement {
    sid    = "QueueAccess"
    effect = "Allow"
    actions = [
      "sqs:SendMessage",
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
    ]
    resources = ["arn:aws:sqs:*:*:${var.name_prefix}-*"]
  }

  statement {
    sid    = "KMSUse"
    effect = "Allow"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:GenerateDataKey",
      "kms:DescribeKey",
    ]
    resources = [aws_kms_key.platform.arn]
  }

  statement {
    sid       = "CloudWatchMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }

  statement {
    sid       = "SecretsRead"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:*:*:secret:${var.name_prefix}*"]
  }
}

resource "aws_iam_policy" "platform" {
  name   = "${var.name_prefix}-platform-policy"
  policy = data.aws_iam_policy_document.platform.json
}

resource "aws_iam_role_policy_attachment" "platform" {
  role       = aws_iam_role.platform.name
  policy_arn = aws_iam_policy.platform.arn
}

# Optional Secrets Manager entry for ingestion API tokens.
resource "aws_secretsmanager_secret" "api_tokens" {
  name                    = "${var.name_prefix}/api-tokens"
  description             = "ApexBet ingestion API bearer tokens (synthetic/dev)."
  kms_key_id              = aws_kms_key.platform.arn
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "api_tokens" {
  secret_id     = aws_secretsmanager_secret.api_tokens.id
  secret_string = jsonencode({ tokens = ["replace-me-in-real-env"] })
}

output "kms_key_arn" { value = aws_kms_key.platform.arn }
output "platform_role_arn" { value = aws_iam_role.platform.arn }
output "api_secret_arn" { value = aws_secretsmanager_secret.api_tokens.arn }

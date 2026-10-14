terraform {
  required_version = ">= 1.5.0"
}

variable "bucket_name" { type = string }
variable "kms_key_arn" { type = string }

resource "aws_s3_bucket" "lake" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "bronze-retention"
    status = "Enabled"
    filter { prefix = "bronze/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    expiration { days = 365 }
    noncurrent_version_expiration { noncurrent_days = 30 }
  }

  rule {
    id     = "silver-gold-retention"
    status = "Enabled"
    filter { prefix = "silver/" }
    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }
    noncurrent_version_expiration { noncurrent_days = 30 }
  }

  rule {
    id     = "abort-incomplete"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

output "bucket_name" { value = aws_s3_bucket.lake.bucket }
output "bucket_arn" { value = aws_s3_bucket.lake.arn }

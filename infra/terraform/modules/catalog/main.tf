terraform {
  required_version = ">= 1.5.0"
}

variable "name_prefix" { type = string }
variable "bucket_name" { type = string }

# Glue catalog database so Athena can query the lake.
resource "aws_glue_catalog_database" "lake" {
  name        = replace("${var.name_prefix}_lake", "-", "_")
  description = "ApexBet synthetic data lake catalog."
}

# Partition projection table over the Bronze prefix (event_type / event_date).
resource "aws_glue_catalog_table" "bronze_events" {
  name          = "bronze_events"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"                 = "parquet"
    "projection.enabled"             = "true"
    "projection.event_type.type"     = "enum"
    "projection.event_type.values"   = "bet_placed,bet_accepted,bet_rejected,bet_settled,odds_updated,market_opened,market_suspended,game_session_started,game_session_ended,deposit_recorded,withdrawal_recorded,account_status_changed,responsible_gambling_limit_changed,risk_signal_generated,pipeline_health_event"
    "projection.event_date.type"     = "date"
    "projection.event_date.format"   = "yyyy-MM-dd"
    "projection.event_date.range"    = "2026-01-01,NOW"
    "projection.event_date.interval" = "1"
    "projection.event_date.interval.unit" = "DAYS"
    "storage.location.template"      = "s3://${var.bucket_name}/bronze/event_type=$${event_type}/event_date=$${event_date}/"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_name}/bronze/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "event_id"
      type = "string"
    }
    columns {
      name = "event_type"
      type = "string"
    }
    columns {
      name = "schema_version"
      type = "string"
    }
    columns {
      name = "event_timestamp"
      type = "string"
    }
    columns {
      name = "original_payload"
      type = "string"
    }
  }

  partition_keys {
    name = "event_type"
    type = "string"
  }
  partition_keys {
    name = "event_date"
    type = "string"
  }
}

resource "aws_athena_workgroup" "lake" {
  name = "${var.name_prefix}-workgroup"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.bucket_name}/athena-results/"
    }
  }
}

output "database_name" { value = aws_glue_catalog_database.lake.name }
output "workgroup" { value = aws_athena_workgroup.lake.name }

module "security" {
  source      = "./modules/security"
  name_prefix = local.name_prefix
}

module "storage" {
  source      = "./modules/storage"
  bucket_name = local.lake_bucket
  kms_key_arn = module.security.kms_key_arn
}

module "streaming" {
  source          = "./modules/streaming"
  stream_name     = local.stream_name
  shard_count     = var.kinesis_shard_count
  retention_hours = var.kinesis_retention_hours
  kms_key_arn     = module.security.kms_key_arn
}

module "messaging" {
  source      = "./modules/messaging"
  name_prefix = local.name_prefix
  kms_key_arn = module.security.kms_key_arn
}

module "monitoring" {
  source            = "./modules/monitoring"
  name_prefix       = local.name_prefix
  stream_name       = module.streaming.stream_name
  alarms_topic_arn  = module.messaging.alarms_topic_arn
  log_retention_days = var.log_retention_days
}

module "compute" {
  source      = "./modules/compute"
  count       = var.enable_ecr ? 1 : 0
  name_prefix = local.name_prefix
}

module "catalog" {
  source      = "./modules/catalog"
  count       = var.enable_glue_catalog ? 1 : 0
  name_prefix = local.name_prefix
  bucket_name = module.storage.bucket_name
}

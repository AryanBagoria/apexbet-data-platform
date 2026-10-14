# ApexBet AWS Deployment (Terraform)

> **Optional. Local development and all tests require no AWS account.** This
> Terraform configuration provisions the AWS-native components of the platform.
> It is validated with `terraform fmt` + `terraform validate` locally and is
> **never applied automatically** by this project.

## What this creates

| Module | Resources | Purpose |
|--------|-----------|---------|
| `security` | KMS key + alias, IAM role/policy (least privilege), Secrets Manager secret | Encryption, permissions, secrets |
| `storage` | S3 lake bucket (versioned, KMS, TLS-only, public-access blocked, lifecycle) | Bronze/Silver/Gold object store |
| `streaming` | Kinesis Data Stream (KMS, shard metrics, retention) | AWS-native event transport |
| `messaging` | SQS DLQ + processing queue (redrive), SNS alarms topic | Dead-letter + alerting |
| `monitoring` | CloudWatch log group, iterator-age + write-throttle alarms | Observability |
| `compute` | ECR repository (scan-on-push, lifecycle) | Container images |
| `catalog` | Glue database + partitioned Bronze table, Athena workgroup | Query the lake with SQL |

## What is NOT created (and why)

- **VPC / ECS Fargate service**: the containers run locally in Docker Compose
  for this project. ECS is a natural next step but adds cost and is not part of
  the locally-validated path, so it is intentionally left out rather than
  half-built.
- **MSK / managed Kafka**: Redpanda runs locally; Kinesis is the AWS-native demo.
- **Lambda**: no function is required; consumers are containerised.

## Prerequisites

- Terraform >= 1.5
- AWS credentials with permission to create the resources above
- (Optional) an S3 + DynamoDB backend, uncommented in `main.tf`

## Commands

```bash
cd infra/terraform
terraform init                 # download providers (no backend required)
terraform fmt -check -recursive
terraform validate
terraform plan  -var-file=terraform.tfvars.example -out=plan.tfplan
# Review plan.tfplan carefully BEFORE applying.
terraform apply plan.tfplan    # only when you intend to deploy
terraform destroy              # tear down (see docs/infrastructure/cleanup.md)
```

## Cost controls

- Small Kinesis shard count, short retention.
- S3 lifecycle transitions Bronze to IA/Glacier.
- 14-day CloudWatch log retention.
- ECR keeps only the last 20 images.
- Tags on all resources for cost attribution (`Project`, `Environment`,
  `CostCenter`).

See `docs/infrastructure/cost-estimation.md` for indicative numbers.

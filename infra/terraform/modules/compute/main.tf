terraform {
  required_version = ">= 1.5.0"
}

variable "name_prefix" { type = string }

# ECR repository for the platform image (API / consumers / jobs).
resource "aws_ecr_repository" "platform" {
  name                 = "${var.name_prefix}/apexbet"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }
}

resource "aws_ecr_lifecycle_policy" "platform" {
  repository = aws_ecr_repository.platform.name
  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 20 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 20
        }
        action = { type = "expire" }
      }
    ]
  })
}

output "ecr_repository_url" { value = aws_ecr_repository.platform.repository_url }
output "ecr_repository_arn" { value = aws_ecr_repository.platform.arn }

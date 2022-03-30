resource "aws_s3_bucket" "cdms-parquet-bucket" {
  bucket = "${local.resource_prefix}-cdms-parquet-bucket"
  acl = "private"
  versioning {
    enabled = true
  }
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }
  policy = jsonencode({
    Version = "2012-10-17",
    Id = "S3PolicyId1",
    Statement = [
      {
        Effect = "Deny",
        Principal = {
          AWS = "*"
        },
        Action = "s3:*",
        Resource = [
          "arn:aws:s3:::${var.ingest_bucket_name}/*",
          "arn:aws:s3:::${var.ingest_bucket_name}"
        ],
        Condition = {
          NotIpAddress = {
            "aws:SourceIp" = var.ip_subnets
          }
        }
      }
    ]
  })
}
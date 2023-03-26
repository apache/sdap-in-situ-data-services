/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
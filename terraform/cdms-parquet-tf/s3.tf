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

data "aws_s3_bucket" "insitu_bucket" {
  bucket = var.insitu_bucket
}

data "aws_s3_bucket" "insitu_bucket_staging" {
  bucket = var.insitu_bucket_staging
}

resource "aws_s3_bucket_notification" "bucket_notification" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_notification
  bucket = data.aws_s3_bucket.insitu_bucket_staging.id
  queue {
    queue_arn     = aws_sqs_queue.in_situ_parquet_sqs.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".json.gz"  // TODO how to enable 2 of them? , .json"
  }
  queue {
    queue_arn     = aws_sqs_queue.in_situ_parquet_sqs.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".json"  // TODO how to enable 2 of them? , .json"
  }
}
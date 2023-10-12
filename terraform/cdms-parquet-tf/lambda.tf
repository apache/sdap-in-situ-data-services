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
resource "aws_lambda_function" "in_situ_parquet_ingestion" {
  filename      = local.lambda_file_name
  source_code_hash = filebase64sha256(local.lambda_file_name)
  function_name = "${var.prefix}-in_situ_parquet_ingestion"
  role          = var.lambda_processing_role_arn
  handler       = "parquet_flask.cdms_lambda_func.ingest_s3_to_cdms.execute_lambda.execute_code"
  runtime       = "python3.8"
  memory_size = 512
  ephemeral_storage {
    size = 1024 # Min 512 MB and the Max 10240 MB
  }
  reserved_concurrent_executions = 2
  timeout       = 900
  environment {
    variables = {
      LOG_LEVEL = var.log_level
      CDMS_BEARER_TOKEN = var.cdms_token
      CDMS_DOMAIN = var.cdms_domain
      PARQUET_META_TBL_NAME = var.metadata_tbl
      SANITIZE_RECORD = var.sanitize_record
      WAIT_TILL_FINISHED = var.wait_till_finished
    }
  }

  vpc_config {
    subnet_ids         = var.insitu_lambda_subnet_ids
    security_group_ids = local.security_group_ids_set ? var.security_group_ids : [aws_security_group.insitu_lambda_sg[0].id]
  }
  tags = var.tags
}
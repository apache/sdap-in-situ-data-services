resource "aws_lambda_function" "ideas_insitu_staging" {
  filename      = local.lambda_file_name
  source_code_hash = filebase64sha256(local.lambda_file_name)
  function_name = "${var.prefix}-ideas_insitu_staging"
  role          = var.lambda_processing_role_arn
  handler       = "parquet_flask.cdms_lambda_func.s3_to_es.execute_lambda.execute_code"
  runtime       = "python3.8"
  reserved_concurrent_executions = 10
  timeout       = 300
  environment {
    variables = {
      LOG_LEVEL = var.log_level
      aws_region = var.aws_region
      es_url = data.aws_elasticsearch_domain.ideas-es.endpoint
      es_port = 443
    }
  }

  vpc_config {
    subnet_ids         = var.ideas_api_lambda_subnet_ids
    security_group_ids = local.security_group_ids_set ? var.security_group_ids : [aws_security_group.unity_cumulus_lambda_sg[0].id]
  }
  tags = var.tags
}

resource "aws_lambda_function" "ideas_insitu_ingestion" {
  filename      = local.lambda_file_name
  function_name = "${var.prefix}-ideas_insitu_ingestion"
  source_code_hash = filebase64sha256(local.lambda_file_name)
  role          = var.lambda_processing_role_arn
  handler       = "parquet_flask.cdms_lambda_func.parquet_ingestion_tracker.execute_lambda.execute_code"
  runtime       = "python3.8"
  reserved_concurrent_executions = 10
  timeout       = 300
  environment {
    variables = {
      LOG_LEVEL = var.log_level
      aws_region = var.aws_region
      es_url = data.aws_elasticsearch_domain.ideas-es.endpoint
      es_port = 443
    }
  }

  vpc_config {
    subnet_ids         = var.ideas_api_lambda_subnet_ids
    security_group_ids = local.security_group_ids_set ? var.security_group_ids : [aws_security_group.unity_cumulus_lambda_sg[0].id]
  }
  tags = var.tags
}

resource "aws_sqs_queue" "ideas_insitu_s3_pipeline" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sqs_queue
  name                      = "${var.prefix}-ideas_insitu_s3_pipeline"
  delay_seconds             = 0
  max_message_size          = 262144
  message_retention_seconds = 345600
  visibility_timeout_seconds = 310
  receive_wait_time_seconds = 0
  policy = templatefile("${path.module}/sqs_policy.json", {
    region: var.aws_region,
    roleArn: var.lambda_processing_role_arn,
    accountId: local.account_id,
    sqsName: "${var.prefix}-ideas_jobs_lis_queue",
  })
//  redrive_policy = jsonencode({
//    deadLetterTargetArn = aws_sqs_queue.terraform_queue_deadletter.arn
//    maxReceiveCount     = 4
//  })
//  tags = {
//    Environment = "production"
//  }
}

resource "aws_sqs_queue" "ideas_insitu_ingestion_completion" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sqs_queue
  name                      = "${var.prefix}-ideas_insitu_ingestion_completion"
  delay_seconds             = 0
  max_message_size          = 262144
  message_retention_seconds = 345600
  visibility_timeout_seconds = 310
  receive_wait_time_seconds = 0
  policy = templatefile("${path.module}/sqs_policy.json", {
    region: var.aws_region,
    roleArn: var.lambda_processing_role_arn,
    accountId: local.account_id,
    sqsName: "${var.prefix}-ideas_jobs_lis_queue",
  })
//  redrive_policy = jsonencode({
//    deadLetterTargetArn = aws_sqs_queue.terraform_queue_deadletter.arn
//    maxReceiveCount     = 4
//  })
//  tags = {
//    Environment = "production"
//  }
}

resource "aws_sns_topic_subscription" "ideas_insitu_s3_pipeline" { // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sns_topic_subscription
  topic_arn = aws_sns_topic.ideas_insitu_s3_pipeline.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.ideas_insitu_s3_pipeline.arn
  filter_policy_scope = "MessageBody"  // MessageAttributes. not using attributes
  filter_policy = templatefile("${path.module}/ideas_insitu_s3_pipeline_filter_policy.json", {})
}

resource "aws_sns_topic_subscription" "ideas_insitu_ingestion_completion" { // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sns_topic_subscription
  topic_arn = aws_sns_topic.ideas_insitu_ingestion_completion.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.ideas_insitu_ingestion_completion.arn
  filter_policy_scope = "MessageBody"  // MessageAttributes. not using attributes
  filter_policy = templatefile("${path.module}/ideas_insitu_ingestion_completion_filter_policy.json", {})
}


resource "aws_lambda_event_source_mapping" "ideas_insitu_s3_pipeline" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/lambda_event_source_mapping#sqs
  event_source_arn = aws_sqs_queue.ideas_insitu_s3_pipeline.arn
  function_name    = aws_lambda_function.ideas_insitu_staging.arn
  batch_size = 1
  enabled = true
}

resource "aws_lambda_event_source_mapping" "ideas_insitu_ingestion_completion" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/lambda_event_source_mapping#sqs
  event_source_arn = aws_sqs_queue.ideas_insitu_ingestion_completion.arn
  function_name    = aws_lambda_function.ideas_insitu_ingestion.arn
  batch_size = 1
  enabled = true
}
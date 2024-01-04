resource "aws_sns_topic" "ideas_insitu_s3_pipeline" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sns_topic.html
  name              = "${var.prefix}-ideas_insitu_s3_pipeline"
#  kms_master_key_id = "alias/aws/sns"
  policy = templatefile("${path.module}/sns_policy.json", {
    region: var.aws_region,
    roleArn: var.lambda_processing_role_arn,
    accountId: local.account_id,
    snsName: "${var.prefix}-ideas_insitu_s3_pipeline",
  })
}

resource "aws_sns_topic" "ideas_insitu_ingestion_completion" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sns_topic.html
  name              = "${var.prefix}-ideas_insitu_ingestion_completion"
#  kms_master_key_id = "alias/aws/sns"
  policy = templatefile("${path.module}/sns_policy.json", {
    region: var.aws_region,
    roleArn: var.lambda_processing_role_arn,
    accountId: local.account_id,
    snsName: "${var.prefix}-ideas_insitu_ingestion_completion",
  })
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = data.aws_s3_bucket.ideas_insitu_staging_bucket.id
  topic {
    topic_arn     = aws_sns_topic.ideas_insitu_s3_pipeline.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".json.gz"
    filter_prefix = var.staging_location_prefix
  }
}
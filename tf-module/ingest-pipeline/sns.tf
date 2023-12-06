resource "aws_sns_topic" "ideas_insitu_s3_pipeline" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sns_topic.html
  name              = "${var.prefix}-ideas_insitu_s3_pipeline"
  kms_master_key_id = "alias/aws/sns"
}


resource "aws_sns_topic" "ideas_insitu_ingestion_completion" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sns_topic.html
  name              = "${var.prefix}-ideas_insitu_ingestion_completion"
  kms_master_key_id = "alias/aws/sns"
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = data.aws_s3_bucket.ideas_insitu_staging_bucket.id
  topic {
    topic_arn     = aws_sns_topic.ideas_insitu_s3_pipeline.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".json*"
    filter_prefix = var.staging_location_prefix
  }
}
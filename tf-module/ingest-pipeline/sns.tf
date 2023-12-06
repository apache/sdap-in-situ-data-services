resource "aws_sns_topic" "ideas_insitu_s3_pipeline" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sns_topic.html
  name              = "${var.prefix}-ideas_insitu_s3_pipeline"
  kms_master_key_id = "alias/aws/sns"
}


resource "aws_sns_topic" "ideas_insitu_ingestion_completion" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sns_topic.html
  name              = "${var.prefix}-ideas_insitu_ingestion_completion"
  kms_master_key_id = "alias/aws/sns"
}
data "aws_s3_bucket" "ideas_insitu_staging_bucket" {
  bucket = var.ideas_insitu_staging_bucket_name
}
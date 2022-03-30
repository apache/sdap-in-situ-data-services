resource "aws_dynamodb_table" "cdms-parquet-metadata-tbl" {
  name           = "${local.resource_prefix}-cdms-parquet-metadata-tbl"
  billing_mode   = "PAY_PER_REQUEST"  // TOOD update to "PROVISIONED" after figuring out read & write capacity
//  read_capacity  = 20
//  write_capacity = 20
  hash_key       = "s3_url"
//  range_key      = "NA"

  attribute {
    name = "UserId"
    type = "S"
  }

//  attribute {
//    name = "NA"
//    type = "S"
//  }

//  ttl {
//    attribute_name = "TimeToExist"
//    enabled        = false
//  }

//  global_secondary_index {
//    name               = "GameTitleIndex"
//    hash_key           = "GameTitle"
//    range_key          = "TopScore"
//    write_capacity     = 10
//    read_capacity      = 10
//    projection_type    = "INCLUDE"
//    non_key_attributes = ["UserId"]
//  }

//  tags = {
//    Name        = "dynamodb-table-1"
//    Environment = "production"
//  }
}
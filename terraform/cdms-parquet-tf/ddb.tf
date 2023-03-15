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
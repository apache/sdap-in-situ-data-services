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

module "cdms-parquet" {
  source = "./cdms-parquet-tf/" # Change this to Github URL once this is public.
  ami = var.ami
  desired_capacity = var.desired_capacity
  environment = var.environment
  ingest_bucket_name = var.ingest_bucket_name
  instance_type = var.instance_type
  ip_subnets = var.ip_subnets
  log_retention = var.log_retention
  max_size = var.max_size
  min_size = var.min_size
  profile = var.profile
  project = var.project
  region = var.region
  shared_credentials_file = var.shared_credentials_file
  subnet_private_1 = var.subnet_private_1
  subnet_private_2 = var.subnet_private_2
  subnet_public_1 = var.subnet_public_1
  subnet_public_2 = var.subnet_public_2
  volume_size = var.volume_size
  vpc = var.vpc
}
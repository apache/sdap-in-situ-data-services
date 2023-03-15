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

variable "region" {}
variable "shared_credentials_file" {}
variable "profile" {}
variable "environment" {}
variable "project" {}
variable "vpc" {}
variable "ami" {}
variable "ingest_bucket_name" {}
variable "subnet_private_1" {}
variable "subnet_private_2" {}
variable "subnet_public_1" {}
variable "subnet_public_2" {}
variable "ip_subnets" {}
variable "instance_type" {}
variable "desired_capacity" {}
variable "min_size" {}
variable "max_size" {}
variable "volume_size" {}
variable "log_retention" {}
#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Replace old_access_key with the current access key that will expire.
# Replace old_secret_key with the current secret key that will expire.
# Then run the script.
# Delete tempfile_please_delete.txt after you have the new credentials.

old_access_key='xxx'
old_secret_key='xxx'
region=us-west-2

# DO NOT MODIFY BELOW THIS LINE
export AWS_ACCESS_KEY_ID="$old_access_key"
export AWS_SECRET_ACCESS_KEY="$old_secret_key"
export AWS_REGION=$region

read -r new_access_key new_secret_key <<<$(/usr/local/bin/aws iam create-access-key --region $AWS_REGION --output text | awk '{print $2 " " $4}');
sleep 5

AWS_ACCESS_KEY_ID="$new_access_key"
AWS_SECRET_ACCESS_KEY="$new_secret_key"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
sleep 10

/usr/local/bin/aws iam delete-access-key --region $AWS_REGION --access-key-id=$old_access_key

echo $new_access_key > tempfile_please_delete.txt
echo $new_secret_key >> tempfile_please_delete.txt
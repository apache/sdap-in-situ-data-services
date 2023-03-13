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

import configparser

cred_file_path = '/Users/wphyo/.aws/credentials'
SAML_GOV = 'saml-gov'
SAML_PUB = 'saml-pub'


def export_as_env(saml_type = 'saml-pub'):
    config = configparser.ConfigParser()
    config.read(cred_file_path)
    saml_cred = config[saml_type]
    return {
        'aws_access_key_id': saml_cred['aws_access_key_id'],
        'aws_secret_access_key': saml_cred['aws_secret_access_key'],
        'aws_session_token': saml_cred['aws_session_token'] if 'aws_session_token' in saml_cred else '',
    }
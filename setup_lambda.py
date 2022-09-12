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

from setuptools import find_packages, setup


install_requires = [
    # 'fastparquet===0.5.0',  # not using it. sticking to pyspark with spark cluster according to Nga
    'jsonschema',  # to verify json objects
    'fastjsonschema===2.15.1',
    'requests===2.26.0',
    'boto3', 'botocore',
    'requests_aws4auth===1.1.1',  # to send aws signed headers in requests
    'elasticsearch===7.13.4',
]

setup(
    name="parquet_ingestion_search",
    version="0.0.1",
    packages=find_packages(),
    install_requires=install_requires,
    author="Apache SDAP",
    author_email="dev@sdap.apache.org",
    python_requires="==3.7",
    license='NONE',
    include_package_data=True,
)

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

FROM bitnami/spark:3.2.0-debian-10-r44

USER root
RUN apt-get update -y && apt-get install vim -y
RUN mkdir /usr/app
WORKDIR /usr/app

COPY setup.py /usr/app
RUN python3 /usr/app/setup.py install
ENV PYTHONPATH="${PYTHONPATH}:/usr/app/"

RUN echo '{"auth_cred":"Mock-CDMS-Flask-Token"}' > /usr/app/cdms_flask_auth.json
ENV authentication_key '/usr/app/cdms_flask_auth.json'
ENV authentication_type 'FILE'

COPY parquet_flask /usr/app/parquet_flask

COPY in_situ_schema.json /usr/app
ENV in_situ_schema=/usr/app/in_situ_schema.json

CMD python3 -m parquet_flask
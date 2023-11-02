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

FROM public.ecr.aws/lambda/python:3.7

# Reference: https://aws.plainenglish.io/spark-on-aws-lambda-c65877c0ac96
#USER root
#RUN apt-get update -y && apt-get install vim -y

RUN yum -y install java-1.8.0-openjdk wget curl
RUN python3 -m pip install pyspark==3.1.2
#ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-amazon-corretto/jre
ENV JAVA_HOME="/usr/lib/jvm/jre-1.8.0-openjdk.x86_64"
ENV PATH=${PATH}:${JAVA_HOME}/bin
ENV SPARK_HOME="/var/lang/lib/python3.7/site-packages/pyspark"
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH
ENV PATH=$SPARK_HOME/python:$PATH
RUN mkdir $SPARK_HOME/conf
RUN echo "SPARK_LOCAL_IP=127.0.0.1" > $SPARK_HOME/conf/spark-env.sh
RUN chmod 777 $SPARK_HOME/conf/spark-env.sh
ARG HADOOP_VERSION=3.3.1
ARG AWS_SDK_VERSION=1.11.901
ARG HADOOP_JAR=https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar
ARG AWS_SDK_JAR=https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar
ADD $HADOOP_JAR  ${SPARK_HOME}/jars/
ADD $AWS_SDK_JAR ${SPARK_HOME}/jars/
COPY etc/lambda-spark/spark-class $SPARK_HOME/bin/spark-class
RUN chmod 777 $SPARK_HOME/bin/spark-class
COPY etc/lambda-spark/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf


RUN mkdir /usr/app
WORKDIR /usr/app

COPY setup_lambda.py /usr/app
RUN python3 /usr/app/setup_lambda.py install
RUN python3 -m pip install pyspark==3.1.2

ENV PYTHONPATH="${PYTHONPATH}:/usr/app/"

COPY parquet_flask /usr/app/parquet_flask

COPY in_situ_schema.json /etc
ENV insitu_schema_file=/etc/in_situ_schema.json

ARG es_url
ENV es_url=$es_url

ARG es_index
ENV es_index=$es_index

CMD ["parquet_flask.cdms_lambda_func.index_to_es.execute_lambda.execute_code"]

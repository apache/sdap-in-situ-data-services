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
import logging
from socket import gethostbyname, gethostname

from pyspark import SparkConf
from pyspark.sql import SparkSession

from parquet_flask.utils.config import Config
from parquet_flask.utils.singleton import Singleton

LOGGER = logging.getLogger(__name__)


class RetrieveSparkSession(metaclass=Singleton):
    def __init__(self):
        self.__sparks = {}
        self.__spark_config = {}

    def set_spark_config(self, input_dict: dict):
        for k, v in input_dict.items():
            self.__spark_config[k] = v
        return self

    def retrieve_spark_session(self, app_name, master_spark, ram='1024m') -> SparkSession:
        session_key = '{}__{}'.format(app_name, master_spark)
        if session_key in self.__sparks:
            return self.__sparks[session_key]
        conf = SparkConf()
        for k, v in self.__spark_config.items():
            conf.set(k, v)
        """
        spark.executor.memory                   3072m
spark.hadoop.fs.s3a.impl                org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3.impl                 org.apache.hadoop.fs.s3.S3FileSystem
spark.hadoop.fs.s3n.impl                org.apache.hadoop.fs.s3native.NativeS3FileSystem
spark.driver.extraClassPath             /usr/bin/spark-3.0.0-bin-hadoop3.2/jars/hadoop-aws-3.2.0.jar:/usr/bin/spark-3.0.0-bin-hadoop3.2/jars/aws-java-sdk-bundle-1.11.563.jar
spark.executor.extraClassPath           /usr/bin/spark-3.0.0-bin-hadoop3.2/jars/hadoop-aws-3.2.0.jar:/usr/bin/spark-3.0.0-bin-hadoop3.2/jars/aws-java-sdk-bundle-1.11.563.jar
spark.executor.extraJavaOptions         -Dcom.amazonaws.services.s3.enableV4=true
spark.driver.extraJavaOptions           -Dcom.amazonaws.services.s3.enableV4=true

spark.eventLog.enabled              true
spark.eventLog.dir                  /tmp/spark-events
spark.eventLog.rolling.enabled      true    
spark.eventLog.rolling.maxFileSize  128m
        """

        # conf.set('spark.eventLog.enabled', 'true')
        # conf.set('spark.eventLog.dir', '/tmp/spark-events')
        # conf.set('spark.eventLog.rolling.enabled', 'true')
        # conf.set('spark.eventLog.rolling.maxFileSize', '128m')
        conf.set('spark.executor.memory', ram)  # something
        conf.set('spark.executor.cores', '1')
        conf.set('spark.driver.port', '50243')
        conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')  # crosscheck the version.
        conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        conf.set('spark.hadoop.fs.s3a.connection.ssl.enabled', 'true')
        # conf.set('spark.driver.extraClassPath', '/opt/bitnami/spark/jars/hadoop-aws-3.2.0.jar:/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.375.jar')
        # conf.set('spark.executor.extraClassPath', '/opt/bitnami/spark/jars/hadoop-aws-3.2.0.jar:/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.375.jar')
        # conf.set('spark.executor.extraJavaOptions', '-Dcom.amazonaws.services.s3.enableV4=true')
        # conf.set('spark.driver.extraJavaOptions', '-Dcom.amazonaws.services.s3.enableV4=true')
        local_ip = gethostbyname(gethostname())
        LOGGER.debug(f'using IP: {local_ip} for spark.driver.host')
        conf.set('spark.driver.host', local_ip)
        conf.set('spark.hadoop.fs.s3a.access.key', Config().get_value('aws_access_key_id'))
        conf.set('spark.hadoop.fs.s3a.secret.key', Config().get_value('aws_secret_access_key'))
        conf.set('spark.hadoop.fs.s3a.session.token', Config().get_value('aws_session_token'))
        # conf.set('spark.default.parallelism', '10')
        # conf.set('spark.hadoop.fs.s3a.endpoint', 's3.us-gov-west-1.amazonaws.com')
        self.__sparks[session_key] = SparkSession.builder.appName(app_name).config(conf=conf).master(master_spark).getOrCreate()
        return self.__sparks[session_key]

    def stop_spark_session(self, app_name, master_spark):
        session_key = '{}__{}'.format(app_name, master_spark)
        if session_key in self.__sparks:
            self.__sparks.pop(session_key).stop()
        return

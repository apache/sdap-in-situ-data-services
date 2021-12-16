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
import json
import logging
from socket import gethostbyname, gethostname

from pyspark import SparkConf
from pyspark.sql import SparkSession

from parquet_flask.io_logic.spark_constants import SparkConstants
from parquet_flask.utils.config import Config
from parquet_flask.utils.singleton import Singleton

LOGGER = logging.getLogger(__name__)


class RetrieveSparkSession(metaclass=Singleton):

    def __init__(self):
        self.__sparks = {}
        self.__spark_config = {
            'spark.executor.cores': '1',  # fixing to 1 core for now
            'spark.driver.port': '50243',  # a random port.
            'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.2.0',  # crosscheck the version.
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            SparkConstants.CRED_PROVIDER_KEY: SparkConstants.SIMPLE_CRED,  # should be overridden
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'true',

            # old configs. no longer needs to be used
            # 'spark.driver.extraJavaOptions': '-Dcom.amazonaws.services.s3.enableV4=true',
            # 'spark.executor.extraJavaOptions': '-Dcom.amazonaws.services.s3.enableV4=true',
            # 'spark.executor.extraClassPath': '/opt/bitnami/spark/jars/hadoop-aws-3.2.0.jar:/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.375.jar',
            # 'spark.driver.extraClassPath': '/opt/bitnami/spark/jars/hadoop-aws-3.2.0.jar:/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.375.jar',
            # settings needed for history server. (still an experiment)
            # 'spark.eventLog.enabled': 'true',
            # 'spark.eventLog.dir': '/tmp/spark-events',
            # 'spark.eventLog.rolling.enabled': 'true',
            # 'spark.eventLog.rolling.maxFileSize': '128m',
        }
        self.__set_spark_config()

    def __set_spark_config(self):
        possible_extra_spark_config = Config().get_value(Config.spark_config_dict, '{}')
        try:
            input_dict = json.loads(possible_extra_spark_config)
            for k, v in input_dict.items():
                self.__spark_config[k] = v
        except:
            LOGGER.exception(f'Not loading extra config. unable to convert to JSON object. {possible_extra_spark_config}.')
        return self

    def __add_aws_cred(self, conf: SparkConf):
        if SparkConstants.CRED_PROVIDER_KEY not in self.__spark_config:  # assume simple
            raise EnvironmentError(f'missing {SparkConstants.CRED_PROVIDER_KEY} in spark_config. This should not happen')
        if self.__spark_config[SparkConstants.CRED_PROVIDER_KEY] == SparkConstants.SIMPLE_CRED:
            conf.set('spark.hadoop.fs.s3a.access.key', Config().get_value(Config.aws_access_key_id, ''))
            conf.set('spark.hadoop.fs.s3a.secret.key', Config().get_value(Config.aws_secret_access_key, ''))
            return
        if self.__spark_config[SparkConstants.CRED_PROVIDER_KEY] == SparkConstants.TEMP_CRED:
            conf.set('spark.hadoop.fs.s3a.access.key', Config().get_value(Config.aws_access_key_id, ''))
            conf.set('spark.hadoop.fs.s3a.secret.key', Config().get_value(Config.aws_secret_access_key, ''))
            conf.set('spark.hadoop.fs.s3a.session.token', Config().get_value(Config.aws_session_token, ''))
            return
        LOGGER.info(f'not setting aws cred ENV values as {SparkConstants.CRED_PROVIDER_KEY} = {self.__spark_config[SparkConstants.CRED_PROVIDER_KEY]}')
        return

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
        """

        conf.set('spark.executor.memory', ram)  # something
        local_ip = gethostbyname(gethostname())
        LOGGER.debug(f'using IP: {local_ip} for spark.driver.host')
        conf.set('spark.driver.host', local_ip)
        self.__add_aws_cred(conf)
        # conf.set('spark.default.parallelism', '10')
        # conf.set('spark.hadoop.fs.s3a.endpoint', 's3.us-gov-west-1.amazonaws.com')
        self.__sparks[session_key] = SparkSession.builder.appName(app_name).config(conf=conf).master(master_spark).getOrCreate()
        return self.__sparks[session_key]

    def stop_spark_session(self, app_name, master_spark):
        session_key = '{}__{}'.format(app_name, master_spark)
        if session_key in self.__sparks:
            self.__sparks.pop(session_key).stop()
        return

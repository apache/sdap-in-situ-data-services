from pyspark import SparkConf
from pyspark.sql import SparkSession

from parquet_flask.utils.config import Config
from parquet_flask.utils.singleton import Singleton


class RetrieveSparkSession(metaclass=Singleton):
    def __init__(self):
        self.__sparks = {}

    def retrieve_spark_session(self, app_name, master_spark, ram='3072m'):
        session_key = '{}__{}'.format(app_name, master_spark)
        # if session_key not in self.__sparks:
        conf = SparkConf()
        """
        spark.executor.memory                   3072m
spark.hadoop.fs.s3a.impl                org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3.impl                 org.apache.hadoop.fs.s3.S3FileSystem
spark.hadoop.fs.s3n.impl                org.apache.hadoop.fs.s3native.NativeS3FileSystem
spark.driver.extraClassPath             /usr/bin/spark-3.0.0-bin-hadoop3.2/jars/hadoop-aws-3.2.0.jar:/usr/bin/spark-3.0.0-bin-hadoop3.2/jars/aws-java-sdk-bundle-1.11.563.jar
spark.executor.extraClassPath           /usr/bin/spark-3.0.0-bin-hadoop3.2/jars/hadoop-aws-3.2.0.jar:/usr/bin/spark-3.0.0-bin-hadoop3.2/jars/aws-java-sdk-bundle-1.11.563.jar
spark.executor.extraJavaOptions         -Dcom.amazonaws.services.s3.enableV4=true
spark.driver.extraJavaOptions           -Dcom.amazonaws.services.s3.enableV4=true
        """
        conf.set('spark.executor.memory', ram)
        # conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        # conf.set('spark.driver.extraClassPath', '/opt/bitnami/spark/jars/hadoop-aws-3.2.0.jar:/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.375.jar')
        # conf.set('spark.executor.extraClassPath', '/opt/bitnami/spark/jars/hadoop-aws-3.2.0.jar:/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.375.jar')
        # conf.set('spark.executor.extraJavaOptions', '-Dcom.amazonaws.services.s3.enableV4=true')
        # conf.set('spark.driver.extraJavaOptions', '-Dcom.amazonaws.services.s3.enableV4=true')
        conf.set('spark.hadoop.fs.s3a.access.key', Config().get_value('aws_access_key_id'))
        conf.set('spark.hadoop.fs.s3a.secret.key', Config().get_value('aws_secret_access_key'))
        conf.set('spark.hadoop.fs.s3a.session.token', Config().get_value('aws_session_token'))
        conf.set('spark.hadoop.fs.s3a.connection.ssl.enabled', 'true')
        # conf.set('spark.default.parallelism', '10')
        # conf.set('spark.hadoop.fs.s3a.endpoint', 's3.us-gov-west-1.amazonaws.com')
        return SparkSession.builder.appName(app_name).config(conf=conf).master(master_spark).getOrCreate()
        # self.__sparks[session_key] = SparkSession.builder.appName(app_name).config(conf=conf).master(master_spark).getOrCreate()
        # return self.__sparks[session_key]

    def stop_spark_session(self, app_name, master_spark):
        session_key = '{}__{}'.format(app_name, master_spark)
        if session_key in self.__sparks:
            self.__sparks.pop(session_key).stop()
        return

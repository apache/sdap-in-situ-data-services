import findspark
from pyspark.sql import SparkSession

from parquet_flask.utils.singleton import Singleton


class RetrieveSparkSession(metaclass=Singleton):
    def __init__(self):
        # findspark.init()
        self.__sparks = {}

    def retrieve_spark_session(self, app_name, master_spark, ram='512m'):
        session_key = '{}__{}'.format(app_name, master_spark)
        if session_key not in self.__sparks:
            # findspark.init()
            self.__sparks[session_key] = SparkSession.builder.appName(app_name).config('spark.executor.memory', ram).getOrCreate()
            # self.__sparks[session_key] = SparkSession.builder.appName(app_name).master(master_spark).config('spark.executor.memory', ram).getOrCreate()
        return self.__sparks[session_key]

    def stop_spark_session(self, app_name, master_spark):
        session_key = '{}__{}'.format(app_name, master_spark)
        if session_key in self.__sparks:
            self.__sparks.pop(session_key).stop()
        return

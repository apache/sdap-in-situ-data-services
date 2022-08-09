from pyspark.sql import SparkSession

from parquet_flask.utils.singleton import Singleton


class LocalSparkSession(metaclass=Singleton):
    def __init__(self):
        self.__spark_session = None

    def get_spark_session(self) -> SparkSession:
        if self.__spark_session is None:
            self.__spark_session = SparkSession.builder \
                .master("local") \
                .appName('TestAppName') \
                .getOrCreate()
        return self.__spark_session

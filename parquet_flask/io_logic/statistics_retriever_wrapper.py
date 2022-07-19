import logging

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException

from parquet_flask.io_logic.cdms_schema import CdmsSchema
from parquet_flask.io_logic.statistics_retriever import StatisticsRetriever
from parquet_flask.utils.config import Config

LOGGER = logging.getLogger(__name__)


class StatisticsRetrieverWrapper:
    def __init__(self):
        config = Config()
        self.__app_name = config.get_value('spark_app_name')
        self.__master_spark = config.get_value('master_spark_url')
        self.__parquet_name = config.get_value('parquet_file_name')
        self.__parquet_name = self.__parquet_name if not self.__parquet_name.endswith('/') else self.__parquet_name[:-1]

    def start(self, parquet_path):
        from parquet_flask.io_logic.retrieve_spark_session import RetrieveSparkSession
        spark: SparkSession = RetrieveSparkSession().retrieve_spark_session(self.__app_name, self.__master_spark)
        full_parquet_path = f"{self.__parquet_name}/{parquet_path}"
        LOGGER.debug(f'searching for full_parquet_path: {full_parquet_path}')
        try:
            read_df: DataFrame = spark.read.schema(CdmsSchema.ALL_SCHEMA).parquet(full_parquet_path)
        except AnalysisException as analysis_exception:
            if analysis_exception.desc is not None and analysis_exception.desc.startswith('Path does not exist'):
                LOGGER.debug(f'no such full_parquet_path: {full_parquet_path}')
                return None
            LOGGER.exception(f'error while retrieving full_parquet_path: {full_parquet_path}')
            raise analysis_exception
        stats = StatisticsRetriever(read_df).start()
        return stats.to_json()

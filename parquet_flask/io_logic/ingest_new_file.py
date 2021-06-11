from parquet_flask.io_logic.retrieve_spark_session import RetrieveSparkSession
from parquet_flask.io_logic.sanitize_record import SanitizeRecord
from parquet_flask.utils.config import Config
from parquet_flask.utils.file_utils import FileUtils

from pyspark.sql.functions import to_timestamp, year, month


class IngestNewJsonFile:
    def __init__(self):
        self.__sss = RetrieveSparkSession()
        config = Config()
        self.__app_name = config.get_value('spark_app_name')
        self.__master_spark = config.get_value('master_spark_url')
        self.__mode = 'overwrite'
        self.__parquet_name = config.get_value('parquet_file_name')

    def ingest(self, abs_file_path, time_col=None, partitions=[]):
        if not FileUtils.file_exist(abs_file_path):
            raise ValueError('missing file to ingest it. path: {}'.format(abs_file_path))
        input_json = SanitizeRecord(Config().get_value('in_situ_schema')).start(abs_file_path)
        df = self.__sss.retrieve_spark_session(self.__app_name, self.__master_spark).createDataFrame(input_json)
        time_partitions = []
        if time_col is not None:
            df = df.withColumn('time_obj', to_timestamp(time_col))\
                .withColumn('year', year(time_col)).withColumn('month', month(time_col))
            time_partitions = ['year', 'month']
        df_writer = df.write
        all_partitions = partitions + time_partitions
        if len(all_partitions) > 0:
            df_writer = df_writer.partitionBy(partitions)
        df_writer.mode(self.__mode).parquet(self.__parquet_name, compression='GZIP')
        return

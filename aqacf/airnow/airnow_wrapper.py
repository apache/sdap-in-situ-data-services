import logging
import os
import tempfile

from aqacf.airnow.download_raw_data import DownloadRawData
from aqacf.airnow.merge_data import MergeData
from aqacf.airnow.parquet_json_formatter import ParquetJsonFormatter
from parquet_flask.aws.aws_s3 import AwsS3
from parquet_flask.utils.file_utils import FileUtils
LOGGER = logging.getLogger(__name__)


class AirNowWrapper:
    def __init__(self, provider, project, bucket, chunk_size=10):
        self.__provider = provider
        self.__project = project
        self.__bucket = bucket
        self.__s3 = AwsS3()
        self.__split_size = chunk_size

    def execute_month(self, start_date, end_date):
        start_month = start_date[:-3]
        LOGGER.debug(f'processing: {start_month}')

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir_name = f'/aqacf/air_now/f{start_month}'
            # DownloadRawData(tmp_dir_name).download_data(start_date, end_date)
            # LOGGER.debug(f'downloaded data for: {start_month}')

            concat_dir = os.path.join(tmp_dir_name, 'concat')
            MergeData(tmp_dir_name, concat_dir).start()
            LOGGER.debug(f'merged data for: {start_month}')

            ParquetJsonFormatter(self.__provider, self.__project).start(os.path.join(concat_dir, 'daily.csv'))
            LOGGER.debug(f'converted daily data for: {start_month}')
            daily_json_file = os.path.join(concat_dir, f'{start_month}_daily.json.gz')
            daily_json_file = FileUtils.gzip_file_unix_os(os.path.join(concat_dir, 'daily.csv_0.json'), output_file_path=daily_json_file, overwrite=True)
            LOGGER.debug(f'zipped daily data for: {daily_json_file}')
            self.__s3.upload(daily_json_file, self.__bucket, f'{self.__provider}/daily', True)
            LOGGER.debug(f'uploaded daily data for: {daily_json_file}')

            ParquetJsonFormatter(self.__provider, f'{self.__project}_raw').start(os.path.join(concat_dir, 'raw.csv'), self.__split_size)
            LOGGER.debug(f'converted raw data for: {start_month}')
            for i in range(self.__split_size):
                raw_json_file = os.path.join(concat_dir, f'{start_month}__{i}_raw.json.gz')
                raw_json_file = FileUtils.gzip_file_unix_os(os.path.join(concat_dir, f'raw.csv_{i}.json'), output_file_path=raw_json_file, overwrite=True)
                LOGGER.debug(f'zipped data for: {raw_json_file}')
                self.__s3.upload(raw_json_file, self.__bucket, f'{self.__provider}/raw', True)
                LOGGER.debug(f'uploaded data for: {raw_json_file}')
        return

    def start(self, year, start_month=1):
        for i in range(start_month, 12):
            start_date = f'{year}-{i:02d}-01'
            end_date = f'{year}-{i+1:02d}-01'
            self.execute_month(start_date, end_date)
        start_date = f'{year}-12-01'
        end_date = f'{year+1}-01-01'
        self.execute_month(start_date, end_date)
        return

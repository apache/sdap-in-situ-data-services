import json
import logging
import os

import requests

from parquet_flask.cdms_lambda_func.cdms_lambda_constants import CdmsLambdaConstants

LOGGER = logging.getLogger(__name__)


class ParquetStatExtractor:
    def __init__(self):
        self.__cdms_url = os.environ.get(CdmsLambdaConstants.cdms_url, None)
        self.__parquet_base_folder = os.environ.get(CdmsLambdaConstants.parquet_base_folder, 'None')
        self.__verify_ssl = os.environ.get(CdmsLambdaConstants.verify_ssl, 'true').strip().upper() == 'TRUE'
        if any([k is None for k in [self.__cdms_url, self.__parquet_base_folder]]):
            raise ValueError(f'invalid env. must have {[CdmsLambdaConstants.cdms_url, CdmsLambdaConstants.parquet_base_folder]}')

    def __get_parquet_s3_path(self, s3_key: str):
        parquet_s3_path = s3_key.replace(self.__parquet_base_folder, '')
        if parquet_s3_path.startswith('/'):
            parquet_s3_path = parquet_s3_path[1:]
        LOGGER.debug(f'parquet_s3_path: {parquet_s3_path}')
        return parquet_s3_path

    def start(self, s3_key: str):
        stats_url = f'{self.__cdms_url}?s3_key={self.__get_parquet_s3_path(s3_key)}'
        LOGGER.debug(f'stats_url: {stats_url}')
        response = requests.get(url=stats_url, verify=self.__verify_ssl)
        if response.status_code > 400:
            raise ValueError(f'wrong status code: {response.status_code}. details: {response.text}')
        return json.loads(response.text)

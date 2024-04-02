import json
import logging

import pandas as pd
import requests

from parquet_flask.io_logic.query_insitu_data.query_insitu_abstract import QueryInsituAbstract
from parquet_flask.io_logic.query_v2 import QueryProps
from parquet_flask.utils.general_utils import GeneralUtils

LOGGER = logging.getLogger(__name__)
# https://github.com/stccenter/PurpleAir-GMU-FireAlarm/blob/main/README.md

class QueryGmuInsituData(QueryInsituAbstract):
    def __init__(self, query_props: QueryProps, base_url: str) -> None:
        super().__init__(query_props)
        self.__ssl_verify = False
        self.__gmu_base_url = base_url
        self.__gmu_base_url = self.__gmu_base_url if self.__gmu_base_url.endswith('/') else f'{self.__gmu_base_url}/'

    def __get_platforms(self):
        if self._query_props.platform_id is not None and len(self._query_props.platform_id) > 0:
            return
        raise NotImplementedError('TODO')
        return self

    def __get_one_page(self):
        result = []
        get_data_url = f'{self.__gmu_base_url}activities?sensor_ids={",".join(self._query_props.platform_id)}&sd={self._query_props.min_datetime}&ed={self._query_props.max_datetime}&resolution_type=hourly'
        LOGGER.debug(f'loading data for {get_data_url}')
        insitu_data = requests.get(get_data_url, verify=self.__ssl_verify)
        insitu_data.raise_for_status()
        insitu_data = json.loads(insitu_data.content.decode('utf-8'))
        df = pd.DataFrame(insitu_data['observations'])
        # Add the 'providers' column
        df['time'] = pd.to_datetime(df['time'])
        result_df = df.groupby(['platform_id', df['time'].dt.date]).mean().reset_index()
        result_df['time'] = result_df['time'].astype('datetime64').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        result_df['platform'] = result_df['platform_id'].apply(lambda x: {"id": x, "short_name": ''})
        result_df['provider'] = insitu_data['provider']
        result_df['project'] = insitu_data['project']
        result_df.drop('platform_id', axis=1, inplace=True)
        result.extend(result_df.to_dict(orient='records'))
        return result

    def search(self, spark_session=None):
        # if self._query_props.platform_id is None:
        # if no platform ID, search platforms
        # if platforms, call them directly
        self.__get_platforms()
        result = self.__get_one_page()
        return {
            'total': len(result),
            'results': result,
        }
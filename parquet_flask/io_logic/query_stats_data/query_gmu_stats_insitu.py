import json
import logging

import pandas as pd
import requests

from parquet_flask.io_logic.query_daily_data.insitu_query_props import InsituQueryProps
from parquet_flask.io_logic.query_stats_data.query_stats_data_abstract import QueryStatsDataAbsract
from parquet_flask.io_logic.query_v2 import QueryProps
from parquet_flask.utils.general_utils import GeneralUtils

LOGGER = logging.getLogger(__name__)


class QueryGmuStatsData(QueryStatsDataAbsract):
    def __init__(self, query_props: QueryProps, base_url: str):
        super().__init__(query_props)
        self._query_props = query_props
        self.__gmu_base_url = base_url
        self.__gmu_base_url = self.__gmu_base_url if self.__gmu_base_url.endswith('/') else f'{self.__gmu_base_url}/'
        self.__ssl_verify = False

    def list_collections(self):
        raise NotImplementedError(f'not necessary to call this method')

    def start(self):
        # https://insitu-api.stcenter.net/statistics?provider=PurpleAir-GMU-Raw&startTime=2022-07-01T00:00:00Z&endTime=2022-07-02T00:00:00Z
        query_params = {
            'provider': self._query_props.provider,
            'startTime': self._query_props.min_datetime,
            'endTime': self._query_props.max_datetime,
        }
        query_params = [f'{k}={v}' for k, v in query_params.items()]
        get_platforms_url = f'{self.__gmu_base_url}statistics?{"&".join(query_params)}'
        LOGGER.debug(f'loading stats for {get_platforms_url}')
        statistics = requests.get(get_platforms_url, verify=self.__ssl_verify)
        statistics.raise_for_status()
        return statistics.json()

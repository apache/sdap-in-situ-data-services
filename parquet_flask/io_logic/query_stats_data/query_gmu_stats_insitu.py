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
    def __init__(self, query_props: QueryProps):
        super().__init__(query_props)
        self._query_props = query_props

    def list_collections(self):
        raise NotImplementedError(f'not necessary to call this method')

    def start(self):
        return {
            'hits': [],
            'marker': [],
        }

from abc import ABC, abstractmethod

from parquet_flask.io_logic.query_daily_data.insitu_query_props import InsituQueryProps
from parquet_flask.io_logic.query_v2 import QueryProps


class QueryStatsDataAbsract(ABC):
    def __init__(self, query_props: QueryProps):
        self._query_props = query_props

    @abstractmethod
    def list_collections(self):
        return []

    @abstractmethod
    def start(self):
        return {
            'hits': [],
            'marker': [],
        }

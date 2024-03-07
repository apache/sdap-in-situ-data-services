from abc import ABC, abstractmethod

from parquet_flask.io_logic.query_daily_data.insitu_query_props import InsituQueryProps


class QueryDailyDataAbsract(ABC):

    @abstractmethod
    def query(self, query_props: InsituQueryProps):
        return {
            'hits': [],
            'marker': [],
        }

import json
import logging

import pandas as pd
import requests

from parquet_flask.io_logic.query_daily_data.insitu_query_props import InsituQueryProps
from parquet_flask.io_logic.query_daily_data.query_daily_data_abstract import QueryDailyDataAbsract
from parquet_flask.utils.general_utils import GeneralUtils

LOGGER = logging.getLogger(__name__)


class GmuInsitu(QueryDailyDataAbsract):
    def __init__(self):
        self.__platform_ids = []
        self.__query_date = None
        self.__gmu_base_url = 'https://insitu-api.stcenter.net'  # TODO
        self.__gmu_base_url = self.__gmu_base_url if self.__gmu_base_url.endswith('/') else f'{self.__gmu_base_url}/'
        self.__ssl_verify = False
        self.__query_props = InsituQueryProps()
        self.__gmu_page_size = 500

    def load_platform_ids(self):
        if self.__query_date is None:
            raise ValueError(f'pls set __query_date before calling this method')
        query_params = {
            'date': self.__query_date,
        }
        if len(self.__query_props.min_lat_lon) > 0:
            query_params['min_lon'] = self.__query_props.min_lat_lon[1]
            query_params['min_lat'] = self.__query_props.min_lat_lon[0]
        if len(self.__query_props.max_lat_lon) > 0:
            query_params['max_lon'] = self.__query_props.max_lat_lon[1]
            query_params['max_lat'] = self.__query_props.max_lat_lon[0]
        if len(self.__query_props.variable) > 0:
            query_params['variable'] = ','.join(self.__query_props.variable)
        query_params = [f'{k}={v}' for k, v in query_params.items()]
        self.__platform_ids = []
        get_platforms_url = f'{self.__gmu_base_url}sensor_data?{"&".join(query_params)}'
        LOGGER.debug(f'loading platforms for {get_platforms_url}')
        platforms = requests.get(get_platforms_url, verify=self.__ssl_verify)
        platforms.raise_for_status()
        self.__platform_ids = sorted([k['platform_id'] for k in platforms.json()])
        # FileUtils.write_json(f'gmu_platforms_{self.__query_date}.json', self.__platform_ids, overwrite=True, prettify=True)
        return self

    def __get_one_page(self, platform_id_chunk):
        result = []
        for each_chunk in GeneralUtils.chunk_list(platform_id_chunk, self.__gmu_page_size):
            platforms = ','.join([str(k) for k in each_chunk])
            get_data_url = f'{self.__gmu_base_url}activities?sensor_ids={platforms}&sd={self.__query_date}&resolution_type=hourly'
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

    def query(self, query_props: InsituQueryProps):
        if any([k is None for k in [query_props.timestamp, query_props.project, query_props.provider]]):
            raise ValueError(f'missing timestamp or project or provider')
        self.__query_props = query_props
        self.__query_date = query_props.timestamp[0:10]  # TODO this is assuming timestamp is in correct format
        self.load_platform_ids()

        start_index = int(query_props.marker[0]) if len(query_props.marker) > 0 else 0
        end_index = start_index + query_props.size
        platform_id_chunk = self.__platform_ids[start_index: end_index]
        result = self.__get_one_page(platform_id_chunk)
        return {
            'hits': result,
            'marker': [str(end_index)],
        }

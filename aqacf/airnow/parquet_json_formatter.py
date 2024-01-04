import json
import logging
import multiprocessing

from parquet_flask.utils.general_utils import GeneralUtils

multiprocessing.set_start_method("fork")

import os
from copy import deepcopy

import numpy as np
import pandas as pd

from parquet_flask.utils.file_utils import FileUtils

LOGGER = logging.getLogger(__name__)


def process_dict(data_dict_input, in_str_form=False):
    data_dict = deepcopy(data_dict_input)
    removing_keys = [k for k, v in data_dict.items() if not isinstance(v, str) and np.isnan(v)]
    data_dict['platform'] = {
        'id': data_dict['id'],
        'short_name': data_dict['short_name'] if 'short_name' in data_dict else '',
    }
    removing_keys += ['id', 'short_name']
    for k in removing_keys:
        if k in data_dict:
            data_dict.pop(k)
    if in_str_form:
        return json.dumps(data_dict)
    return data_dict

class ParquetJsonFormatter:
    def __init__(self, provider_name: str, project_name):
        self.__provider_name = provider_name
        self.__project_name = project_name

    def start(self, csv_file: str, split_size=1, platform_appender=''):
        LOGGER.debug(f'processing: {csv_file}')
        airnow_data = pd.read_csv(csv_file, sep=',', encoding='latin1')
        airnow_data['site_id'] = airnow_data['site_id'].astype(str) + platform_appender

        LOGGER.debug(f'read: {csv_file}')
        """
        {
          "time": "2023-01-01T04:00:00Z",
          "latitude": 34.1439,
          "longitude": -117.8508,
          "o3": 19.0,
          "co": 0.1,
          "no": 0.0,
          "no2": 2.2,
          "pm2_5": 0.0,
          "platform": {
            "id": "060370016",
            "short_name": "Glendora - Laurel"
          }
      site_id,time,CO,NO2,NO,SO2,PM2.5,PM10,OZONE,site_name,lat,lon
        """
        renaming_column_dict = {
            'site_id': 'id',
            'site_name': 'short_name',
            'lat': 'latitude',
            'lon': 'longitude',
            'OZONE': 'o3',
            'CO': 'co',
            'NO': 'no',
            'NO2': 'no2',
            'SO2': 'so2',
            'SO3': 'so3',
            'PM2.5': 'pm2_5',
            'PM10': 'pm10',
        }
        airnow_data.rename(columns=renaming_column_dict, inplace=True)
        # airnow_data.fillna(None, inplace=True)
        LOGGER.debug(f'renamed: {csv_file}')

        row_size = int(airnow_data.shape[0] / split_size)
        LOGGER.debug(f'to raw_json row_size: {row_size}')
        for i in range(split_size):
            start_index = row_size * i
            end_index = row_size * (i+1) if (i+1) < split_size else airnow_data.shape[0]
            LOGGER.debug(f'to processing batch: {i}: {start_index}:{end_index}')
            current_airnow_data = airnow_data.iloc[start_index:end_index]

            json_list = current_airnow_data.to_dict(orient='records')
            # LOGGER.debug(f'to raw_json: {csv_file}_{i}')

            # result_list = [process_dict(k) for k in json_list]
            # mysterious error in parallel processing
            # pool = multiprocessing.Pool()
            #
            # # Use the Pool to apply the process_dict function to each dictionary in parallel
            # result_list = pool.map(process_dict, json_list)
            # # Close the Pool
            # pool.close()
            # pool.join()
            LOGGER.debug(f'to parquet_json: {csv_file}_{i}')
            with open(f'{csv_file}_{i}.json', 'w') as ff:
                ff.write('{\n')
                ff.write(f'"project": "{self.__project_name}",\n')
                ff.write(f'"provider": "{self.__provider_name}",\n')
                ff.write(f'"observations": [\n')
                splitter_comma = ''
                for each_chunk in GeneralUtils.chunk_list(json_list, 10**4 * 5):
                    ff.write(splitter_comma)
                    str_chunk = [process_dict(k, True) for k in each_chunk]
                    ff.write(','.join(str_chunk))
                    splitter_comma = ','
                ff.write(']}\n')
            # FileUtils.write_json(f'{csv_file}_{i}.json', site_json, overwrite=True, prettify=True)
            LOGGER.debug(f'written to file: {csv_file}_{i}')
        return

# logging.basicConfig(level=10, format="%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s")
# ParquetJsonFormatter('AirNow', 'air_quality').start('/private/tmp/debugging/concat/daily.csv')
# ParquetJsonFormatter('AirNow', 'air_quality').start('/private/tmp/debugging/concat/raw.csv', 3)

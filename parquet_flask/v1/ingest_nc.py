import logging

import xarray as xr
import numpy as np
import pandas as pd
import json
import os

from parquet_flask.io_logic.ingest_new_file import IngestNewJsonFile

LOGGER = logging.getLogger(__name__)
class NcInsituProcessor:
    def __init__(self):
        self.__file_path = '/Users/wphyo/Downloads/Qout_23_20201101_20210301_VIC.nc'
        self.__platform_id_key = 'rivid'
        self.__observation_key = 'Qout'
        self.__lat_key, self.__lon_key = 'lat', 'lon'
        self.__time_key = 'time'
        self.__chunk_size = 1200
        self.__is_overwriting = False
        self.__job_id = 'TODO'
        self.__provider = 'TODO'
        self.__project = 'TODO'

    def h5_chunk_generator(self, dset, chunk_size):
        """
        Looping dataset and yielding each chunk

        :param dset: HDF5 Dataset object
        :param chunk_size: how many rows in one chunk
        :return:
        """
        total_size = len(dset)
        current_start = 0
        current_end = chunk_size
        while current_start < total_size:
            if current_end > total_size:
                current_end = total_size
            yield dset[current_start: current_end]
            current_start = current_end
            current_end += chunk_size
        return

    def start(self):
        with xr.open_dataset(self.__file_path) as x_ds:
            platforms = x_ds[self.__platform_id_key].values
            LOGGER.debug(f'total platforms: {len(platforms)}')
            for i, each_chunk in enumerate(self.h5_chunk_generator(platforms, self.__chunk_size)):
                LOGGER.debug(f'processing chunk {i}')
                slice_ds = x_ds.sel(rivid=each_chunk)
                df = slice_ds[self.__observation_key].to_dataframe().sort_index(level=self.__platform_id_key)
                df['platform'] = df.apply(lambda row: {'id': str(row.name[1]), 'short_name': f'River reach {str(row.name[1])}'}, axis=1)
                df = df.reset_index(level=[self.__time_key, self.__platform_id_key]).set_index(self.__time_key)
                df['timestep'] = df.index.strftime('%Y-%m-%dT%H:%M:%SZ')
                df = df.reset_index()
                df = df.drop([self.__time_key, self.__platform_id_key], axis=1)
                df = df.rename({'timestep': 'time', self.__lon_key: 'longitude', self.__lat_key: 'latitude'}, axis=1)
                df = df[['time', self.__observation_key, 'longitude', 'latitude', 'platform']]
                LOGGER.debug(f'ingesting dataframe')
                IngestNewJsonFile(self.__is_overwriting).ingest_df(df, self.__job_id, self.__provider, self.__project)
        return

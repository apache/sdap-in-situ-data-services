import logging
import os
from datetime import datetime
from glob import glob
import pandas as pd

from parquet_flask.utils.file_utils import FileUtils
LOGGER = logging.getLogger(__name__)


class MergeData:
    def __init__(self, raw_data_dir: str, merged_data_dir: str):
        self.__raw_data_dir = raw_data_dir
        self.__merged_data_dir = merged_data_dir

    def start(self):
        LOGGER.debug(f'processing {self.__raw_data_dir}')
        cs_files = glob(os.path.join(self.__raw_data_dir, '*.csv'))
        data_frames = [pd.read_csv(each_file, sep=',', encoding='latin1') for each_file in cs_files]
        concat_data_frame = pd.concat(data_frames, axis=0)
        concat_data_frame.sort_values(by=['site_id', 'time'], ascending=[True, True], inplace=True)
        LOGGER.debug(f'concatenated {self.__raw_data_dir}')
        FileUtils.mk_dir_p(self.__merged_data_dir)
        concat_data_frame.to_csv(os.path.join(self.__merged_data_dir, 'raw.csv'), index=False)
        LOGGER.debug(f'written raw data {self.__raw_data_dir}')
        site_names = concat_data_frame[['site_id', 'site_name']].drop_duplicates(subset=['site_id'])
        concat_data_frame['time'] = pd.to_datetime(concat_data_frame['time'])
        concat_data_frame.drop(columns=['site_name'], inplace=True)
        data_columns = set(concat_data_frame.columns.tolist())
        data_columns.remove('site_id')
        data_columns.remove('time')
        LOGGER.debug(f'prepped to aggregate {self.__raw_data_dir}')
        result_df = concat_data_frame.groupby(['site_id', concat_data_frame['time'].dt.date])[list(data_columns)].mean().reset_index()
        result_df['time'] = result_df['time'].astype('datetime64').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        result_df = result_df.merge(site_names, on='site_id', how='left')
        LOGGER.debug(f'aggregated {self.__raw_data_dir}')
        result_df['site_name'] = result_df['site_name'].fillna('')
        result_df.to_csv(os.path.join(self.__merged_data_dir, 'daily.csv'), index=False)
        LOGGER.debug(f'written aggregated data {self.__raw_data_dir}')
        return

# time1 = datetime.now()
# # MergeData('/tmp/airnow3', '/tmp/airnow3/concat').start()
# time2 = datetime.now()
# print(time2 - time1)

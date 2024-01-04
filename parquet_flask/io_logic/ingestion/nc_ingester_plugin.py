# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import xarray as xr
import logging
from parquet_flask.io_logic.ingestion.aws_file_ingester_plugin_abstract import AwsFileIngesterPluginAbstract
from parquet_flask.io_logic.ingest_new_file import IngestNewJsonFile
from parquet_flask.utils.file_utils import FileUtils
from parquet_flask.utils.time_utils import TimeUtils
LOGGER = logging.getLogger(__name__)


class NcIngesterPlugin(AwsFileIngesterPluginAbstract):
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

    def _execute_ingest_data(self):
        try:
            LOGGER.debug(f'ingesting file: {self._saved_file_name}')
            start_time = TimeUtils.get_current_time_unix()

            with xr.open_dataset(self._saved_file_name) as x_ds:
                platforms = x_ds[self._props.platform_id_key].values
                LOGGER.debug(f'total platforms: {len(platforms)}')
                for i, each_chunk in enumerate(self.h5_chunk_generator(platforms, self._props.chunk_size)):
                    LOGGER.debug(f'processing chunk {i}')
                    slice_ds = x_ds.sel(rivid=each_chunk)
                    df = slice_ds[self._props.observation_key].to_dataframe().sort_index(
                        level=self._props.platform_id_key)
                    df['platform'] = df.apply(
                        lambda row: {'id': str(row.name[1]), 'short_name': f'River reach {str(row.name[1])}'}, axis=1)
                    df = df.reset_index(level=[self._props.time_key, self._props.platform_id_key]).set_index(
                        self._props.time_key)
                    df['timestep'] = df.index.strftime('%Y-%m-%dT%H:%M:%SZ')
                    df = df.reset_index()
                    df = df.drop([self._props.time_key, self._props.platform_id_key], axis=1)
                    df = df.rename(
                        {'timestep': 'time', self._props.lon_key: 'longitude', self._props.lat_key: 'latitude'},
                        axis=1)
                    df = df[['time', self._props.observation_key, 'longitude', 'latitude', 'platform']]
                    LOGGER.debug(f'ingesting dataframe')
                    num_records = IngestNewJsonFile(self._props.is_replacing).ingest_df(df, self._props.uuid,
                                                                                         self._props.provider,
                                                                                         self._props.project)
            end_time = TimeUtils.get_current_time_unix()
            LOGGER.debug(f'uploading to metadata table')
            self._generate_db_record(start_time, end_time, num_records)
            LOGGER.debug(f'deleting used file')
            FileUtils.del_file(self._saved_file_name)
            LOGGER.warning('Disabled tagging S3 due to IAM issues')
            # LOGGER.debug(f'tagging s3')
            # s3.add_tags_to_obj({
            #     'parquet_ingested': TimeUtils.get_time_str(self.__ingested_date),
            #     'job_id': self.__props.uuid,
            # })
        except Exception as e:
            LOGGER.exception(f'deleting error file')
            FileUtils.del_file(self._saved_file_name)
            raise e
        if self._sha512_result is True:
            return {'message': 'ingested', 'job_id': self._props.uuid}, 201
        return {'message': 'ingested, different sha512', 'cause': self._sha512_cause, 'job_id': self._props.uuid}, 203

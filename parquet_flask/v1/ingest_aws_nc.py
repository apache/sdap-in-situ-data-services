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
import os
from multiprocessing.context import Process

from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.aws.es_factory import ESFactory
from parquet_flask.io_logic.metadata_tbl_es import MetadataTblES

from parquet_flask.aws.aws_s3 import AwsS3
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.ingest_new_file import IngestNewJsonFile
from parquet_flask.io_logic.metadata_tbl_interface import MetadataTblInterface
from parquet_flask.utils.config import Config
from parquet_flask.utils.file_utils import FileUtils
from parquet_flask.utils.time_utils import TimeUtils
from parquet_flask.v1.ingest_aws_json import IngestAwsJsonProps

LOGGER = logging.getLogger(__name__)


class IngestAwsNc:
    def __init__(self, props=IngestAwsJsonProps()):
        self.__props = props
        self.__saved_file_name = None
        self.__ingested_date = TimeUtils.get_current_time_unix()
        self.__file_sha512 = None
        self.__sha512_result = None
        self.__sha512_cause = None
        config = Config()
        es_url = config.get_value(Config.es_url)
        es_port = int(config.get_value(Config.es_port, '443'))
        self.__es: ESAbstract = ESFactory().get_instance('AWS', index='', base_url=es_url, port=es_port)
        self.__db_io: MetadataTblInterface = MetadataTblES(self.__es)

    def __get_s3_sha512(self):
        """
        sha512 file is in this format
        <sha-512><space or tab><s3 json filename>
        :return:
        """
        if self.__props.s3_sha_url is None:
            LOGGER.warning(f's3_sha_url is None. using s3_url to generate one')
            self.__props.s3_sha_url = f'{self.__props.s3_url}.sha512'
        s3 = AwsS3().set_s3_url(self.__props.s3_sha_url)
        try:
            s3.get_s3_obj_size()
            sha512_content = s3.read_small_txt_file()
            return sha512_content.replace(os.path.basename(self.__props.s3_url), '').strip()
        except:
            LOGGER.exception(f'cannot find s3_sha_url')
            return None

    def __compare_sha512(self, s3_sha512):
        if s3_sha512 is None:
            self.__sha512_result = False
            self.__sha512_cause = 'missing S3 sha512'
            return
        if s3_sha512 == self.__file_sha512:
            self.__sha512_result = True
            self.__sha512_cause = ''
            return
        self.__sha512_result = False
        self.__sha512_cause = f'mismatched sha512: {s3_sha512} vs {self.__file_sha512}'
        return

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

    def __execute_ingest_data(self):
        try:
            LOGGER.debug(f'ingesting file: {self.__saved_file_name}')
            start_time = TimeUtils.get_current_time_unix()

            with xr.open_dataset(self.__saved_file_name) as x_ds:
                platforms = x_ds[self.__props.platform_id_key].values
                LOGGER.debug(f'total platforms: {len(platforms)}')
                for i, each_chunk in enumerate(self.h5_chunk_generator(platforms, self.__props.chunk_size)):
                    LOGGER.debug(f'processing chunk {i}')
                    slice_ds = x_ds.sel(rivid=each_chunk)
                    df = slice_ds[self.__props.observation_key].to_dataframe().sort_index(level=self.__props.platform_id_key)
                    df['platform'] = df.apply(lambda row: {'id': str(row.name[1]), 'short_name': f'River reach {str(row.name[1])}'}, axis=1)
                    df = df.reset_index(level=[self.__props.time_key, self.__props.platform_id_key]).set_index(self.__props.time_key)
                    df['timestep'] = df.index.strftime('%Y-%m-%dT%H:%M:%SZ')
                    df = df.reset_index()
                    df = df.drop([self.__props.time_key, self.__props.platform_id_key], axis=1)
                    df = df.rename({'timestep': 'time', self.__props.lon_key: 'longitude', self.__props.lat_key: 'latitude'},
                                   axis=1)
                    df = df[['time', self.__props.observation_key, 'longitude', 'latitude', 'platform']]
                    LOGGER.debug(f'ingesting dataframe')
                    num_records = IngestNewJsonFile(self.__props.is_replacing).ingest_df(df, self.__props.uuid, self.__props.provider, self.__props.project)
            end_time = TimeUtils.get_current_time_unix()
            LOGGER.debug(f'uploading to metadata table')
            new_record = {
                CDMSConstants.s3_url_key: self.__props.s3_url,
                CDMSConstants.uuid_key: self.__props.uuid,
                CDMSConstants.ingested_date_key: self.__ingested_date,
                CDMSConstants.file_size_key: FileUtils.get_size(self.__saved_file_name),
                CDMSConstants.checksum_key: self.__file_sha512,
                CDMSConstants.checksum_validation: self.__sha512_result,
                CDMSConstants.checksum_cause: self.__sha512_cause,
                CDMSConstants.job_start_key: start_time,
                CDMSConstants.job_end_key: end_time,
                CDMSConstants.records_count_key: num_records,
            }
            if self.__props.is_replacing:
                self.__db_io.replace_record(new_record)
            else:
                self.__db_io.insert_record(new_record)
            LOGGER.debug(f'deleting used file')
            FileUtils.del_file(self.__saved_file_name)
            # TODO make it background process?
            LOGGER.warning('Disabled tagging S3 due to IAM issues')
            # LOGGER.debug(f'tagging s3')
            # s3.add_tags_to_obj({
            #     'parquet_ingested': TimeUtils.get_time_str(self.__ingested_date),
            #     'job_id': self.__props.uuid,
            # })
        except Exception as e:
            LOGGER.exception(f'deleting error file')
            FileUtils.del_file(self.__saved_file_name)
            return {'message': 'failed to ingest to parquet', 'details': str(e)}, 500
        if self.__sha512_result is True:
            return {'message': 'ingested', 'job_id': self.__props.uuid}, 201
        return {'message': 'ingested, different sha512', 'cause': self.__sha512_cause,
                'job_id': self.__props.uuid}, 203

    def ingest(self):
        """
        - download s3 file
        - unzip if needed
        - ingest to parquet
        - update to metadata tbl
        - delete local file
        - tag s3 object

        :return: tuple - (json object, return code)
        """
        try:
            LOGGER.debug(f'starting to ingest: {self.__props.s3_url}')
            existing_record = self.__db_io.get_by_s3_url(self.__props.s3_url)
            if existing_record is None and self.__props.is_replacing is True:
                LOGGER.error(f'unable to replace file as it is new. {self.__props.s3_url}')
                return {'message': 'unable to replace file as it is new'}, 500

            if existing_record is not None and self.__props.is_replacing is False:
                LOGGER.error(f'unable to ingest file as it is already ingested. {self.__props.s3_url}. ingested record: {existing_record}')
                return {'message': 'unable to ingest file as it is already ingested'}, 500

            s3 = AwsS3().set_s3_url(self.__props.s3_url)
            LOGGER.debug(f'downloading s3 file: {self.__props.uuid}')
            FileUtils.mk_dir_p(self.__props.working_dir)
            self.__saved_file_name = s3.download(self.__props.working_dir)
            self.__file_sha512 = FileUtils.get_checksum(self.__saved_file_name)
            if self.__saved_file_name.lower().endswith('.gz'):
                LOGGER.debug(f's3 file is in gzipped form. unzipping. {self.__saved_file_name}')
                self.__saved_file_name = FileUtils.gunzip_file_os(self.__saved_file_name)
            self.__compare_sha512(self.__get_s3_sha512())
            if self.__props.wait_till_complete is True:
                return self.__execute_ingest_data()
            else:
                bg_process = Process(target=self.__execute_ingest_data, args=())
                bg_process.daemon = True
                bg_process.start()
                return {'message': 'ingesting. Not waiting.', 'job_id': self.__props.uuid}, 204
        except Exception as e:
            LOGGER.exception(f'deleting error file')
            FileUtils.del_file(self.__saved_file_name)
            return {'message': 'failed to ingest to parquet', 'details': str(e)}, 500

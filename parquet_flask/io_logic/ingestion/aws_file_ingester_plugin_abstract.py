import logging
import os
from multiprocessing import Process

from parquet_flask.aws.aws_s3 import AwsS3
from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.aws.es_factory import ESFactory
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.ingestion.ingest_plugin_abstract import IngestPluginAbstract
from parquet_flask.io_logic.ingestion.ingest_props import IngestProps
from parquet_flask.io_logic.metadata_tbl_es import MetadataTblES
from parquet_flask.io_logic.metadata_tbl_interface import MetadataTblInterface
from parquet_flask.utils.file_utils import FileUtils
from parquet_flask.utils.time_utils import TimeUtils
LOGGER = logging.getLogger(__name__)


class AwsFileIngesterPluginAbstract(IngestPluginAbstract):
    def __init__(self, props: IngestProps) -> None:
        super().__init__(props)
        self._saved_file_name = None
        self._ingested_date = TimeUtils.get_current_time_unix()
        self._file_sha512 = None
        self._sha512_result = None
        self._sha512_cause = None
        self._es: ESAbstract = ESFactory().get_instance('AWS', index='', base_url=self._props.es_url, port=self._props.es_port)
        self._db_io: MetadataTblInterface = MetadataTblES(self._es)

    def _get_s3_sha512(self):
        """
        sha512 file is in this format
        <sha-512><space or tab><s3 json filename>
        :return:
        """
        if self._props.s3_sha_url is None:
            LOGGER.warning(f's3_sha_url is None. using s3_url to generate one')
            self._props.s3_sha_url = f'{self._props.s3_url}.sha512'
        s3 = AwsS3().set_s3_url(self._props.s3_sha_url)
        try:
            sha512_content = s3.read_small_txt_file()
            return sha512_content.replace(os.path.basename(self._props.s3_url), '').strip()
        except:
            LOGGER.exception(f'cannot find s3_sha_url')
            return None

    def _compare_sha512(self, s3_sha512):
        if s3_sha512 is None:
            self._sha512_result = False
            self._sha512_cause = 'missing S3 sha512'
            return
        if s3_sha512 == self._file_sha512:
            self._sha512_result = True
            self._sha512_cause = ''
            return
        self._sha512_result = False
        self._sha512_cause = f'mismatched sha512: {s3_sha512} vs {self._file_sha512}'
        return

    def _generate_db_record(self, start_time, end_time, num_records):
        self._props.generated_record = {
            CDMSConstants.s3_url_key: self._props.s3_url,
            CDMSConstants.uuid_key: self._props.uuid,
            CDMSConstants.ingested_date_key: self._ingested_date,
            CDMSConstants.file_size_key: FileUtils.get_size(self._saved_file_name),
            CDMSConstants.checksum_key: self._file_sha512,
            CDMSConstants.checksum_validation: self._sha512_result,
            CDMSConstants.checksum_cause: self._sha512_cause,
            CDMSConstants.job_start_key: start_time,
            CDMSConstants.job_end_key: end_time,
            CDMSConstants.records_count_key: num_records,
        }
        return self
    def _execute_ingest_data(self):
        raise NotImplemented('required concrete implementation')

    def ingest(self):
        LOGGER.debug(f'starting to ingest: {self._props.s3_url}')
        existing_record = self._db_io.get_by_s3_url(self._props.s3_url)
        if existing_record is None and self._props.is_replacing is True:
            LOGGER.error(f'unable to replace file as it is new. {self._props.s3_url}')
            raise RuntimeError('unable to replace file as it is new')

        if existing_record is not None and self._props.is_replacing is False:
            LOGGER.error(f'unable to ingest file as it is already ingested. {self._props.s3_url}. ingested record: {existing_record}')
            raise RuntimeError('unable to ingest file as it is already ingested')

        try:
            s3 = AwsS3().set_s3_url(self._props.s3_url)
            LOGGER.debug(f'downloading s3 file: {self._props.uuid}')
            FileUtils.mk_dir_p(self._props.working_dir)
            self._saved_file_name = s3.download(self._props.working_dir)
            self._file_sha512 = FileUtils.get_checksum(self._saved_file_name)
            if self._saved_file_name.lower().endswith('.gz'):
                LOGGER.debug(f's3 file is in gzipped form. unzipping. {self._saved_file_name}')
                self._saved_file_name = FileUtils.gunzip_file_os(self._saved_file_name)
            self._compare_sha512(self._get_s3_sha512())
            if self._props.wait_till_complete is True:
                return self._execute_ingest_data()
            else:
                bg_process = Process(target=self._execute_ingest_data, args=())
                bg_process.daemon = True
                bg_process.start()
                return {'message': 'ingesting. Not waiting.', 'job_id': self._props.uuid}, 204  # TODO
        except Exception as e:
            LOGGER.exception(f'deleting error file')
            FileUtils.del_file(self._saved_file_name)
            raise e

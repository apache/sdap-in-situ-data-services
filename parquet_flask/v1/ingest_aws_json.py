import logging
import uuid

from parquet_flask.aws.aws_s3 import AwsS3
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.ingest_new_file import IngestNewJsonFile
from parquet_flask.io_logic.metadata_tbl_io import MetadataTblIO
from parquet_flask.utils.file_utils import FileUtils
from parquet_flask.utils.time_utils import TimeUtils

LOGGER = logging.getLogger(__name__)


class IngestAwsJsonProps:
    def __init__(self):
        self.__s3_url = None
        self.__uuid = str(uuid.uuid4())
        self.__working_dir = f'/tmp/{str(uuid.uuid4())}'
        self.__is_replacing = False

    @property
    def is_replacing(self):
        return self.__is_replacing

    @is_replacing.setter
    def is_replacing(self, val):
        """
        :param val:
        :return: None
        """
        self.__is_replacing = val
        return

    @property
    def working_dir(self):
        return self.__working_dir

    @working_dir.setter
    def working_dir(self, val):
        """
        :param val:
        :return: None
        """
        self.__working_dir = val
        return

    @property
    def s3_url(self):
        return self.__s3_url

    @s3_url.setter
    def s3_url(self, val):
        """
        :param val:
        :return: None
        """
        self.__s3_url = val
        return

    @property
    def uuid(self):
        return self.__uuid

    @uuid.setter
    def uuid(self, val):
        """
        :param val:
        :return: None
        """
        self.__uuid = val
        return


class IngestAwsJson:
    def __init__(self, props=IngestAwsJsonProps()):
        self.__props = props
        self.__saved_file_name = None
        self.__ingested_date = TimeUtils.get_current_time_unix()
        self.__db_io = MetadataTblIO()

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
            if self.__saved_file_name.lower().endswith('.gz'):
                LOGGER.debug(f's3 file is in gzipped form. unzipping. {self.__saved_file_name}')
                self.__saved_file_name = FileUtils.gunzip_file_os(self.__saved_file_name)
            LOGGER.debug(f'ingesting file: {self.__saved_file_name}')
            start_time = TimeUtils.get_current_time_unix()
            num_records = IngestNewJsonFile().ingest(self.__saved_file_name, self.__props.uuid)
            end_time = TimeUtils.get_current_time_unix()
            LOGGER.debug(f'uploading to metadata table')
            new_record = {
                CDMSConstants.s3_url_key: self.__props.s3_url,
                CDMSConstants.uuid_key: self.__props.uuid,
                CDMSConstants.ingested_date_key: self.__ingested_date,
                CDMSConstants.file_size_key: FileUtils.get_size(self.__saved_file_name),
                CDMSConstants.checksum_key: FileUtils.get_checksum(self.__saved_file_name),
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
            LOGGER.debug(f'tagging s3')
            s3.add_tags_to_obj({
                'parquet_ingested': TimeUtils.get_time_str(self.__ingested_date),
                'job_id': self.__props.uuid,
            })
            return {'message': 'ingested'}, 201
        except Exception as e:
            LOGGER.debug(f'deleting error file')
            FileUtils.del_file(self.__saved_file_name)
            return {'message': 'failed to ingest to parquet', 'details': str(e)}, 500

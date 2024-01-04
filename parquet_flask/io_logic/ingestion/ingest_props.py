import uuid


class IngestProps:
    def __init__(self):
        self.__s3_url = None
        self.__s3_sha_url = None
        self.__uuid = str(uuid.uuid4())
        self.__working_dir = f'/tmp/{str(uuid.uuid4())}'
        self.__is_replacing = False
        self.__is_sanitizing = True
        self.__wait_till_complete = True
        self.__platform_id_key = 'rivid'
        self.__observation_key = 'Qout'
        self.__lat_key = 'lat'
        self.__lon_key = 'lon'
        self.__time_key = 'time'
        self.__chunk_size = 1200
        self.__provider = ''
        self.__project = ''
        self.__es_url = None
        self.__es_port = 443
        self.__pub_sub_topic = None
        self.__generated_record = None

        self.__result_json = {}
        self.__result_status_code = 500

    @property
    def result_json(self):
        return self.__result_json

    @result_json.setter
    def result_json(self, val):
        """
        :param val:
        :return: None
        """
        self.__result_json = val
        return

    @property
    def result_status_code(self):
        return self.__result_status_code

    @result_status_code.setter
    def result_status_code(self, val):
        """
        :param val:
        :return: None
        """
        self.__result_status_code = val
        return

    @property
    def generated_record(self):
        return self.__generated_record

    @generated_record.setter
    def generated_record(self, val):
        """
        :param val:
        :return: None
        """
        self.__generated_record = val
        return
    @property
    def es_url(self):
        return self.__es_url

    @es_url.setter
    def es_url(self, val):
        """
        :param val:
        :return: None
        """
        self.__es_url = val
        return

    @property
    def es_port(self):
        return self.__es_port

    @es_port.setter
    def es_port(self, val):
        """
        :param val:
        :return: None
        """
        self.__es_port = val
        return

    @property
    def pub_sub_topic(self):
        return self.__pub_sub_topic

    @pub_sub_topic.setter
    def pub_sub_topic(self, val):
        """
        :param val:
        :return: None
        """
        self.__pub_sub_topic = val
        return

    @property
    def platform_id_key(self):
        return self.__platform_id_key

    @platform_id_key.setter
    def platform_id_key(self, val):
        """
        :param val:
        :return: None
        """
        self.__platform_id_key = val
        return

    @property
    def observation_key(self):
        return self.__observation_key

    @observation_key.setter
    def observation_key(self, val):
        """
        :param val:
        :return: None
        """
        self.__observation_key = val
        return

    @property
    def lat_key(self):
        return self.__lat_key

    @lat_key.setter
    def lat_key(self, val):
        """
        :param val:
        :return: None
        """
        self.__lat_key = val
        return

    @property
    def lon_key(self):
        return self.__lon_key

    @lon_key.setter
    def lon_key(self, val):
        """
        :param val:
        :return: None
        """
        self.__lon_key = val
        return

    @property
    def time_key(self):
        return self.__time_key

    @time_key.setter
    def time_key(self, val):
        """
        :param val:
        :return: None
        """
        self.__time_key = val
        return

    @property
    def chunk_size(self):
        return self.__chunk_size

    @chunk_size.setter
    def chunk_size(self, val):
        """
        :param val:
        :return: None
        """
        self.__chunk_size = val
        return

    @property
    def provider(self):
        return self.__provider

    @provider.setter
    def provider(self, val):
        """
        :param val:
        :return: None
        """
        self.__provider = val
        return

    @property
    def project(self):
        return self.__project

    @project.setter
    def project(self, val):
        """
        :param val:
        :return: None
        """
        self.__project = val
        return

    @property
    def wait_till_complete(self):
        return self.__wait_till_complete

    @wait_till_complete.setter
    def wait_till_complete(self, val):
        """
        :param val:
        :return: None
        """
        self.__wait_till_complete = val
        return

    @property
    def is_sanitizing(self):
        return self.__is_sanitizing

    @is_sanitizing.setter
    def is_sanitizing(self, val):
        """
        :param val:
        :return: None
        """
        self.__is_sanitizing = val
        return

    @property
    def s3_sha_url(self):
        return self.__s3_sha_url

    @s3_sha_url.setter
    def s3_sha_url(self, val):
        """
        :param val:
        :return: None
        """
        self.__s3_sha_url = val
        return

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

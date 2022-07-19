from parquet_flask.io_logic.cdms_constants import CDMSConstants


class S3StatExtractor:
    def __init__(self, s3_url: str):
        self.__s3_url = s3_url
        self.__provider = None
        self.__project = None
        self.__platform_code = None
        self.__geo_interval = None
        self.__year = None
        self.__month = None
        self.__job_id = None
        self.__name = None
        self.__bucket = None

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
    def bucket(self):
        return self.__bucket

    @bucket.setter
    def bucket(self, val):
        """
        :param val:
        :return: None
        """
        self.__bucket = val
        return

    @property
    def job_id(self):
        return self.__job_id

    @job_id.setter
    def job_id(self, val):
        """
        :param val:
        :return: None
        """
        self.__job_id = val
        return

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, val):
        """
        :param val:
        :return: None
        """
        self.__name = val
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
    def platform_code(self):
        return self.__platform_code

    @platform_code.setter
    def platform_code(self, val):
        """
        :param val:
        :return: None
        """
        self.__platform_code = val
        return

    @property
    def geo_interval(self):
        return self.__geo_interval

    @geo_interval.setter
    def geo_interval(self, val):
        """
        :param val:
        :return: None
        """
        self.__geo_interval = val
        return

    @property
    def year(self):
        return self.__year

    @year.setter
    def year(self, val):
        """
        :param val:
        :return: None
        """
        self.__year = val
        return

    @property
    def month(self):
        return self.__month

    @month.setter
    def month(self, val):
        """
        :param val:
        :return: None
        """
        self.__month = val
        return

    def to_json(self):
        out_dict = {
            's3_url': self.s3_url,
        }
        if self.bucket is not None:
            out_dict['bucket'] = self.bucket
        if self.name is not None:
            out_dict['name'] = self.name
        if self.provider is not None:
            out_dict[CDMSConstants.provider_col] = self.provider
        if self.project is not None:
            out_dict[CDMSConstants.project_col] = self.project
        if self.platform_code is not None:
            out_dict[CDMSConstants.platform_code_col] = self.platform_code
        if self.geo_interval is not None:
            out_dict[CDMSConstants.geo_spatial_interval_col] = self.geo_interval
        if self.year is not None:
            out_dict[CDMSConstants.year_col] = self.year
        if self.month is not None:
            out_dict[CDMSConstants.month_col] = self.month
        return out_dict

    def start(self):
        split_s3_url = self.__s3_url.split('://')
        if len(split_s3_url) != 2:
            raise ValueError(f'invalid S3 URL: {self.__s3_url}')
        split_s3_path = split_s3_url[1].strip().split('/')
        if len(split_s3_path) < 2:
            raise ValueError(f'invalid s3 path: {split_s3_url[1]}')
        self.bucket = split_s3_path[0]
        self.name = split_s3_path[-1]

        partition_dict = [k.split('=') for k in split_s3_path[1: -1] if '=' in k]
        partition_dict = {k[0]: k[1] for k in partition_dict}

        if CDMSConstants.provider_col in partition_dict:
            self.provider = partition_dict[CDMSConstants.provider_col]

        if CDMSConstants.project_col in partition_dict:
            self.project = partition_dict[CDMSConstants.project_col]

        if CDMSConstants.platform_code_col in partition_dict:
            self.platform_code = partition_dict[CDMSConstants.platform_code_col]

        if CDMSConstants.geo_spatial_interval_col in partition_dict:
            self.geo_interval = partition_dict[CDMSConstants.geo_spatial_interval_col]

        if CDMSConstants.year_col in partition_dict:
            self.year = partition_dict[CDMSConstants.year_col]

        if CDMSConstants.month_col in partition_dict:
            self.month = partition_dict[CDMSConstants.month_col]

        if CDMSConstants.job_id_col in partition_dict:
            self.job_id = partition_dict[CDMSConstants.job_id_col]
        return self

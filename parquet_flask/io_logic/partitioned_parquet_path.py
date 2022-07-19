from copy import copy

from parquet_flask.io_logic.cdms_constants import CDMSConstants


class PartitionedParquetPath:
    def __init__(self, base_name: str):
        self.__base_name = base_name
        self.__provider = None
        self.__project = None
        self.__platform = None
        self.__year = None
        self.__month = None
        self.__lat_lon = None

    def set_lat_lon(self, val):
        self.lat_lon = val
        return self

    def set_provider(self, val):
        self.provider = val
        return self

    def set_project(self, val):
        self.project = val
        return self

    def set_platform(self, val):
        self.platform = val
        return self

    def set_year(self, val):
        self.year = val
        return self

    def set_month(self, val):
        self.month = val
        return self

    def load_from_es(self, es_result: dict):
        if CDMSConstants.provider_col in es_result:
            self.set_provider(es_result[CDMSConstants.provider_col])
        if CDMSConstants.project_col in es_result:
            self.set_project(es_result[CDMSConstants.project_col])
        if CDMSConstants.platform_code_col in es_result:
            self.set_platform(es_result[CDMSConstants.platform_code_col])
        if CDMSConstants.year_col in es_result:
            self.set_year(es_result[CDMSConstants.year_col])
        if CDMSConstants.month_col in es_result:
            self.set_month(es_result[CDMSConstants.month_col])
        if CDMSConstants.geo_spatial_interval_col in es_result:
            self.set_lat_lon(es_result[CDMSConstants.geo_spatial_interval_col])
        return self

    def duplicate(self):
        return copy(self)

    def get_df_columns(self) -> dict:
        column_set = {}
        if self.provider is not None:
            column_set[CDMSConstants.provider_col] = self.provider
        if self.project is not None:
            column_set[CDMSConstants.project_col] = self.project
        if self.platform is not None:
            column_set[CDMSConstants.platform_code_col] = self.platform
        return column_set

    @property
    def lat_lon(self):
        return self.__lat_lon

    @lat_lon.setter
    def lat_lon(self, val):
        """
        :param val:
        :return: None
        """
        self.__lat_lon = val
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
    def platform(self):
        return self.__platform

    @platform.setter
    def platform(self, val):
        """
        :param val:
        :return: None
        """
        self.__platform = val
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

    def __format_lat_lon(self):
        if self.lat_lon is None:
            raise ValueError('failed to format lat_long. Value is NULL')
        if isinstance(self.lat_lon, str):
            return self.lat_lon
        if isinstance(self.lat_lon, tuple) or isinstance(self.lat_lon, list):
            return f'{self.lat_lon[0]}_{self.lat_lon[1]}'
        raise TypeError(f'unknown lat_lon type: {type(self.lat_lon)}. value: {self.lat_lon}')

    def __str__(self) -> str:
        return self.generate_path()

    def generate_path(self):
        parquet_path = self.__base_name
        if self.provider is None:
            return parquet_path
        parquet_path = f'{parquet_path}/{CDMSConstants.provider_col}={self.provider}'
        if self.project is None:
            return parquet_path
        parquet_path = f'{parquet_path}/{CDMSConstants.project_col}={self.project}'
        if self.platform is None:
            return parquet_path
        parquet_path = f'{parquet_path}/{CDMSConstants.platform_code_col}={self.platform}'
        if self.lat_lon is None:
            return parquet_path
        parquet_path = f'{parquet_path}/{CDMSConstants.geo_spatial_interval_col}={self.__format_lat_lon()}'
        if self.year is None:
            return parquet_path
        parquet_path = f'{parquet_path}/{CDMSConstants.year_col}={self.year}'
        if self.month is None:
            return parquet_path
        parquet_path = f'{parquet_path}/{CDMSConstants.month_col}={self.month}'
        return parquet_path

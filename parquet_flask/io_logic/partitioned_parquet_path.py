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
        if self.year is None:
            return parquet_path
        parquet_path = f'{parquet_path}/{CDMSConstants.year_col}={self.year}'
        if self.month is None:
            return parquet_path
        parquet_path = f'{parquet_path}/{CDMSConstants.month_col}={self.month}'
        return parquet_path

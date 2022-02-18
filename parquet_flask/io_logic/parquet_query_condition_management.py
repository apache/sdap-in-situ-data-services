import logging

from parquet_flask.utils.time_utils import TimeUtils
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.query_v2 import QueryProps

LOGGER = logging.getLogger(__name__)


class ParquetQueryConditionManagement:
    def __init__(self, parquet_name, missing_depth_value, props=QueryProps()):
        self.__conditions = []
        self.__parquet_name = parquet_name if not parquet_name.endswith('/') else parquet_name[:-1]
        self.__columns = [CDMSConstants.time_col, CDMSConstants.depth_col, CDMSConstants.lat_col, CDMSConstants.lon_col]
        self.__is_extending_base = True
        self.__query_props = props
        self.__missing_depth_value = missing_depth_value

    @property
    def conditions(self):
        return self.__conditions

    @conditions.setter
    def conditions(self, val):
        """
        :param val:
        :return: None
        """
        self.__conditions = val
        return

    @property
    def parquet_name(self):
        return self.__parquet_name

    @parquet_name.setter
    def parquet_name(self, val):
        """
        :param val:
        :return: None
        """
        self.__parquet_name = val
        return

    @property
    def columns(self):
        return self.__columns

    @columns.setter
    def columns(self, val):
        """
        :param val:
        :return: None
        """
        self.__columns = val
        return

    def __check_provider(self):
        if self.__query_props.provider is None:
            self.__is_extending_base = False
            self.__columns.append(CDMSConstants.provider_col)
            return
        LOGGER.debug(f'setting provider condition: {self.__query_props.provider}')
        self.__parquet_name = f'{self.__parquet_name}/{CDMSConstants.provider_col}={self.__query_props.provider}'
        return

    def __check_project(self):
        if self.__query_props.project is None:
            self.__is_extending_base = False
            self.__columns.append(CDMSConstants.project_col)
            return
        if not self.__is_extending_base:
            LOGGER.debug(f'setting project condition as sql: {self.__query_props.project}')
            self.__columns.append(CDMSConstants.project_col)
            self.__conditions.append(f"{CDMSConstants.project_col} == '{self.__query_props.project}'")
            return
        LOGGER.debug(f'setting project condition as path: {self.__query_props.project}')
        self.__parquet_name = f'{self.__parquet_name}/{CDMSConstants.project_col}={self.__query_props.project}'
        return

    def __check_platform(self):
        if self.__query_props.platform_code is None:
            self.__is_extending_base = False
            # self.__columns.append(CDMSConstants.platform_code_col)  # platform_code has separate nested column.
            return
        if not self.__is_extending_base:
            LOGGER.debug(f'setting platform_code condition as sql: {self.__query_props.platform_code}')
            # self.__columns.append(CDMSConstants.platform_code_col)
            self.__conditions.append(f"{CDMSConstants.platform_code_col} == '{self.__query_props.platform_code}'")
            return
        LOGGER.debug(f'setting platform_code condition as path: {self.__query_props.platform_code}')
        self.__parquet_name = f'{self.__parquet_name}/{CDMSConstants.platform_code_col}={self.__query_props.platform_code}'
        return

    def __check_time_range(self):
        if self.__query_props.min_datetime is None and self.__query_props.max_datetime is None:
            self.__is_extending_base = False
            return None
        min_year = max_year = None
        if self.__query_props.min_datetime is not None:
            LOGGER.debug(f'setting datetime min condition as sql: {self.__query_props.min_datetime}')
            min_year = TimeUtils.get_datetime_obj(self.__query_props.min_datetime).year
            # conditions.append(f"{CDMSConstants.year_col} >= {min_year}")
            self.__conditions.append(f"{CDMSConstants.time_obj_col} >= '{self.__query_props.min_datetime}'")
        if self.__query_props.max_datetime is not None:
            LOGGER.debug(f'setting datetime max condition as sql: {self.__query_props.max_datetime}')
            max_year = TimeUtils.get_datetime_obj(self.__query_props.max_datetime).year
            # conditions.append(f"{CDMSConstants.year_col} <= {max_year}")
            self.__conditions.append(f"{CDMSConstants.time_obj_col} <= '{self.__query_props.max_datetime}'")
        if self.__is_extending_base is False:
            self.__is_extending_base = False
            # TODO add year and month to the conditions, but not sure it will have any effect
            return
        if min_year is None or max_year is None:
            self.__is_extending_base = False
            # TODO add year and month to the query conditions. But not sure it will have any effect
            return
        if min_year != max_year:
            # LOGGER.debug(f'setting year range condition as path: {min_year}')
            # year_set = '{' + ','.join([str(k) for k in range(min_year, max_year + 1)]) + '}'
            # self.__parquet_name = f'{self.__parquet_name}/{CDMSConstants.year_col}={year_set}'
            self.__is_extending_base = False
            # TODO add (year and) month to the query conditions. But not sure it will have any effect
            return
        LOGGER.debug(f'setting year condition as path: {min_year}')
        self.__parquet_name = f'{self.__parquet_name}/{CDMSConstants.year_col}={min_year}'
        # since both min_year and max_year are not None, getting the month
        min_month = TimeUtils.get_datetime_obj(self.__query_props.min_datetime).month
        max_month = TimeUtils.get_datetime_obj(self.__query_props.max_datetime).month
        if min_month != max_month:
            # LOGGER.debug(f'setting month range condition as path: {min_year}')
            # month_set = '{' + ','.join([str(k) for k in range(min_month, max_month + 1)]) + '}'
            # self.__parquet_name = f'{self.__parquet_name}/{CDMSConstants.month_col}={month_set}'
            # TODO add month to the query conditions. But not sure it will have any effect
            self.__is_extending_base = False
            return
        LOGGER.debug(f'setting month condition as path: {min_year}')
        self.__parquet_name = f'{self.__parquet_name}/{CDMSConstants.month_col}={min_month}'
        return

    def __check_bbox(self):
        if self.__query_props.min_lat_lon is not None:
            LOGGER.debug(f'setting Lat-Lon min condition as sql: {self.__query_props.min_lat_lon}')
            self.__conditions.append(f"{CDMSConstants.lat_col} >= {self.__query_props.min_lat_lon[0]}")
            self.__conditions.append(f"{CDMSConstants.lon_col} >= {self.__query_props.min_lat_lon[1]}")
        if self.__query_props.max_lat_lon is not None:
            LOGGER.debug(f'setting Lat-Lon max condition as sql: {self.__query_props.max_lat_lon}')
            self.__conditions.append(f"{CDMSConstants.lat_col} <= {self.__query_props.max_lat_lon[0]}")
            self.__conditions.append(f"{CDMSConstants.lon_col} <= {self.__query_props.max_lat_lon[1]}")
        return

    def __check_depth(self):
        if self.__query_props.min_depth is None and self.__query_props.max_depth is None:
            return None
        depth_conditions = []
        if self.__query_props.min_depth is not None:
            LOGGER.debug(f'setting depth min condition: {self.__query_props.min_depth}')
            depth_conditions.append(f"{CDMSConstants.depth_col} >= {self.__query_props.min_depth}")
        if self.__query_props.max_depth is not None:
            LOGGER.debug(f'setting depth max condition: {self.__query_props.max_depth}')
            depth_conditions.append(f"{CDMSConstants.depth_col} <= {self.__query_props.max_depth}")
        LOGGER.debug(f'has depth condition. adding missing depth conditon')
        if len(depth_conditions) == 1:
            self.__conditions.append(f'({depth_conditions[0]} OR {CDMSConstants.depth_col} == {self.__missing_depth_value})')
            return
        self.__conditions.append(f"(({' AND '.join(depth_conditions) }) OR {CDMSConstants.depth_col} == {self.__missing_depth_value})")
        return

    def __add_variables_filter(self):
        if len(self.__query_props.variable) < 1:
            return None
        variables_filter = []
        for each in self.__query_props.variable:
            LOGGER.debug(f'setting not null variable: {each}')
            variables_filter.append(f"{each} IS NOT NULL")
        self.__conditions.append(f"({' OR '.join(variables_filter)})")
        return

    def __check_columns(self):
        if len(self.__query_props.columns) < 1:
            self.__columns = []
            return
        variable_columns = []
        for each in self.__query_props.variable:
            variable_columns.append(each)
            if self.__query_props.quality_flag is True:
                LOGGER.debug(f'adding quality flag for : {each}')
                variable_columns.append(f'{each}_quality')
        self.__columns = self.__query_props.columns + variable_columns + self.__columns
        return

    def manage_query_props(self):
        self.__is_extending_base = True
        self.__check_provider()
        self.__check_project()
        self.__check_platform()
        self.__check_time_range()
        self.__check_bbox()
        self.__check_depth()
        self.__add_variables_filter()
        self.__check_columns()
        return

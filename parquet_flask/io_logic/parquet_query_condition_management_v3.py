import logging

from parquet_flask.io_logic.ingest_new_file import GEOSPATIAL_INTERVAL
from parquet_flask.io_logic.partitioned_parquet_path import PartitionedParquetPath
from parquet_flask.utils.spatial_utils import SpatialUtils
from parquet_flask.utils.time_utils import TimeUtils
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.query_v2 import QueryProps

LOGGER = logging.getLogger(__name__)


class ParquetQueryConditionManagementV3:
    def __init__(self, parquet_name, missing_depth_value, props=QueryProps()):
        self.__conditions = []
        self.__parquet_name = parquet_name if not parquet_name.endswith('/') else parquet_name[:-1]
        self.__columns = [CDMSConstants.time_col, CDMSConstants.depth_col, CDMSConstants.lat_col, CDMSConstants.lon_col, CDMSConstants.platform_col, CDMSConstants.provider_col, CDMSConstants.project_col, CDMSConstants.meta_col]
        self.__is_extending_base = True
        self.__query_props = props
        self.__missing_depth_value = missing_depth_value
        self.__parquet_names: [PartitionedParquetPath] = []

    def stringify_parquet_names(self):
        return [k.generate_path() for k in self.__parquet_names]

    @property
    def parquet_names(self):
        return self.__parquet_names

    @parquet_names.setter
    def parquet_names(self, val):
        """
        :param val:
        :return: None
        """
        self.__parquet_names = val
        return

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
        self.parquet_names = [PartitionedParquetPath(self.__parquet_name).set_provider(self.__query_props.provider)]
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
        new_parquet_names = [k.duplicate().set_project(self.__query_props.project) for k in self.parquet_names]
        self.parquet_names = new_parquet_names
        return

    def __check_platform(self):
        if self.__query_props.platform_code is None:
            self.__is_extending_base = False
            # self.__columns.append(CDMSConstants.platform_code_col)  # platform_code has separate nested column.
            return
        if not self.__is_extending_base:
            LOGGER.debug(f'setting platform_code condition as sql: {self.__query_props.platform_code}')
            # self.__columns.append(CDMSConstants.platform_code_col)
            comma_sep_platforms = ','.join([f"'{k}'" for k in self.__query_props.platform_code])
            self.__conditions.append(f"{CDMSConstants.platform_code_col} in ({comma_sep_platforms})")
            return
        LOGGER.debug(f'setting platform_code condition as path: {self.__query_props.platform_code}')
        new_parquet_names = []
        for each in self.__query_props.platform_code:
            new_parquet_names.extend([k.duplicate().set_platform(each) for k in self.parquet_names])
        self.parquet_names = new_parquet_names
        return

    def __generate_time_partition_list(self, min_time, max_time):
        if min_time.year == max_time.year: # same year
            new_parquet_names = []
            for each_month in range(min_time.month, max_time.month + 1):
                new_parquet_names.extend([k.duplicate().set_year(min_time.year).set_month(each_month) for k in self.parquet_names])
            self.parquet_names = new_parquet_names
            return
        # different year
        new_parquet_names = []
        for each_whole_year in range(min_time.year + 1, max_time.year):  # for whole years
            new_parquet_names.extend([k.duplicate().set_year(each_whole_year) for k in self.parquet_names])
        if min_time.month == 1:
            new_parquet_names.extend([k.duplicate().set_year(min_time.year) for k in self.parquet_names])
        else:
            for each_month in range(min_time.month, 13):  # months for beginning year
                new_parquet_names.extend([k.duplicate().set_year(min_time.year).set_month(each_month) for k in self.parquet_names])
        if max_time.month == 12:
            new_parquet_names.extend([k.duplicate().set_year(max_time.year) for k in self.parquet_names])
        else:
            for each_month in range(1, max_time.month + 1):  # months for ending year
                new_parquet_names.extend([k.duplicate().set_year(max_time.year).set_month(each_month) for k in self.parquet_names])
        self.parquet_names = new_parquet_names
        return

    def __check_time_range(self):
        if self.__query_props.min_datetime is None and self.__query_props.max_datetime is None:
            self.__is_extending_base = False
            return None
        min_time = max_time = None
        if self.__query_props.min_datetime is not None:
            LOGGER.debug(f'setting datetime min condition as sql: {self.__query_props.min_datetime}')
            min_time = TimeUtils.get_datetime_obj(self.__query_props.min_datetime)
            # conditions.append(f"{CDMSConstants.year_col} >= {min_year}")
            self.__conditions.append(f"{CDMSConstants.time_obj_col} >= '{self.__query_props.min_datetime}'")
        if self.__query_props.max_datetime is not None:
            LOGGER.debug(f'setting datetime max condition as sql: {self.__query_props.max_datetime}')
            max_time = TimeUtils.get_datetime_obj(self.__query_props.max_datetime)
            # conditions.append(f"{CDMSConstants.year_col} <= {max_year}")
            self.__conditions.append(f"{CDMSConstants.time_obj_col} <= '{self.__query_props.max_datetime}'")
        if self.__is_extending_base is False:
            self.__is_extending_base = False
            # TODO add year and month to the conditions, but not sure it will have any effect
            return
        if min_time is None or max_time is None:
            self.__is_extending_base = False
            # TODO add year and month to the query conditions. But not sure it will have any effect
            return
        if min_time > max_time:
            # TODO should we throw an error here?
            raise ValueError(f'invalid time range')
        self.__generate_time_partition_list(min_time, max_time)
        return

    def __generate_bbox_list(self, lat_lon_intervals: list):
        new_parquet_names = []
        for each_lat_lon in lat_lon_intervals:
            new_parquet_names.extend([k.duplicate().set_lat_lon(each_lat_lon) for k in self.parquet_names])
        self.parquet_names = new_parquet_names
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

        if self.__query_props.min_lat_lon is None or self.__query_props.max_lat_lon is None:
            self.__is_extending_base = False
            return
        lat_lon_intervals = SpatialUtils.generate_lat_lon_intervals(tuple(self.__query_props.min_lat_lon), tuple(self.__query_props.max_lat_lon), GEOSPATIAL_INTERVAL)
        self.__generate_bbox_list(lat_lon_intervals)
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
        LOGGER.debug(f'has depth condition. adding missing depth condition')
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
        self.__check_bbox()
        self.__check_time_range()
        self.__check_depth()
        self.__add_variables_filter()
        self.__check_columns()
        return

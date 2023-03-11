import logging

from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.aws.es_factory import ESFactory
from parquet_flask.insitu.file_structure_setting import FileStructureSetting
from parquet_flask.io_logic.parquet_path_retriever import ParquetPathRetriever
from parquet_flask.io_logic.partitioned_parquet_path import PartitionedParquetPath
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.io_logic.query_v2 import QueryProps

LOGGER = logging.getLogger(__name__)


class ParquetQueryConditionManagementV4:
    def __init__(self, parquet_name: str, missing_depth_value, es_config: dict, file_structure_setting: FileStructureSetting, props=QueryProps()):
        self.__file_structure_setting = file_structure_setting
        self.__conditions = []
        self.__parquet_name = parquet_name if not parquet_name.endswith('/') else parquet_name[:-1]
        self.__columns = [CDMSConstants.time_col, CDMSConstants.depth_col, CDMSConstants.lat_col, CDMSConstants.lon_col]
        self.__query_props = props
        self.__missing_depth_value = missing_depth_value
        self.__parquet_names: [PartitionedParquetPath] = []
        self.__es_config = es_config

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

    # TODO: abstraction refactor removed __generate_time_partition_list method as it is not used anymore.
    def __check_time_range(self):
        if self.__query_props.min_datetime is None and self.__query_props.max_datetime is None:
            return None
        min_time = max_time = None
        if self.__query_props.min_datetime is not None:
            LOGGER.debug(f'setting datetime min condition as sql: {self.__query_props.min_datetime}')
            self.__conditions.append(f"{CDMSConstants.time_obj_col} >= '{self.__query_props.min_datetime}'")
        if self.__query_props.max_datetime is not None:
            LOGGER.debug(f'setting datetime max condition as sql: {self.__query_props.max_datetime}')
            self.__conditions.append(f"{CDMSConstants.time_obj_col} <= '{self.__query_props.max_datetime}'")
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
        self.__check_bbox()
        self.__check_time_range()
        self.__check_depth()
        self.__add_variables_filter()
        self.__check_columns()
        aws_es: ESAbstract = ESFactory().get_instance('AWS', index=self.__es_config['es_index'], base_url=self.__es_config['es_url'], port=self.__es_config.get('es_port', 443))
        # TODO astraction : how to convert to this dict from __query_props
        query_dict = {
            'provider': 'Florida State University, COAPS',
            'project': 'SAMOS',
            'platform': ','.join(['30', '31', '32']),
            'startTime': '2017-01-25T09:00:00Z',
            'endTime': '2018-10-24T09:00:00Z',
            'bbox': ','.join([str(k) for k in [-180.0, -90, 179.38330739034632, 89.90]]),
        }
        es_retriever = ParquetPathRetriever(aws_es, self.__file_structure_setting, self.__parquet_name)
        self.__parquet_names = es_retriever.start(query_dict)
        return self

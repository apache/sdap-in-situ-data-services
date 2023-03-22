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
import logging
from datetime import datetime


from parquet_flask.insitu.file_structure_setting import FileStructureSetting
from parquet_flask.utils.file_utils import FileUtils
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import Row

from parquet_flask.io_logic.cdms_schema import CdmsSchema
from parquet_flask.io_logic.parquet_query_condition_management_v4 import ParquetQueryConditionManagementV4
from parquet_flask.io_logic.partitioned_parquet_path import PartitionedParquetPath
from parquet_flask.io_logic.cdms_constants import CDMSConstants
from parquet_flask.utils.config import Config
from parquet_flask.utils.general_utils import GeneralUtils

LOGGER = logging.getLogger(__name__)


class QueryV4:
    def __init__(self, query_params_dict: dict):
        # TODO : abstraction : big time here
        self.__query_params_dict = query_params_dict
        config = Config()
        self.__file_structure_setting = FileStructureSetting(FileUtils.read_json(config.get_value(Config.in_situ_schema)), FileUtils.read_json(config.get_value(Config.file_structure_setting)))
        self.__app_name = config.get_spark_app_name()
        self.__master_spark = config.get_value(Config.master_spark_url)
        self.__parquet_name = config.get_value(Config.parquet_file_name)
        self.__es_config = {
            'es_url': config.get_value(Config.es_url),
            'es_index': CDMSConstants.es_index_parquet_stats,
            'es_port': int(config.get_value(Config.es_port, '443')),
        }
        self.__parquet_name = self.__parquet_name if not self.__parquet_name.endswith('/') else self.__parquet_name[:-1]
        self.__missing_depth_value = CDMSConstants.missing_depth_value
        self.__conditions = []
        self.__sorting_columns = self.__file_structure_setting.get_query_sort_mechanism()['sorting_columns']
        self.__set_missing_depth_val()
        self.__page_size = self.__get_size()
        self.__page_marker = self.__get_page_marker()
        
    def __get_page_marker(self):
        pagination_marker_key = self.__file_structure_setting.get_query_sort_mechanism()['pagination_marker_key']
        if pagination_marker_key not in self.__query_params_dict:
            return None
        return self.__query_params_dict[pagination_marker_key]

    def __get_size(self):
        page_size_key = self.__file_structure_setting.get_query_sort_mechanism()['page_size_key']
        if page_size_key not in self.__query_params_dict:
            return 10  # TODO abstraction. this default value comes from setting
        return int(self.__query_params_dict[page_size_key])
    
    def __set_missing_depth_val(self):
        possible_missing_depth = Config().get_value(Config.missing_depth_value)
        if GeneralUtils.is_int(possible_missing_depth):
            self.__missing_depth_value = int(possible_missing_depth)
        return

    def __retrieve_spark(self):
        from parquet_flask.io_logic.retrieve_spark_session import RetrieveSparkSession
        spark = RetrieveSparkSession().retrieve_spark_session(self.__app_name, self.__master_spark)
        return spark

    def __strip_duplicates_maintain_order(self, condition_manager: ParquetQueryConditionManagementV4):
        LOGGER.warning(f'length of parquet_names: {len(condition_manager.parquet_names)}')
        distinct_list = []
        distinct_set = set([])
        for each in condition_manager.parquet_names:
            each: PartitionedParquetPath = each
            parquet_path = each.generate_path()
            if parquet_path in distinct_set:
                continue
            distinct_set.add(parquet_path)
            distinct_list.append(each)
        LOGGER.warning(f'length of distinct_parquet_names: {len(distinct_list)}')
        LOGGER.warning(f'distinct_parquet_names: {distinct_set}')
        return distinct_list

    def get_unioned_read_df(self, condition_manager: ParquetQueryConditionManagementV4, spark: SparkSession) -> DataFrame:
        cdms_spark_struct = CdmsSchema().get_schema_from_json(FileUtils.read_json(Config().get_value(Config.in_situ_schema)))
        if len(condition_manager.parquet_names) < 1:
            LOGGER.fatal(f'cannot find any in ES. returning None instead of searching entire parquet directory for now. ')
            return None
            # read_df: DataFrame = spark.read.schema(cdms_spark_struct).parquet(condition_manager.parquet_name)
            # return read_df
        read_df_list = []
        distinct_parquet_names = self.__strip_duplicates_maintain_order(condition_manager)
        for each in distinct_parquet_names:
            each: PartitionedParquetPath = each
            try:
                temp_df: DataFrame = spark.read.schema(cdms_spark_struct).parquet(each.generate_path())
                for k, v in each.generate_continuous_partitioned_dict().items():
                    temp_df: DataFrame = temp_df.withColumn(k, lit(v))
                    # TODO : abstraction: remove columns that are not needed like year, month, lat_long
                    # TODO : abstraction - maybe that's not needed. It's defined in start() method
                read_df_list.append(temp_df)
            except Exception as e:
                LOGGER.exception(f'failed to retrieve data from spark for: {each.generate_path()}')
        if len(read_df_list) < 1:
            return None
        main_read_df: DataFrame = read_df_list[0]
        for each in read_df_list[1:]:
            main_read_df = main_read_df.union(each)
        return main_read_df

    def __get_paged_result(self, result_df: DataFrame, total_result: int):
        current_page_size = total_result if total_result < self.__page_size else self.__page_size
        result = result_df.limit(current_page_size).tail(current_page_size)
        return result

    def __get_sorting_params(self, query_result: DataFrame):
        return [query_result[k].asc() for k in self.__sorting_columns]

    def __get_nth_first_page(self, query_result: DataFrame):
        result_head = query_result.where(f"{CDMSConstants.time_col} = '{self.__props.min_datetime}'").sort(self.__get_sorting_params(query_result)).collect()
        new_index = -1
        for i, each_row in enumerate(result_head):
            each_row: Row = each_row
            each_sha_256 = GeneralUtils.gen_sha_256_json_obj(each_row.asDict())
            if each_sha_256 == self.__page_marker:
                new_index = i
                break
        if new_index < 0:
            LOGGER.warning(f'comparing sha256: {self.__page_marker}')
            for each_row in result_head:
                each_row: Row = each_row
                each_sha_256 = GeneralUtils.gen_sha_256_json_obj(each_row.asDict())
                LOGGER.warning(f'each row: {str(each_row)}. each_sha_256: {each_sha_256}')
            raise ValueError(f'cannot find existing row. It should not happen.')
        result_page = query_result.take(self.__page_size + new_index + 1)
        result_tail = result_page[new_index + 1:]
        return result_tail

    def __get_page(self, query_result: DataFrame, total_result: int):
        if self.__page_size == 0:
            return []
        if self.__page_marker is not None:  # pagination new logic
            return self.__get_nth_first_page(query_result)
        if total_result < 0:
            raise ValueError('total_result is not calculated for old pagination logic. This should not happen. Something has horribly gone wrong')
        return self.__get_paged_result(query_result, total_result)

    def __get_total_count(self, query_result: DataFrame):
        if self.__page_marker is not None:
            LOGGER.debug(f'not counting total since this is an Nth page')
            return -1
        LOGGER.debug(f'counting total')
        return int(query_result.count())

    def search(self, spark_session=None):
        LOGGER.debug(f'<delay_check> query_v4_search started')
        condition_manager = ParquetQueryConditionManagementV4(self.__parquet_name, self.__missing_depth_value, self.__es_config, self.__file_structure_setting, self.__query_params_dict)
        condition_manager.manage_query_props()

        conditions = ' AND '.join(condition_manager.conditions)
        query_begin_time = datetime.now()
        LOGGER.debug(f'<delay_check> query begins at {query_begin_time}')
        spark = self.__retrieve_spark() if spark_session is None else spark_session
        created_spark_session_time = datetime.now()
        LOGGER.debug(f'<delay_check>spark session created at {created_spark_session_time}. duration: {created_spark_session_time - query_begin_time}')
        LOGGER.debug(f'__parquet_name: {condition_manager.parquet_name}')
        read_df: DataFrame = self.get_unioned_read_df(condition_manager, spark)
        if read_df is None:
            return {
                'total': 0,
                'results': [],
            }
        read_df_time = datetime.now()
        LOGGER.debug(f'<delay_check> parquet read created at {read_df_time}. duration: {read_df_time - created_spark_session_time}')
        query_result = read_df.where(conditions)
        query_result = query_result.sort(self.__get_sorting_params(query_result))
        query_time = datetime.now()
        LOGGER.debug(f'<delay_check> parquet read filtered at {query_time}. duration: {query_time - read_df_time}')
        LOGGER.debug(f'<delay_check> total duration: {query_time - query_begin_time}')
        total_result = self.__get_total_count(query_result)
        LOGGER.debug(f'<delay_check> total calc count duration: {datetime.now() - query_time}')
        if self.__page_size < 1:
            LOGGER.debug(f'returning only the size: {total_result}')
            return {
                'total': total_result,
                'results': [],
            }
        query_time = datetime.now()
        removing_cols = self.__file_structure_setting.get_query_input_column_filters()['removing_columns']
        if len(condition_manager.columns) > 0:
            query_result = query_result.select(condition_manager.columns)
        else:
            query_result = query_result.drop(*removing_cols)
        LOGGER.debug(f'<delay_check> returning size : {total_result}')
        result = self.__get_page(query_result, total_result)
        query_result.unpersist()
        LOGGER.debug(f'<delay_check> total retrieval duration: {datetime.now() - query_time}')
        # spark.stop()
        return {
            'total': total_result,
            'results': [k.asDict() for k in result],
        }

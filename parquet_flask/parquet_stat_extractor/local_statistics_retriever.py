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

from parquet_flask.parquet_stat_extractor.local_spark_session import LocalSparkSession
from parquet_flask.parquet_stat_extractor.statistics_retriever import StatisticsRetriever
from pyspark.sql.utils import AnalysisException

from parquet_flask.utils.file_utils import FileUtils
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from parquet_flask.io_logic.cdms_schema import CdmsSchema

LOGGER = logging.getLogger(__name__)


class LocalStatisticsRetriever:
    def __init__(self, local_parquet_file_path: str, in_situ_schema_file_path: str):
        self.__local_parquet_file_path = local_parquet_file_path
        self.__in_situ_schema_file_path = in_situ_schema_file_path

    def start(self):
        spark: SparkSession = LocalSparkSession().get_spark_session()
        try:
            insitu_schema = FileUtils.read_json(self.__in_situ_schema_file_path)
            cdms_spark_struct = CdmsSchema().get_schema_from_json(insitu_schema)
            read_df: DataFrame = spark.read.schema(cdms_spark_struct).parquet(self.__local_parquet_file_path)
        except AnalysisException as analysis_exception:
            if analysis_exception.desc is not None and analysis_exception.desc.startswith('Path does not exist'):
                LOGGER.debug(f'no such full_parquet_path: {self.__in_situ_schema_file_path}')
                return None
            LOGGER.exception(f'error while retrieving full_parquet_path: {self.__in_situ_schema_file_path}')
            raise analysis_exception
        stats = StatisticsRetriever(read_df, CdmsSchema().get_observation_names(insitu_schema)).start()
        return stats.to_json()

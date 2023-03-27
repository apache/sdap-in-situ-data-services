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
from copy import deepcopy


class PartitionedParquetPath:
    def __init__(self, base_name: str, partitioned_columns: list):
        self.__base_name = base_name
        self.__partitioned_columns = partitioned_columns
        self.__partitioned_values = []

    def load_from_es(self, es_result: dict):
        self.__partitioned_values = [es_result[k] if k in es_result else None for k in self.__partitioned_columns]
        return self

    def duplicate(self):
        return deepcopy(self)

    def __str__(self) -> str:
        return self.generate_path()

    def generate_continuous_partitioned_dict(self):
        if len(self.__partitioned_columns) != len(self.__partitioned_values):
            raise ValueError(f'mismatch key v. value: {self.__partitioned_columns} v. {self.__partitioned_values}')
        partitioned_dict = {k: v for k, v in zip(self.__partitioned_columns, self.__partitioned_values) if v is not None}
        return partitioned_dict

    def generate_path(self):
        if len(self.__partitioned_columns) != len(self.__partitioned_values):
            raise ValueError(f'mismatch key v. value: {self.__partitioned_columns} v. {self.__partitioned_values}')
        parquet_path = self.__base_name
        for k, v in zip(self.__partitioned_columns, self.__partitioned_values):
            if v is None:
                return parquet_path
            parquet_path = f'{parquet_path}/{k}={v}'  # This assumes lat_lon (if exists) is already a string split by `_`
        return parquet_path

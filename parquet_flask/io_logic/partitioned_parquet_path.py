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
            parquet_path = f'{parquet_path}/{k}={v}'  # TODO abstraction This assumes lat_lon (if exists) is already a string split by `_`
        return parquet_path

from parquet_flask.insitu.file_structure_setting import FileStructureSetting


class ParquetFilePathStatExtractor:
    def __init__(self, file_structure_setting: FileStructureSetting, abs_file_path: str,
                 base_name_key: str = 'base_name',
                 file_name_key: str = 'file_name',
                 abs_file_path_key: str = 'abs_file_path'):
        self.__file_structure_setting = file_structure_setting
        self.__abs_file_path = abs_file_path
        self.__base_name_key = base_name_key
        self.__file_name_key = file_name_key
        self.__abs_file_path_key = abs_file_path_key
        self.__base_name = ''
        self.__file_name = ''
        self.__partitions = {}

    def __compute_s3(self):
        split_s3_url = self.__abs_file_path.split('://')
        if len(split_s3_url) != 2:
            raise ValueError(f'invalid S3 URL: {self.__abs_file_path}')
        split_s3_path = split_s3_url[1].strip().split('/')
        if len(split_s3_path) < 2:
            raise ValueError(f'invalid s3 path: {split_s3_url[1]}')
        self.__base_name = split_s3_path[0]
        self.__file_name = split_s3_path[-1]
        partition_dict = [k.split('=') for k in split_s3_path[1: -1] if '=' in k]
        partition_dict = {k[0]: k[1] for k in partition_dict}
        configured_partitions = set(self.__file_structure_setting.get_partitioning_columns())
        self.__partitions = {k: v for k, v in partition_dict.items() if k in configured_partitions}
        return self

    def to_json(self):
        output_json = {
            **{
                self.__base_name_key: self.__base_name,
                self.__file_name_key: self.__file_name,
                self.__abs_file_path_key: self.__abs_file_path,
            },
            **self.__partitions
        }
        return output_json

    def start(self):
        if self.__abs_file_path.startswith('s3://'):
            return self.__compute_s3()
        raise NotImplementedError(f'not implemented for : {self.__abs_file_path}')

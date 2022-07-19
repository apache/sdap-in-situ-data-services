from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as pyspark_functions

from parquet_flask.io_logic.cdms_constants import CDMSConstants


class StatisticsRetriever:
    def __init__(self, input_dataset: DataFrame):
        self.__input_dataset = input_dataset
        self.__total = -1
        self.__min_datetime = None
        self.__max_datetime = None
        self.__min_depth = None
        self.__max_depth = None
        self.__min_lat = None
        self.__max_lat = None
        self.__min_lon = None
        self.__max_lon = None

    @property
    def total(self):
        if self.__total == -1:
            self.total = int(self.__input_dataset.count())
        return self.__total

    @total.setter
    def total(self, val):
        """
        :param val:
        :return: None
        """
        self.__total = val
        return

    @property
    def min_datetime(self):
        return self.__min_datetime

    @min_datetime.setter
    def min_datetime(self, val):
        """
        :param val:
        :return: None
        """
        self.__min_datetime = val
        return

    @property
    def max_datetime(self):
        return self.__max_datetime

    @max_datetime.setter
    def max_datetime(self, val):
        """
        :param val:
        :return: None
        """
        self.__max_datetime = val
        return

    @property
    def min_depth(self):
        return self.__min_depth

    @min_depth.setter
    def min_depth(self, val):
        """
        :param val:
        :return: None
        """
        self.__min_depth = val
        return

    @property
    def max_depth(self):
        return self.__max_depth

    @max_depth.setter
    def max_depth(self, val):
        """
        :param val:
        :return: None
        """
        self.__max_depth = val
        return

    @property
    def min_lat(self):
        return self.__min_lat

    @min_lat.setter
    def min_lat(self, val):
        """
        :param val:
        :return: None
        """
        self.__min_lat = val
        return

    @property
    def max_lat(self):
        return self.__max_lat

    @max_lat.setter
    def max_lat(self, val):
        """
        :param val:
        :return: None
        """
        self.__max_lat = val
        return

    @property
    def min_lon(self):
        return self.__min_lon

    @min_lon.setter
    def min_lon(self, val):
        """
        :param val:
        :return: None
        """
        self.__min_lon = val
        return

    @property
    def max_lon(self):
        return self.__max_lon

    @max_lon.setter
    def max_lon(self, val):
        """
        :param val:
        :return: None
        """
        self.__max_lon = val
        return

    def __get_min_depth_exclude_missing_val(self):
        filtered_input_dsert = self.__input_dataset.where(f'{CDMSConstants.depth_col} != {CDMSConstants.missing_depth_value}')
        stats = filtered_input_dsert.select(pyspark_functions.min(CDMSConstants.depth_col)).collect()
        if len(stats) != 1:
            raise ValueError(f'invalid row count on stats function: {stats}')
        stats = stats[0].asDict()
        self.min_depth = stats[f'min({CDMSConstants.depth_col})']
        return

    def to_json(self) -> dict:
        """
        :return:
        """
        return {
            'total': self.total,
            'min_datetime': self.min_datetime,
            'max_datetime': self.max_datetime,
            'min_depth': self.min_depth,
            'max_depth': self.max_depth,
            'min_lat': self.min_lat,
            'max_lat': self.max_lat,
            'min_lon': self.min_lon,
            'max_lon': self.max_lon,
        }

    def start(self):
        stats = self.__input_dataset.select(pyspark_functions.min(CDMSConstants.lat_col),
                                            pyspark_functions.max(CDMSConstants.lat_col),
                                            pyspark_functions.min(CDMSConstants.lon_col),
                                            pyspark_functions.max(CDMSConstants.lon_col),
                                            pyspark_functions.min(CDMSConstants.depth_col),
                                            pyspark_functions.max(CDMSConstants.depth_col),
                                            pyspark_functions.min(CDMSConstants.time_obj_col),
                                            pyspark_functions.max(CDMSConstants.time_obj_col)).collect()
        if len(stats) != 1:
            raise ValueError(f'invalid row count on stats function: {stats}')

        stats = stats[0].asDict()
        self.min_lat = stats[f'min({CDMSConstants.lat_col})']
        self.max_lat = stats[f'max({CDMSConstants.lat_col})']

        self.min_lon = stats[f'min({CDMSConstants.lon_col})']
        self.max_lon = stats[f'max({CDMSConstants.lon_col})']

        self.min_depth = stats[f'min({CDMSConstants.depth_col})']
        self.max_depth = stats[f'max({CDMSConstants.depth_col})']

        self.min_datetime = stats[f'min({CDMSConstants.time_obj_col})'].timestamp()
        self.max_datetime = stats[f'max({CDMSConstants.time_obj_col})'].timestamp()

        if self.min_depth - CDMSConstants.missing_depth_value == 0:
            self.__get_min_depth_exclude_missing_val()
        return self

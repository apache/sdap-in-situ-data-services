import math


class SpatialUtils:
    @staticmethod
    def generate_lat_lon_intervals(min_lat_lon: tuple, max_lat_lon: tuple, interval: int) -> list:
        """

        :param min_lat_lon: tuple : (lat, lon)
        :param max_lat_lon: tuple : (lat, lon)
        :param interval: int
        :return: list
        """
        def __floor_by_interval(input_value):
            return int(input_value - divmod(input_value, interval)[1])

        def __get_end_range(input_value):
            potential_end_range = math.ceil(input_value)
            return potential_end_range + 1

        if not isinstance(min_lat_lon, tuple) or not isinstance(max_lat_lon, tuple) or len(min_lat_lon) != 2 or len(max_lat_lon) != 2:
            raise ValueError(f'incorrect input. min_lat_lon & max_lat_lon should be tuple with size 2: {min_lat_lon}, {max_lat_lon}')
        min_lat = __floor_by_interval(min_lat_lon[0])
        min_lon = __floor_by_interval(min_lat_lon[1])

        lat_intervals = [k for k in range(min_lat, __get_end_range(max_lat_lon[0]), interval)]
        lon_intervals = [k for k in range(min_lon, __get_end_range(max_lat_lon[1]), interval)]

        lat_long_list = [(each_lat, each_lon) for each_lon in lon_intervals for each_lat in lat_intervals]
        return lat_long_list

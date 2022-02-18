import json

import requests

from tests.bench_mark.func_exec_time_decorator import func_exec_time_decorator


class BenchMark:
    def __init__(self):
        self.__cdms_domain = 'http://localhost:9801'
        # self.__cdms_domain = 'https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu'
        self.__size = 100
        self.__start_index = 0

        self.__provider = 'NCAR'
        self.__project = 'ICOADS Release 3.0'
        self.__platform_code = '41'

        self.__variable = 'relative_humidity'
        self.__columns = 'air_temperature'
        self.__start_time = '2017-01-01T00:00:00Z'
        self.__end_time = '2017-01-30T00:00:00Z'
        self.__min_depth = -99
        self.__max_depth = 0
        self.__min_lat_lon = (-111, 11)
        self.__max_lat_lon = (111, 99)

    @func_exec_time_decorator
    def __execute_query(self):
        """
        time curl 'https://doms.jpl.nasa.gov/insitu?startIndex=3&itemsPerPage=20&minDepth=-99&variable=relative_humidity&columns=air_temperature&maxDepth=-1&startTime=2019-02-14T00:00:00Z&endTime=2021-02-16T00:00:00Z&platform=3B&bbox=-111,11,111,99'

        :return:
        """
        response = requests.get(
            url=f'{self.__cdms_domain}/1.0/query_data_doms?startIndex={self.__start_index}&itemsPerPage={self.__size}'
                f'&provider={self.__provider}'
                f'&project={self.__project}'
                f'&platform={self.__platform_code}'
                f'{"" if self.__variable is None else f"&variable={self.__variable}"}'
                f'{"" if self.__columns is None else f"&columns={self.__columns}"}'
                f'&minDepth={self.__min_depth}&maxDepth={self.__max_depth}'
                f'&startTime={self.__start_time}&endTime={self.__end_time}'
                f'&bbox={self.__min_lat_lon[0]},{self.__min_lat_lon[1]},{self.__max_lat_lon[0]},{self.__max_lat_lon[1]}', verify=False
        )
        if response.status_code > 400:
            raise ValueError(f'wrong status code: {response.status_code}. details: {response.text}')
        return json.loads(response.text)

    def pagination_bench_mark(self):
        """
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- start_index: 0 -- total: 113561 -- duration: 16.313075
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 10000 -- duration: 11.699316 -- first_item: {'air_temperature': 7.9, 'relative_humidity': 56.2, 'time': '2017-01-28T11:30:00Z', 'depth': -99999.0, 'latitude': 33.4, 'longitude': -77.7}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 10000 -- duration: 13.780819 -- first_item: {'air_temperature': 24.0, 'relative_humidity': 70.0, 'time': '2017-01-16T23:30:00Z', 'depth': -99999.0, 'latitude': 23.5, 'longitude': -71.8}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 10000 -- duration: 14.555451 -- first_item: {'air_temperature': 5.9, 'relative_humidity': 74.9, 'time': '2017-01-06T19:00:00Z', 'depth': -99999.0, 'latitude': 53.1, 'longitude': 2.8}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 10000 -- duration: 15.820533 -- first_item: {'air_temperature': 24.2, 'relative_humidity': 70.1, 'time': '2017-01-23T05:49:48Z', 'depth': -99999.0, 'latitude': 21.6, 'longitude': -58.6}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 10000 -- duration: 18.046887 -- first_item: {'air_temperature': 26.1, 'relative_humidity': 74.9, 'time': '2017-01-18T17:40:12Z', 'depth': -99999.0, 'latitude': 11.3, 'longitude': -60.5}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 10000 -- duration: 19.41441 -- first_item: {'air_temperature': 9.8, 'relative_humidity': 94.8, 'time': '2017-01-18T15:00:00Z', 'depth': -99999.0, 'latitude': 61.2, 'longitude': 0.9}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 10000 -- duration: 20.412385 -- first_item: {'air_temperature': 6.4, 'relative_humidity': 81.1, 'time': '2017-01-28T21:00:00Z', 'depth': -99999.0, 'latitude': 53.4, 'longitude': 1.7}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 10000 -- duration: 21.88728 -- first_item: {'air_temperature': 24.3, 'relative_humidity': 62.5, 'time': '2017-01-24T22:40:12Z', 'depth': -99999.0, 'latitude': 23.6, 'longitude': -72.1}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 10000 -- duration: 23.670248 -- first_item: {'air_temperature': 27.6, 'relative_humidity': 69.0, 'time': '2017-01-01T18:30:00Z', 'depth': -99999.0, 'latitude': 18.4, 'longitude': -69.6}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 10000 -- duration: 25.683189 -- first_item: {'air_temperature': 25.7, 'relative_humidity': 65.3, 'time': '2017-01-23T02:49:48Z', 'depth': -99999.0, 'latitude': 15.3, 'longitude': -67.5}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 10000 -- duration: 26.184124 -- first_item: {'air_temperature': 20.6, 'relative_humidity': 81.9, 'time': '2017-01-18T19:19:48Z', 'depth': -99999.0, 'latitude': 33.4, 'longitude': -77.7}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 10000 -- duration: 25.682943 -- first_item: {'air_temperature': 9.0, 'relative_humidity': 85.5, 'time': '2017-01-25T11:00:00Z', 'depth': -99999.0, 'latitude': 58.0, 'longitude': -0.3}

time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- start_index: 0 -- total: 113561 -- duration: 14.74615
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 20000 -- duration: 15.640875 -- first_item: {'air_temperature': 27.0, 'relative_humidity': 82.2, 'time': '2017-01-24T21:30:00Z', 'depth': -99999.0, 'latitude': 15.3, 'longitude': -67.5}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 20000 -- duration: 22.962723 -- first_item: {'air_temperature': 10.2, 'relative_humidity': 72.1, 'time': '2017-01-07T18:49:48Z', 'depth': -99999.0, 'latitude': 28.7, 'longitude': -86.0}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 20000 -- duration: 29.17014 -- first_item: {'air_temperature': 26.9, 'relative_humidity': 67.2, 'time': '2017-01-27T22:10:12Z', 'depth': -99999.0, 'latitude': 11.3, 'longitude': -60.5}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 20000 -- duration: 36.511349 -- first_item: {'air_temperature': 6.0, 'relative_humidity': 77.1, 'time': '2017-01-05T23:00:00Z', 'depth': -99999.0, 'latitude': 56.4, 'longitude': 2.1}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 20000 -- duration: 39.386719 -- first_item: {'air_temperature': 17.4, 'relative_humidity': 55.1, 'time': '2017-01-27T03:00:00Z', 'depth': -99999.0, 'latitude': 26.2, 'longitude': -97.1}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- total: -1 -- current_count: 20000 -- duration: 40.542413 -- first_item: {'air_temperature': 27.1, 'relative_humidity': 74.6, 'time': '2017-01-05T04:10:12Z', 'depth': -99999.0, 'latitude': 11.3, 'longitude': -60.5}

from EKS
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- start_index: 0 -- total: 113561 -- current_count: 20000 -- duration: 23.322453 -- first_item: {'air_temperature': 6.4, 'relative_humidity': 81.1, 'time': '2017-01-11T12:00:00Z', 'depth': -99999.0, 'latitude': 40.9, 'longitude': -73.7}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- start_index: 20000 -- total: 113561 -- current_count: 20000 -- duration: 9.129667 -- first_item: {'air_temperature': 24.0, 'relative_humidity': 64.1, 'time': '2017-01-17T11:00:00Z', 'depth': -99999.0, 'latitude': 23.5, 'longitude': -71.9}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- start_index: 40000 -- total: 113561 -- current_count: 20000 -- duration: 10.186722 -- first_item: {'air_temperature': 23.0, 'relative_humidity': 65.9, 'time': '2017-01-20T00:00:00Z', 'depth': -99999.0, 'latitude': 18.4, 'longitude': -69.6}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- start_index: 60000 -- total: 113561 -- current_count: 20000 -- duration: 11.604984 -- first_item: {'air_temperature': 26.5, 'relative_humidity': 71.4, 'time': '2017-01-01T17:10:12Z', 'depth': -99999.0, 'latitude': 21.1, 'longitude': -64.9}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- start_index: 80000 -- total: 113561 -- current_count: 20000 -- duration: 12.949413 -- first_item: {'air_temperature': 9.6, 'relative_humidity': 70.5, 'time': '2017-01-20T00:00:00Z', 'depth': -99999.0, 'latitude': 57.8, 'longitude': -0.9}
time: 2017-01-01T00:00:00Z - 2017-01-30T00:00:00Z -- start_index: 100000 -- total: 113561 -- current_count: 13561 -- duration: 16.5869 -- first_item: {'air_temperature': 24.7, 'relative_humidity': 62.2, 'time': '2017-01-15T16:00:00Z', 'depth': -99999.0, 'latitude': 21.6, 'longitude': -58.6}


        :return:
        """
        self.__start_time = '2017-01-01T00:00:00Z'
        self.__end_time = '2017-01-30T00:00:00Z'
        self.__start_index = 0
        self.__size = 20000
        response = self.__execute_query()
        print(f'time: {self.__start_time} - {self.__end_time} -- start_index: {self.__start_index} -- total: {response[0]["total"]} -- current_count: {len(response[0]["results"])} -- duration: {response[1]} -- first_item: {response[0]["results"][0]}')
        total = response[0]['total']
        while self.__start_index < total:
            self.__start_index += self.__size
            response = self.__execute_query()
            print(f'time: {self.__start_time} - {self.__end_time} -- start_index: {self.__start_index} -- total: {response[0]["total"]} -- current_count: {len(response[0]["results"])} -- duration: {response[1]} -- first_item: {response[0]["results"][0]}')
        return

    def time_bench_mark(self):
        """
time: 2017-01-01T00:00:00Z - 2017-01-02T00:00:00Z -- total: 8316 -- duration: 105.139927
time: 2017-12-01T00:00:00Z - 2017-12-16T00:00:00Z -- total: 59753 -- duration: 72.037163
time: 2017-02-01T00:00:00Z - 2017-02-28T00:00:00Z -- total: 104602 -- duration: 67.783443
time: 2017-04-01T00:00:00Z - 2017-05-30T00:00:00Z -- total: 380510 -- duration: 112.183817
time: 2017-06-01T00:00:00Z - 2017-08-30T00:00:00Z -- total: 661753 -- duration: 145.768916
time: 2017-01-01T00:00:00Z - 2017-06-30T00:00:00Z -- total: 979690 -- duration: 251.343631

        :return:
        """
        self.__min_depth = -99
        self.__max_depth = 0
        self.__min_lat_lon = (-111, 11)
        self.__max_lat_lon = (111, 99)
        self.__provider = 'Florida State University, COAPS'
        self.__project = 'SAMOS'
        self.__platform_code = '30'
        self.__start_index = 10


        # self.__start_time = '2017-01-01T00:00:00Z'
        # self.__end_time = '2017-01-02T00:00:00Z'
        # response = self.__execute_query()
        # print(f'time: {self.__start_time} - {self.__end_time} -- total: {response[0]["total"]} -- duration: {response[1]}')
        self.__start_time = '2017-12-01T00:00:00Z'
        self.__end_time = '2017-12-16T00:00:00Z'
        response = self.__execute_query()
        print(
            f'time: {self.__start_time} - {self.__end_time} -- total: {response[0]["total"]} -- current_count: {len(response[0]["results"])} -- duration: {response[1]} -- first_item: {response[0]["results"][0]}')
        # raise ValueError('not yet')
        self.__start_time = '2017-02-01T00:00:00Z'
        self.__end_time = '2017-02-28T00:00:00Z'
        response = self.__execute_query()
        print(
            f'time: {self.__start_time} - {self.__end_time} -- total: {response[0]["total"]} -- duration: {response[1]}')
        self.__start_time = '2017-04-01T00:00:00Z'
        self.__end_time = '2017-05-30T00:00:00Z'
        response = self.__execute_query()
        print(
            f'time: {self.__start_time} - {self.__end_time} -- total: {response[0]["total"]} -- duration: {response[1]}')
        self.__start_time = '2017-06-01T00:00:00Z'
        self.__end_time = '2017-08-30T00:00:00Z'
        response = self.__execute_query()
        print(
            f'time: {self.__start_time} - {self.__end_time} -- total: {response[0]["total"]} -- duration: {response[1]}')
        self.__start_time = '2017-01-01T00:00:00Z'
        self.__end_time = '2017-06-30T00:00:00Z'
        response = self.__execute_query()
        print(
            f'time: {self.__start_time} - {self.__end_time} -- total: {response[0]["total"]} -- duration: {response[1]}')
        self.__start_time = '2017-01-01T00:00:00Z'
        self.__end_time = '2017-12-30T00:00:00Z'
        response = self.__execute_query()
        print(
            f'time: {self.__start_time} - {self.__end_time} -- total: {response[0]["total"]} -- duration: {response[1]}')
        return

    def depth_bench_mark(self):
        return

    def bbox_bench_mark(self):
        return

    def samos_test(self):
        """
        provider=Florida State University, COAPS/
project=SAMOS/
platform_code=30/
        :return:
        """
        self.__variable = 'relative_humidity'
        self.__columns = None
        self.__start_time = '2017-01-01T00:00:00Z'
        self.__end_time = '2017-01-03T00:00:00Z'
        self.__min_depth = -99
        self.__max_depth = 0
        self.__min_lat_lon = (-111, 11)
        self.__max_lat_lon = (111, 99)
        self.__provider = 'Florida State University, COAPS'
        self.__project = 'SAMOS'
        self.__platform_code = '301'
        print(self.__execute_query())
        return


if __name__ == '__main__':
    BenchMark().pagination_bench_mark()

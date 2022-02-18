import json

import requests

from tests.bench_mark.func_exec_time_decorator import func_exec_time_decorator


class BenchMark:
    def __init__(self):
        self.__cdms_domain = 'http://localhost:30801'
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
        time: 2017-01-01T00:00:00Z - 2017-12-30T00:00:00Z -- start_index: 0 -- total: 2072146 -- duration: 1078.875818
        time: 2017-01-01T00:00:00Z - 2017-12-30T00:00:00Z -- start_index: 2072000 -- total: 2072146 -- duration: 1957.478031
        :return:
        """
        self.__start_time = '2017-01-01T00:00:00Z'
        self.__end_time = '2017-12-30T00:00:00Z'
        self.__start_index = 2072000
        response = self.__execute_query()
        print(f'time: {self.__start_time} - {self.__end_time} -- start_index: {self.__start_index} -- total: {response[0]["total"]} -- duration: {response[1]}')
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


        # self.__start_time = '2017-01-01T00:00:00Z'
        # self.__end_time = '2017-01-02T00:00:00Z'
        # response = self.__execute_query()
        # print(f'time: {self.__start_time} - {self.__end_time} -- total: {response[0]["total"]} -- duration: {response[1]}')
        self.__start_time = '2017-12-01T00:00:00Z'
        self.__end_time = '2017-12-16T00:00:00Z'
        response = self.__execute_query()
        print(
            f'time: {self.__start_time} - {self.__end_time} -- total: {response[0]["total"]} -- duration: {response[1]}')
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
    BenchMark().time_bench_mark()

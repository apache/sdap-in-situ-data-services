import json

import requests

from tests.bench_mark.func_exec_time_decorator import func_exec_time_decorator


class BenchMark:
    def __init__(self):
        # self.__cdms_domain = 'http://localhost:30801'
        self.__cdms_domain = 'https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu'
        self.__size = 100
        self.__start_index = 0

        self.__provider = 'NCAR'
        self.__project = 'ICOADS Release 3.0'
        self.__platform_code = '41'

        self.__variable = 'relative_humidity'
        self.__columns = 'air_temperature'
        self.__start_time = '2017-01-01T00:00:00Z'
        self.__end_time = '2017-03-30T00:00:00Z'
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
        print(f'{self.__cdms_domain}/1.0/query_data_doms?startIndex={self.__start_index}&itemsPerPage={self.__size}'
                f'&provider={self.__provider}'
                f'&project={self.__project}'
                f'&platform={self.__platform_code}'
                f'{"" if self.__variable is None else f"&variable={self.__variable}"}'
                f'{"" if self.__columns is None else f"&columns={self.__columns}"}'
                f'&minDepth={self.__min_depth}&maxDepth={self.__max_depth}'
                f'&startTime={self.__start_time}&endTime={self.__end_time}'
                f'&bbox={self.__min_lat_lon[0]},{self.__min_lat_lon[1]},{self.__max_lat_lon[0]},{self.__max_lat_lon[1]}')
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
Connected to pydev debugger (build 201.7223.92)
http://localhost:30801/1.0/query_data_doms?startIndex=0&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2017-03-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2017-03-30T00:00:00Z -- start_index: 0 -- total: 121010 -- current_count: 20000 -- duration: 21.127051
first_item: {'air_temperature': 4.5, 'relative_humidity': 78.6, 'time': '2017-01-12T00:00:00Z', 'depth': -99999.0, 'latitude': 61.6, 'longitude': 1.3}
http://localhost:30801/1.0/query_data_doms?startIndex=20000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2017-03-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2017-03-30T00:00:00Z -- start_index: 20000 -- total: 121010 -- current_count: 20000 -- duration: 22.362244
first_item: {'air_temperature': 23.8, 'relative_humidity': 77.8, 'time': '2017-01-25T16:49:48Z', 'depth': -99999.0, 'latitude': 25.9, 'longitude': -89.7}
http://localhost:30801/1.0/query_data_doms?startIndex=40000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2017-03-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2017-03-30T00:00:00Z -- start_index: 40000 -- total: 121010 -- current_count: 20000 -- duration: 22.788451
first_item: {'air_temperature': 8.4, 'relative_humidity': 94.7, 'time': '2017-01-07T06:49:48Z', 'depth': -99999.0, 'latitude': 29.3, 'longitude': -88.7}
http://localhost:30801/1.0/query_data_doms?startIndex=60000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2017-03-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2017-03-30T00:00:00Z -- start_index: 60000 -- total: 121010 -- current_count: 20000 -- duration: 39.308498
first_item: {'air_temperature': 10.7, 'relative_humidity': 61.9, 'time': '2017-01-05T15:00:00Z', 'depth': -99999.0, 'latitude': 57.8, 'longitude': -0.9}
http://localhost:30801/1.0/query_data_doms?startIndex=80000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2017-03-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2017-03-30T00:00:00Z -- start_index: 80000 -- total: 121010 -- current_count: 20000 -- duration: 28.825153
first_item: {'air_temperature': 16.9, 'relative_humidity': 62.9, 'time': '2017-01-19T12:10:12Z', 'depth': -99999.0, 'latitude': 33.4, 'longitude': -77.7}
http://localhost:30801/1.0/query_data_doms?startIndex=100000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2017-03-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2017-03-30T00:00:00Z -- start_index: 100000 -- total: 121010 -- current_count: 20000 -- duration: 32.178053
first_item: {'air_temperature': 3.4, 'relative_humidity': 89.9, 'time': '2017-01-07T22:00:00Z', 'depth': -99999.0, 'latitude': 33.4, 'longitude': -77.7}
http://localhost:30801/1.0/query_data_doms?startIndex=120000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2017-03-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2017-03-30T00:00:00Z -- start_index: 120000 -- total: 121010 -- current_count: 1010 -- duration: 21.210814
first_item: {'air_temperature': 5.8, 'relative_humidity': 78.8, 'time': '2017-01-13T00:00:00Z', 'depth': -99999.0, 'latitude': 61.3, 'longitude': 1.5}
http://localhost:30801/1.0/query_data_doms?startIndex=140000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2017-03-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2017-03-30T00:00:00Z -- start_index: 140000 -- total: 121010 -- current_count: 0 -- duration: 14.375344
Process finished with exit code 0

Connected to pydev debugger (build 201.7223.92)
http://localhost:30801/1.0/query_data_doms?startIndex=0&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2018-01-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2018-01-30T00:00:00Z -- start_index: 0 -- total: 2076034 -- current_count: 20000 -- duration: 124.080378
first_item: {'air_temperature': 24.5, 'relative_humidity': 73.7, 'time': '2017-09-30T12:30:00Z', 'depth': -99999.0, 'latitude': 33.4, 'longitude': -77.7}
http://localhost:30801/1.0/query_data_doms?startIndex=20000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2018-01-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2018-01-30T00:00:00Z -- start_index: 20000 -- total: 2076034 -- current_count: 20000 -- duration: 134.163414
first_item: {'air_temperature': 8.7, 'relative_humidity': 91.5, 'time': '2017-05-12T16:19:48Z', 'depth': -99999.0, 'latitude': 44.0, 'longitude': -86.6}
http://localhost:30801/1.0/query_data_doms?startIndex=40000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2018-01-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2018-01-30T00:00:00Z -- start_index: 40000 -- total: 2076034 -- current_count: 20000 -- duration: 170.192412
first_item: {'air_temperature': 27.1, 'relative_humidity': 76.9, 'time': '2017-08-25T13:19:48Z', 'depth': -99999.0, 'latitude': 28.9, 'longitude': -78.5}
http://localhost:30801/1.0/query_data_doms?startIndex=60000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2018-01-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2018-01-30T00:00:00Z -- start_index: 60000 -- total: 2076034 -- current_count: 20000 -- duration: 174.84866
first_item: {'air_temperature': 10.7, 'relative_humidity': 79.0, 'time': '2017-10-18T15:00:00Z', 'depth': -99999.0, 'latitude': 57.0, 'longitude': 1.8}
http://localhost:30801/1.0/query_data_doms?startIndex=80000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2018-01-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2018-01-30T00:00:00Z -- start_index: 80000 -- total: 2076034 -- current_count: 20000 -- duration: 174.773341
first_item: {'air_temperature': 22.3, 'relative_humidity': 69.2, 'time': '2017-04-17T00:40:12Z', 'depth': -99999.0, 'latitude': 33.4, 'longitude': -77.7}
http://localhost:30801/1.0/query_data_doms?startIndex=100000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2018-01-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2018-01-30T00:00:00Z -- start_index: 100000 -- total: 2076034 -- current_count: 20000 -- duration: 200.328648
first_item: {'air_temperature': 22.2, 'relative_humidity': 99.4, 'time': '2017-07-11T10:10:12Z', 'depth': -99999.0, 'latitude': 41.8, 'longitude': -87.0}
http://localhost:30801/1.0/query_data_doms?startIndex=120000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2018-01-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2018-01-30T00:00:00Z -- start_index: 120000 -- total: 2076034 -- current_count: 20000 -- duration: 196.793639
first_item: {'air_temperature': 26.3, 'relative_humidity': 80.6, 'time': '2017-05-09T03:30:00Z', 'depth': -99999.0, 'latitude': 21.1, 'longitude': -64.9}
http://localhost:30801/1.0/query_data_doms?startIndex=140000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2018-01-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2018-01-30T00:00:00Z -- start_index: 140000 -- total: 2076034 -- current_count: 20000 -- duration: 225.118882
first_item: {'air_temperature': 28.2, 'relative_humidity': 71.2, 'time': '2017-04-26T15:19:48Z', 'depth': -99999.0, 'latitude': 18.4, 'longitude': -69.6}
http://localhost:30801/1.0/query_data_doms?startIndex=160000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2018-01-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2018-01-30T00:00:00Z -- start_index: 160000 -- total: 2076034 -- current_count: 20000 -- duration: 216.740009
first_item: {'air_temperature': 18.1, 'relative_humidity': 48.6, 'time': '2017-11-20T00:00:00Z', 'depth': -99999.0, 'latitude': 32.5, 'longitude': -79.1}
http://localhost:30801/1.0/query_data_doms?startIndex=180000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2018-01-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2018-01-30T00:00:00Z -- start_index: 180000 -- total: 2076034 -- current_count: 20000 -- duration: 235.660017
first_item: {'air_temperature': 22.8, 'relative_humidity': 79.1, 'time': '2017-08-15T05:00:00Z', 'depth': -99999.0, 'latitude': 41.8, 'longitude': -87.0}
http://localhost:30801/1.0/query_data_doms?startIndex=200000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2018-01-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2018-01-30T00:00:00Z -- start_index: 200000 -- total: 2076034 -- current_count: 20000 -- duration: 249.714485
first_item: {'air_temperature': 16.9, 'relative_humidity': 90.3, 'time': '2017-05-29T09:10:12Z', 'depth': -99999.0, 'latitude': 41.6, 'longitude': -81.8}
http://localhost:30801/1.0/query_data_doms?startIndex=220000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2018-01-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2018-01-30T00:00:00Z -- start_index: 220000 -- total: 2076034 -- current_count: 20000 -- duration: 253.446502
first_item: {'air_temperature': 19.3, 'relative_humidity': 62.6, 'time': '2017-04-27T16:49:48Z', 'depth': -99999.0, 'latitude': 41.6, 'longitude': -81.8}
http://localhost:30801/1.0/query_data_doms?startIndex=240000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2018-01-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2018-01-30T00:00:00Z -- start_index: 240000 -- total: 2076034 -- current_count: 20000 -- duration: 270.454133
first_item: {'air_temperature': 14.7, 'relative_humidity': 68.6, 'time': '2017-08-30T12:00:00Z', 'depth': -99999.0, 'latitude': 56.4, 'longitude': 2.1}
http://localhost:30801/1.0/query_data_doms?startIndex=260000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2018-01-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-01-01T00:00:00Z - 2018-01-30T00:00:00Z -- start_index: 260000 -- total: 2076034 -- current_count: 20000 -- duration: 269.728347
first_item: {'air_temperature': 8.3, 'relative_humidity': 90.3, 'time': '2017-01-07T12:00:00Z', 'depth': -99999.0, 'latitude': 55.0, 'longitude': 6.4}
http://localhost:30801/1.0/query_data_doms?startIndex=280000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-01-01T00:00:00Z&endTime=2018-01-30T00:00:00Z&bbox=-111,11,111,99

Connected to pydev debugger (build 201.7223.92)
http://localhost:30801/1.0/query_data_doms?startIndex=0&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 0 -- total: 168250 -- current_count: 20000 -- duration: 17.341993
first_item: {'air_temperature': 7.6, 'relative_humidity': 91.5, 'time': '2017-04-20T12:00:00Z', 'depth': -99999.0, 'latitude': 57.7, 'longitude': 1.8}
http://localhost:30801/1.0/query_data_doms?startIndex=20000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 20000 -- total: 168250 -- current_count: 20000 -- duration: 21.21373
first_item: {'air_temperature': 23.2, 'relative_humidity': 64.3, 'time': '2017-04-16T19:19:48Z', 'depth': -99999.0, 'latitude': 23.8, 'longitude': -68.4}
http://localhost:30801/1.0/query_data_doms?startIndex=40000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 40000 -- total: 168250 -- current_count: 20000 -- duration: 20.055859
first_item: {'air_temperature': 14.6, 'relative_humidity': 62.8, 'time': '2017-04-15T00:40:12Z', 'depth': -99999.0, 'latitude': 42.0, 'longitude': -86.6}
http://localhost:30801/1.0/query_data_doms?startIndex=60000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 60000 -- total: 168250 -- current_count: 20000 -- duration: 35.323143
first_item: {'air_temperature': 22.8, 'relative_humidity': 56.5, 'time': '2017-04-25T15:40:12Z', 'depth': -99999.0, 'latitude': 26.0, 'longitude': -85.6}
http://localhost:30801/1.0/query_data_doms?startIndex=80000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 80000 -- total: 168250 -- current_count: 20000 -- duration: 40.637501
first_item: {'air_temperature': 27.8, 'relative_humidity': 62.5, 'time': '2017-04-08T20:10:12Z', 'depth': -99999.0, 'latitude': 11.3, 'longitude': -60.5}
http://localhost:30801/1.0/query_data_doms?startIndex=100000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 100000 -- total: 168250 -- current_count: 20000 -- duration: 47.147783
first_item: {'air_temperature': 5.3, 'relative_humidity': 70.1, 'time': '2017-04-26T11:00:00Z', 'depth': -99999.0, 'latitude': 53.3, 'longitude': 2.0}
http://localhost:30801/1.0/query_data_doms?startIndex=120000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 120000 -- total: 168250 -- current_count: 20000 -- duration: 53.092327
first_item: {'air_temperature': 27.2, 'relative_humidity': 63.6, 'time': '2017-04-01T16:00:00Z', 'depth': -99999.0, 'latitude': 19.8, 'longitude': -70.7}
http://localhost:30801/1.0/query_data_doms?startIndex=140000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 140000 -- total: 168250 -- current_count: 20000 -- duration: 33.10979
first_item: {'air_temperature': 9.0, 'relative_humidity': 87.3, 'time': '2017-04-01T14:00:00Z', 'depth': -99999.0, 'latitude': 57.6, 'longitude': 1.1}
http://localhost:30801/1.0/query_data_doms?startIndex=160000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 160000 -- total: 168250 -- current_count: 8250 -- duration: 27.929617
first_item: {'air_temperature': 23.8, 'relative_humidity': 67.8, 'time': '2017-04-19T02:10:12Z', 'depth': -99999.0, 'latitude': 21.6, 'longitude': -58.6}
http://localhost:30801/1.0/query_data_doms?startIndex=180000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 180000 -- total: 168250 -- current_count: 0 -- duration: 9.532945

Process finished with exit code 0


Connected to pydev debugger (build 201.7223.92)
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=0&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 0 -- total: 168250 -- current_count: 20000 -- duration: 24.769869
first_item: {'air_temperature': 7.8, 'relative_humidity': 91.5, 'time': '2017-04-01T09:00:00Z', 'depth': -99999.0, 'latitude': 61.6, 'longitude': 1.3}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=20000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 20000 -- total: 168250 -- current_count: 20000 -- duration: 10.757908
first_item: {'air_temperature': 23.2, 'relative_humidity': 64.3, 'time': '2017-04-16T19:19:48Z', 'depth': -99999.0, 'latitude': 23.8, 'longitude': -68.4}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=40000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 40000 -- total: 168250 -- current_count: 20000 -- duration: 11.468385
first_item: {'air_temperature': 24.0, 'relative_humidity': 98.2, 'time': '2017-04-05T21:49:48Z', 'depth': -99999.0, 'latitude': 28.8, 'longitude': -86.0}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=60000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 60000 -- total: 168250 -- current_count: 20000 -- duration: 12.194898
first_item: {'air_temperature': 20.4, 'relative_humidity': 82.4, 'time': '2017-04-22T03:10:12Z', 'depth': -99999.0, 'latitude': 31.9, 'longitude': -69.6}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=80000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 80000 -- total: 168250 -- current_count: 20000 -- duration: 13.594509
first_item: {'air_temperature': 4.2, 'relative_humidity': 70.4, 'time': '2017-04-22T13:00:00Z', 'depth': -99999.0, 'latitude': 60.6, 'longitude': 1.6}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=100000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 100000 -- total: 168250 -- current_count: 20000 -- duration: 16.949609
first_item: {'air_temperature': 5.2, 'relative_humidity': 58.4, 'time': '2017-04-16T03:00:00Z', 'depth': -99999.0, 'latitude': 61.1, 'longitude': 1.0}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=120000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 120000 -- total: 168250 -- current_count: 20000 -- duration: 45.506358
first_item: {'air_temperature': 26.6, 'relative_humidity': 61.9, 'time': '2017-04-19T15:19:48Z', 'depth': -99999.0, 'latitude': 19.8, 'longitude': -70.7}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=140000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 140000 -- total: 168250 -- current_count: 20000 -- duration: 57.124638
first_item: {'air_temperature': 14.8, 'relative_humidity': 65.5, 'time': '2017-04-25T01:49:48Z', 'depth': -99999.0, 'latitude': 41.6, 'longitude': -81.8}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=160000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 160000 -- total: 168250 -- current_count: 8250 -- duration: 22.821795
first_item: {'air_temperature': 27.8, 'relative_humidity': 76.5, 'time': '2017-04-15T02:40:12Z', 'depth': -99999.0, 'latitude': 11.3, 'longitude': -60.5}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=180000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-04-01T00:00:00Z&endTime=2017-04-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-04-01T00:00:00Z - 2017-04-30T00:00:00Z -- start_index: 180000 -- total: 168250 -- current_count: 0 -- duration: 3.48374

Process finished with exit code 0

Connected to pydev debugger (build 201.7223.92)
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=0&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-06-01T00:00:00Z&endTime=2017-09-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-06-01T00:00:00Z - 2017-09-30T00:00:00Z -- start_index: 0 -- total: 227787 -- current_count: 20000 -- duration: 11.207761
first_item: {'air_temperature': 14.1, 'relative_humidity': 61.9, 'time': '2017-06-21T12:00:00Z', 'depth': -99999.0, 'latitude': 61.6, 'longitude': 1.3}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=20000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-06-01T00:00:00Z&endTime=2017-09-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-06-01T00:00:00Z - 2017-09-30T00:00:00Z -- start_index: 20000 -- total: 227787 -- current_count: 20000 -- duration: 11.799825
first_item: {'air_temperature': 26.0, 'relative_humidity': 90.9, 'time': '2017-06-16T23:19:48Z', 'depth': -99999.0, 'latitude': 31.9, 'longitude': -69.6}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=40000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-06-01T00:00:00Z&endTime=2017-09-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-06-01T00:00:00Z - 2017-09-30T00:00:00Z -- start_index: 40000 -- total: 227787 -- current_count: 20000 -- duration: 14.555546
first_item: {'air_temperature': 11.3, 'relative_humidity': 97.4, 'time': '2017-06-30T01:00:00Z', 'depth': -99999.0, 'latitude': 46.8, 'longitude': -91.8}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=60000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-06-01T00:00:00Z&endTime=2017-09-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-06-01T00:00:00Z - 2017-09-30T00:00:00Z -- start_index: 60000 -- total: 227787 -- current_count: 20000 -- duration: 18.231606
first_item: {'air_temperature': 28.4, 'relative_humidity': 89.5, 'time': '2017-06-24T09:49:48Z', 'depth': -99999.0, 'latitude': 27.9, 'longitude': -95.4}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=80000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-06-01T00:00:00Z&endTime=2017-09-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-06-01T00:00:00Z - 2017-09-30T00:00:00Z -- start_index: 80000 -- total: 227787 -- current_count: 20000 -- duration: 15.588901
first_item: {'air_temperature': 10.5, 'relative_humidity': 85.1, 'time': '2017-06-07T03:49:48Z', 'depth': -99999.0, 'latitude': 47.3, 'longitude': -88.6}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=100000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-06-01T00:00:00Z&endTime=2017-09-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-06-01T00:00:00Z - 2017-09-30T00:00:00Z -- start_index: 100000 -- total: 227787 -- current_count: 20000 -- duration: 40.102472
first_item: {'air_temperature': 27.3, 'relative_humidity': 76.9, 'time': '2017-06-04T06:49:48Z', 'depth': -99999.0, 'latitude': 14.5, 'longitude': -53.0}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=120000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-06-01T00:00:00Z&endTime=2017-09-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-06-01T00:00:00Z - 2017-09-30T00:00:00Z -- start_index: 120000 -- total: 227787 -- current_count: 20000 -- duration: 54.26978
first_item: {'air_temperature': 28.1, 'relative_humidity': 82.3, 'time': '2017-06-17T01:19:48Z', 'depth': -99999.0, 'latitude': 16.4, 'longitude': -63.2}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=140000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-06-01T00:00:00Z&endTime=2017-09-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-06-01T00:00:00Z - 2017-09-30T00:00:00Z -- start_index: 140000 -- total: 227787 -- current_count: 20000 -- duration: 41.277232
first_item: {'air_temperature': 16.1, 'relative_humidity': 82.9, 'time': '2017-06-07T22:49:48Z', 'depth': -99999.0, 'latitude': 36.6, 'longitude': -74.8}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=160000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-06-01T00:00:00Z&endTime=2017-09-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-06-01T00:00:00Z - 2017-09-30T00:00:00Z -- start_index: 160000 -- total: 227787 -- current_count: 20000 -- duration: 51.041598
first_item: {'air_temperature': 15.4, 'relative_humidity': 76.0, 'time': '2017-06-04T14:00:00Z', 'depth': -99999.0, 'latitude': 45.2, 'longitude': -5.0}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=180000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-06-01T00:00:00Z&endTime=2017-09-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-06-01T00:00:00Z - 2017-09-30T00:00:00Z -- start_index: 180000 -- total: 227787 -- current_count: 20000 -- duration: 43.003454
first_item: {'air_temperature': 15.4, 'relative_humidity': 96.8, 'time': '2017-06-23T17:49:48Z', 'depth': -99999.0, 'latitude': 43.5, 'longitude': -70.1}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=200000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-06-01T00:00:00Z&endTime=2017-09-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
Traceback (most recent call last):
  File "/Applications/PyCharm.app/Contents/plugins/python/helpers/pydev/pydevd.py", line 1438, in _exec
    pydev_imports.execfile(file, globals, locals)  # execute the script
  File "/Applications/PyCharm.app/Contents/plugins/python/helpers/pydev/_pydev_imps/_pydev_execfile.py", line 18, in execfile
    exec(compile(contents+"\n", file, 'exec'), glob, loc)
  File "/Users/wphyo/Projects/access/parquet_test_1/tests/bench_mark/bench_mark.py", line 326, in <module>
    BenchMark().pagination_bench_mark()
  File "/Users/wphyo/Projects/access/parquet_test_1/tests/bench_mark/bench_mark.py", line 233, in pagination_bench_mark
    response = self.__execute_query()
  File "/Users/wphyo/Projects/access/parquet_test_1/tests/bench_mark/func_exec_time_decorator.py", line 12, in decorated_function
    func_result = f(*args, **kwargs)
  File "/Users/wphyo/Projects/access/parquet_test_1/tests/bench_mark/bench_mark.py", line 56, in __execute_query
    raise ValueError(f'wrong status code: {response.status_code}. details: {response.text}')
ValueError: wrong status code: 504. details:



Connected to pydev debugger (build 201.7223.92)
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=0&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-10-01T00:00:00Z&endTime=2017-10-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-10-01T00:00:00Z - 2017-10-30T00:00:00Z -- start_index: 0 -- total: 178348 -- current_count: 20000 -- duration: 7.309901
first_item: {'air_temperature': 5.8, 'relative_humidity': 59.4, 'time': '2017-10-29T09:00:00Z', 'depth': -99999.0, 'latitude': 61.6, 'longitude': 1.3}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=20000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-10-01T00:00:00Z&endTime=2017-10-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-10-01T00:00:00Z - 2017-10-30T00:00:00Z -- start_index: 20000 -- total: 178348 -- current_count: 20000 -- duration: 14.485547
first_item: {'air_temperature': 14.1, 'relative_humidity': 88.9, 'time': '2017-10-08T08:00:00Z', 'depth': -99999.0, 'latitude': 46.8, 'longitude': -91.8}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=40000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-10-01T00:00:00Z&endTime=2017-10-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-10-01T00:00:00Z - 2017-10-30T00:00:00Z -- start_index: 40000 -- total: 178348 -- current_count: 20000 -- duration: 21.060409
first_item: {'air_temperature': 28.2, 'relative_humidity': 85.3, 'time': '2017-10-10T18:49:48Z', 'depth': -99999.0, 'latitude': 29.2, 'longitude': -88.2}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=60000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-10-01T00:00:00Z&endTime=2017-10-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-10-01T00:00:00Z - 2017-10-30T00:00:00Z -- start_index: 60000 -- total: 178348 -- current_count: 20000 -- duration: 18.033041
first_item: {'air_temperature': 13.3, 'relative_humidity': 78.8, 'time': '2017-10-14T16:00:00Z', 'depth': -99999.0, 'latitude': 57.0, 'longitude': 1.9}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=80000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-10-01T00:00:00Z&endTime=2017-10-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-10-01T00:00:00Z - 2017-10-30T00:00:00Z -- start_index: 80000 -- total: 178348 -- current_count: 20000 -- duration: 35.704179
first_item: {'air_temperature': 10.7, 'relative_humidity': 86.3, 'time': '2017-10-25T19:00:00Z', 'depth': -99999.0, 'latitude': 59.7, 'longitude': 1.6}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=100000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-10-01T00:00:00Z&endTime=2017-10-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-10-01T00:00:00Z - 2017-10-30T00:00:00Z -- start_index: 100000 -- total: 178348 -- current_count: 20000 -- duration: 44.254885
first_item: {'air_temperature': 11.3, 'relative_humidity': 70.8, 'time': '2017-10-19T13:00:00Z', 'depth': -99999.0, 'latitude': 61.3, 'longitude': 1.5}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=120000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-10-01T00:00:00Z&endTime=2017-10-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-10-01T00:00:00Z - 2017-10-30T00:00:00Z -- start_index: 120000 -- total: 178348 -- current_count: 20000 -- duration: 46.126414
first_item: {'air_temperature': 4.3, 'relative_humidity': 71.0, 'time': '2017-10-29T05:30:00Z', 'depth': -99999.0, 'latitude': 42.1, 'longitude': -87.7}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=140000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-10-01T00:00:00Z&endTime=2017-10-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-10-01T00:00:00Z - 2017-10-30T00:00:00Z -- start_index: 140000 -- total: 178348 -- current_count: 20000 -- duration: 55.652159
first_item: {'air_temperature': 15.5, 'relative_humidity': 96.8, 'time': '2017-10-15T00:00:00Z', 'depth': -99999.0, 'latitude': 54.1, 'longitude': 14.2}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=160000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-10-01T00:00:00Z&endTime=2017-10-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-10-01T00:00:00Z - 2017-10-30T00:00:00Z -- start_index: 160000 -- total: 178348 -- current_count: 18348 -- duration: 59.856939
first_item: {'air_temperature': 27.9, 'relative_humidity': 83.8, 'time': '2017-10-26T08:10:12Z', 'depth': -99999.0, 'latitude': 16.9, 'longitude': -81.4}
https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu/1.0/query_data_doms?startIndex=180000&itemsPerPage=20000&provider=NCAR&project=ICOADS Release 3.0&platform=41&variable=relative_humidity&columns=air_temperature&minDepth=-99&maxDepth=0&startTime=2017-10-01T00:00:00Z&endTime=2017-10-30T00:00:00Z&bbox=-111,11,111,99
/Users/wphyo/anaconda3/envs/cdms_parquet_3.6/lib/python3.6/site-packages/urllib3-1.26.7-py3.6.egg/urllib3/connectionpool.py:1020: InsecureRequestWarning: Unverified HTTPS request is being made to host 'a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
  InsecureRequestWarning,
time: 2017-10-01T00:00:00Z - 2017-10-30T00:00:00Z -- start_index: 180000 -- total: 178348 -- current_count: 0 -- duration: 3.206068

Process finished with exit code 0

        :return:
        """
        self.__start_time = '2017-10-01T00:00:00Z'
        self.__end_time = '2017-10-30T00:00:00Z'
        self.__start_index = 0
        self.__size = 20000
        response = self.__execute_query()
        print(f'time: {self.__start_time} - {self.__end_time} -- start_index: {self.__start_index} -- total: {response[0]["total"]} -- current_count: {len(response[0]["results"])} -- duration: {response[1]}')
        if len(response[0]["results"]) > 0:
            print(f'first_item: {response[0]["results"][0]}')
        total = response[0]['total']
        while self.__start_index < total:
            self.__start_index += self.__size
            response = self.__execute_query()
            print(f'time: {self.__start_time} - {self.__end_time} -- start_index: {self.__start_index} -- total: {response[0]["total"]} -- current_count: {len(response[0]["results"])} -- duration: {response[1]}')
            if len(response[0]["results"]) > 0:
                print(f'first_item: {response[0]["results"][0]}')
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

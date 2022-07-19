import json
from datetime import datetime
from multiprocessing.context import Process

import requests

from tests.bench_mark.func_exec_time_decorator import func_exec_time_decorator


class InsituProps:
    def __init__(self):
        # self.cdms_domain = 'http://localhost:30801/insitu'
        self.cdms_domain = 'https://doms.jpl.nasa.gov/insitu'
        # self.cdms_domain = 'https://a106a87ec5ba747c5915cc0ec23c149f-881305611.us-west-2.elb.amazonaws.com/insitu'
        self.size = 100
        self.start_index = 0

        self.provider = 'NCAR'
        self.project = 'ICOADS Release 3.0'
        self.platform_code = '30,41,42'

        self.variable = 'relative_humidity'
        self.columns = 'air_temperature'
        self.start_time = '2017-01-01T00:00:00Z'
        self.end_time = '2017-03-30T00:00:00Z'
        self.min_depth = -99
        self.max_depth = 0
        self.min_lat_lon = (-111, 11)
        self.max_lat_lon = (111, 99)

class BenchParallelProcess:
    @func_exec_time_decorator
    def __execute_query_custom_pagination(self, in_situ_props: InsituProps):
        """
        time curl 'https://doms.jpl.nasa.gov/insitu?startIndex=3&itemsPerPage=20&minDepth=-99&variable=relative_humidity&columns=air_temperature&maxDepth=-1&startTime=2019-02-14T00:00:00Z&endTime=2021-02-16T00:00:00Z&platform=3B&bbox=-111,11,111,99'

        :return:
        """
        rest_keyword = 'query_data_doms_custom_pagination'
        get_url = f'{in_situ_props.cdms_domain}/1.0/{rest_keyword}?startIndex={in_situ_props.start_index}&itemsPerPage={in_situ_props.size}' \
                    f'&provider={in_situ_props.provider}' \
                    f'&project={in_situ_props.project}' \
                    f'&platform={in_situ_props.platform_code}' \
                    f'&variable={"" if in_situ_props.variable is None else f"{in_situ_props.variable}"}' \
                    f'&columns={"" if in_situ_props.columns is None else f"{in_situ_props.columns}"}' \
                    f'&minDepth={in_situ_props.min_depth}&maxDepth={in_situ_props.max_depth}' \
                    f'&startTime={in_situ_props.start_time}&endTime={in_situ_props.end_time}' \
                    f'&bbox={in_situ_props.min_lat_lon[1]},{in_situ_props.min_lat_lon[0]},{in_situ_props.max_lat_lon[1]},{in_situ_props.max_lat_lon[0]}'
        # rest_keyword = 'query_data_doms'
        print(get_url)
        response = requests.get(url=get_url, verify=False)
        if response.status_code > 400:
            return {'err_message': f'wrong status code: {response.status_code}. details: {response.text}'}
        return json.loads(response.text)

    def __get_raw_props(self):
        """
https://doms.jpl.nasa.gov/insitu/1.0/query_data_doms_custom_pagination?
startIndex=0&itemsPerPage=1000&
startTime=2018-08-25T09:00:00Z&endTime=2018-10-24T09:00:00Z&
bbox=160.0,-29.884000009000008,172.38330739034632,-25.0&
minDepth=0.0&maxDepth=5.0&
provider=NCAR&project=ICOADS%20Release%203.0&platform=42

        :return:
        """
        insitu_props = InsituProps()
        insitu_props.size = 20000
        insitu_props.start_index = 0

        # insitu_props.provider = 'NCAR'
        # insitu_props.project = 'ICOADS Release 3.0'
        # insitu_props.platform_code = '42'

        insitu_props.provider = 'Florida State University, COAPS'
        insitu_props.project = 'SAMOS'
        insitu_props.platform_code = '30'

        insitu_props.min_depth = -20.0
        insitu_props.max_depth = 10.0

        insitu_props.columns = None
        insitu_props.variable = None

        # insitu_props.min_lat_lon = (-29.884000009000008, 160.0)
        # insitu_props.max_lat_lon = (-25.0, 172.38330739034632)

        insitu_props.min_lat_lon = (20, -100)
        insitu_props.max_lat_lon = (30, -79)

        insitu_props.start_time = '2017-01-01T09:00:00Z'
        insitu_props.end_time = '2017-01-30T09:00:00Z'
        return insitu_props

    def __execute_insitu_query(self, month: str):
        raw_props = self.__get_raw_props()
        raw_props.start_time = f'2017-{month}-01T09:00:00Z'
        raw_props.end_time = f'2017-{month}-02T00:00:00Z'
        response = self.__execute_query_custom_pagination(raw_props)
        if 'err_message' in response[0]:
            print(f'error: {response[0]["err_message"]} -- duration: {response[1]} -- details: {response[2]}')
            return
        print(f'time: {raw_props.start_time} - {raw_props.end_time} -- total: {response[0]["total"]} -- current_count: {len(response[0]["results"])} -- duration: {response[1]} -- details: {response[2]}')
        return

    def parallel_test(self):
        print(datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
        print(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
        consumers = []
        for each_month in range(3, 12):
            bg_process = Process(target=self.__execute_insitu_query, args=(f'{each_month:02}', ))
            bg_process.daemon = True
            consumers.append(bg_process)
        for c in consumers:
            c.start()
        for c in consumers:
            c.join()
        return


if __name__ == '__main__':
    BenchParallelProcess().parallel_test()
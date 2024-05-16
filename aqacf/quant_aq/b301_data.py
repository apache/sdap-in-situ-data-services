#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  Copyright 2024, by the California Institute of Technology. ALL RIGHTS RESERVED.
#  United States Government Sponsorship acknowledged. Any commercial use must be
#  negotiated with the Office of Technology Transfer at the California Institute of
#  Technology.  This software is subject to U.S. export control laws and regulations
#  and has been classified as EAR99.  By accepting this software, the user agrees to
#  comply with all applicable U.S. export laws and regulations.  User has the
#  responsibility to obtain export licenses, or other export authority as may be
#  required before exporting such information to foreign countries or providing
#  access to foreign persons.
#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""
Go to this page and log in (username: jpl_aqacf password: Jpl$aqacf??#): https://app.quant-aq.com/auth/sign-in


timestamp,           id,      timestamp_local,     sn,       rh,  temp,   lat,   lon,       device_state,pm1,pm25,pm10,co,no,no2,o3,co2,pm1_model_id,pm25_model_id,pm10_model_id,co_model_id,no_model_id,no2_model_id,o3_model_id,co2_model_id
2023-09-05T21:05:03Z,60291403,2023-09-05T14:05:03Z,MOD-00106,37.8,32.8,34.199383,-118.17325,ACTIVE,      15.544,17.827,25.004,370.59,2.246,10.665,46.349,584.946,7379,7380,7381,10045,10046,10049,10050,10052
"""
import json

import pandas as pd
from pandas import DataFrame


class B301Transformer:
    def raw(self, filepath):
        df = pd.read_csv(filepath)
        df['Temperature'] = df['temp']
        df['time'] = df['timestamp']
        df['pm2_5'] = df['pm25']
        df['RelativeHumidity'] = df['rh']
        df['site_id'] = 'B301'
        df['latitude'] = df['lat']
        df['longitude'] = df['lon']
        df['site name'] = 'JPL Building 301'
        removing_columns = ['timestamp', 'temp', 'rh', 'pm25', 'id', 'timestamp_local', 'sn', 'lat', 'lon',
                            'device_state', 'pm1_model_id', 'pm25_model_id', 'pm10_model_id', 'co_model_id',
                            'no_model_id', 'no2_model_id', 'o3_model_id', 'co2_model_id']
        remaining_columns = ['pm1', 'pm10', 'co', 'no', 'no2', 'o3', 'co2']
        df.drop(removing_columns, axis=1, inplace=True)
        df['time'] = pd.to_datetime(df['time'])
        df['time'] = df['time'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        site_json = {
            "project": "AQIC-Raw",
            "provider": "QuantAQ",
            "observations": [row.dropna().to_dict() for index, row in df.iterrows()]
        }
        for each_obs in site_json['observations']:
            each_obs['platform'] = {
                "id": each_obs['site_id'],
                "short_name": each_obs['site name'],
            }
            each_obs.pop('site_id')
            each_obs.pop('site name')
        with open(f'quant_aq_raw-{df["time"].min()}-{df["time"].max()}.json', 'w') as f:
            f.write(json.dumps(site_json))
        return

    def start(self, filepath):
        raise ValueError('Check Units First')
        measurement_param_dict = {
            'OZONE': 'o3',
            'CO': 'co',
            'NO': 'no',
            'NO2': 'no2',
            'PM2.5': 'pm2_5',
        }
        df = pd.read_csv(filepath)
        df['Temperature'] = df['temp']
        df['time'] = df['timestamp']
        df['pm2_5'] = df['pm25']
        df['RelativeHumidity'] = df['rh']
        lat, lon, site_id = df.loc[0]['lat'], df.loc[0]['lon'], df.loc[0]['id']

        # "platform": "B301",
        # "platform_short_name": "JPL Building 301",
        #
        removing_columns = ['timestamp', 'temp', 'rh', 'pm25', 'id', 'timestamp_local', 'sn', 'lat', 'lon', 'device_state', 'pm1_model_id', 'pm25_model_id', 'pm10_model_id', 'co_model_id', 'no_model_id', 'no2_model_id', 'o3_model_id', 'co2_model_id']
        remaining_columns = ['pm1', 'pm10', 'co', 'no', 'no2', 'o3', 'co2']
        df.drop(removing_columns, axis=1, inplace=True)
        df['time'] = pd.to_datetime(df['time'])

        df.set_index('time', inplace=True)
        avgs = df.resample('D').mean(numeric_only=True)
        # avgs.fillna('', inplace=True)
        # avgs.dropna(inplace=True)
        avgs.reset_index(inplace=True)
        avgs.time = avgs.time.dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        # avgs.drop(['time'], axis=1, inplace=True)
        avgs['site_id'] = 'B301'
        avgs['site name'] = 'JPL Building 301'
        avgs['latitude'] = lat
        avgs['longitude'] = lon
        print(avgs.columns)
        site_json = {
            "project": "air_quality",
            "provider": "QuantAQ",
            "observations": [row.dropna().to_dict() for index, row in avgs.iterrows()]
        }
        print(avgs['time'].min())
        print(avgs['time'].max())
        for each_obs in site_json['observations']:
            each_obs['platform'] = {
                "id": each_obs['site_id'],
                "short_name": each_obs['site name'],
            }
            each_obs.pop('site_id')
            each_obs.pop('site name')
        with open(f'quant_aq_daily-{avgs["time"].min()}-{avgs["time"].max()}.json', 'w') as f:
            f.write(json.dumps(site_json))
        hourly = df.resample('60min').mean(numeric_only=True)
        # hourly.dropna(inplace=True)
        # hourly.fillna(-99999, inplace=True)
        hourly.reset_index(inplace=True)
        hourly.time = hourly.time.dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        # hourly.drop(['time'], axis=1, inplace=True)
        hourly['site_id'] = 'B301'
        hourly['site name'] = 'JPL Building 301'
        hourly['latitude'] = lat
        hourly['longitude'] = lon
        print(hourly.columns)

        site_json = {
            "project": "air_quality",
            "provider": "QuantAQ-Hourly",  # NOTE: previously, "Hourly is part of project. It can be there too.
            "observations": [row.dropna().to_dict() for index, row in hourly.iterrows()]
        }
        for each_obs in site_json['observations']:
            each_obs['platform'] = {
                "id": each_obs['site_id'],
                "short_name": each_obs['site name'],
            }
            each_obs.pop('site_id')
            each_obs.pop('site name')
        with open(f'quant_aq_hourly-{hourly["time"].min()}-{hourly["time"].max()}.json', 'w') as f:
            f.write(json.dumps(site_json))
        debugg = 1
        return

B301Transformer().start('MOD-00106-d65ce52bfbb349e8980d3b81da2460a7.csv')
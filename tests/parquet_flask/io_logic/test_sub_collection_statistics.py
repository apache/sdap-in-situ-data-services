import json
from unittest import TestCase

from parquet_flask.aws.es_abstract import ESAbstract
from parquet_flask.aws.es_factory import ESFactory
from parquet_flask.insitu.file_structure_setting import FileStructureSetting

from parquet_flask.io_logic.sub_collection_statistics import SubCollectionStatistics
from parquet_flask.utils.file_utils import FileUtils


class TestSubCollectionStatistics(TestCase):
    def test_01(self):
        insitu_schema = FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/in_situ_schema.json')
        file_structure_setting = FileStructureSetting(insitu_schema, FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/insitu.file.structure.config.json'))
        es_url = 'https://search-insitu-parquet-dev-1-vgwt2bx23o5w3gpnq4afftmvaq.us-west-2.es.amazonaws.com'
        aws_es: ESAbstract = ESFactory().get_instance('AWS', index='', base_url=es_url, port=443)
        query_dict = {
            'provider': 'sample_provider',
            'project': 'sample_project',
            'platform': '1,2,3,4',
            'startTime': '2023-01-01T00:00:00Z',
            'endTime': '2023-01-01T00:00:00Z',
            'minDepth': '-120.12',
            'maxDepth': '-10.102',
            'bbox': '-100.1, -50.2, 22.3, 2.4',
            'variable': 'a1,a2,a3',
            'columns': 'c1, c2, c3'
        }
        query_dict = {}
        scs = SubCollectionStatistics(aws_es, insitu_schema, query_dict, file_structure_setting)
        dsl = scs.generate_dsl()
        expected_dsl = {
            "size": 0,
            "query": {
                'bool': {
                    'must': []
                }
            },
            "aggs": {
                "provider": {
                    "terms": {
                        "field": 'provider'
                    },
                    "aggs": {
                        "project": {
                            "terms": {"field": 'project'},
                            "aggs": {
                                "platform_code": {
                                    "terms": {"field": 'platform_code'},
                                    "aggs": {
                                            "total": {
                                                "sum": {
                                                    "field": "total"
                                                }
                                            },
                                            "max_datetime": {
                                                "max": {
                                                    "field": "max_datetime"
                                                }
                                            },
                                            "max_depth": {
                                                "max": {
                                                    "field": "max_depth"
                                                }
                                            },
                                            "max_lat": {
                                                "max": {
                                                    "field": "max_lat"
                                                }
                                            },
                                            "max_lon": {
                                                "max": {
                                                    "field": "max_lon"
                                                }
                                            },
                                            "min_datetime": {
                                                "min": {
                                                    "field": "min_datetime"
                                                }
                                            },
                                            "min_depth": {
                                                "min": {
                                                    "field": "min_depth"
                                                }
                                            },
                                            "min_lat": {
                                                "min": {
                                                    "field": "min_lat"
                                                }
                                            },
                                            "min_lon": {
                                                "min": {
                                                    "field": "min_lon"
                                                }
                                            },
                                            "air_pressure": {
                                                "sum": {
                                                    "field": "observation_counts.air_pressure"
                                                }
                                            },
                                            "air_temperature": {
                                                "sum": {
                                                    "field": "observation_counts.air_temperature"
                                                }
                                            },
                                            "dew_point_temperature": {
                                                "sum": {
                                                    "field": "observation_counts.dew_point_temperature"
                                                }
                                            },
                                            "downwelling_longwave_flux_in_air": {
                                                "sum": {
                                                    "field": "observation_counts.downwelling_longwave_flux_in_air"
                                                }
                                            },
                                            "downwelling_longwave_radiance_in_air": {
                                                "sum": {
                                                    "field": "observation_counts.downwelling_longwave_radiance_in_air"
                                                }
                                            },
                                            "downwelling_shortwave_flux_in_air": {
                                                "sum": {
                                                    "field": "observation_counts.downwelling_shortwave_flux_in_air"
                                                }
                                            },
                                            "mass_concentration_of_chlorophyll_in_sea_water": {
                                                "sum": {
                                                    "field": "observation_counts.mass_concentration_of_chlorophyll_in_sea_water"
                                                }
                                            },
                                            "rainfall_rate": {
                                                "sum": {
                                                    "field": "observation_counts.rainfall_rate"
                                                }
                                            },
                                            "relative_humidity": {
                                                "sum": {
                                                    "field": "observation_counts.relative_humidity"
                                                }
                                            },
                                            "sea_surface_salinity": {
                                                "sum": {
                                                    "field": "observation_counts.sea_surface_salinity"
                                                }
                                            },
                                            "sea_surface_skin_temperature": {
                                                "sum": {
                                                    "field": "observation_counts.sea_surface_skin_temperature"
                                                }
                                            },
                                            "sea_surface_subskin_temperature": {
                                                "sum": {
                                                    "field": "observation_counts.sea_surface_subskin_temperature"
                                                }
                                            },
                                            "sea_surface_temperature": {
                                                "sum": {
                                                    "field": "observation_counts.sea_surface_temperature"
                                                }
                                            },
                                            "sea_water_density": {
                                                "sum": {
                                                    "field": "observation_counts.sea_water_density"
                                                }
                                            },
                                            "sea_water_electrical_conductivity": {
                                                "sum": {
                                                    "field": "observation_counts.sea_water_electrical_conductivity"
                                                }
                                            },
                                            "sea_water_practical_salinity": {
                                                "sum": {
                                                    "field": "observation_counts.sea_water_practical_salinity"
                                                }
                                            },
                                            "sea_water_salinity": {
                                                "sum": {
                                                    "field": "observation_counts.sea_water_salinity"
                                                }
                                            },
                                            "sea_water_temperature": {
                                                "sum": {
                                                    "field": "observation_counts.sea_water_temperature"
                                                }
                                            },
                                            "surface_downwelling_photosynthetic_photon_flux_in_air": {
                                                "sum": {
                                                    "field": "observation_counts.surface_downwelling_photosynthetic_photon_flux_in_air"
                                                }
                                            },
                                            "wet_bulb_temperature": {
                                                "sum": {
                                                    "field": "observation_counts.wet_bulb_temperature"
                                                }
                                            },
                                            "wind_speed": {
                                                "sum": {
                                                    "field": "observation_counts.wind_speed"
                                                }
                                            },
                                            "wind_from_direction": {
                                                "sum": {
                                                    "field": "observation_counts.wind_from_direction"
                                                }
                                            },
                                            "wind_to_direction": {
                                                "sum": {
                                                    "field": "observation_counts.wind_to_direction"
                                                }
                                            },
                                            "eastward_wind": {
                                                "sum": {
                                                    "field": "observation_counts.eastward_wind"
                                                }
                                            },
                                            "northward_wind": {
                                                "sum": {
                                                    "field": "observation_counts.northward_wind"
                                                }
                                            },
                                            "meta": {
                                                "sum": {
                                                    "field": "observation_counts.meta"
                                                }
                                            }
                                        }
                                }
                            }
                        }
                    }
                }
            }
        }
        self.assertEqual(json.dumps(dsl, sort_keys=True), json.dumps(expected_dsl, sort_keys=True), f'mismatch dsl vs expected_dsl')
        result = scs.start()
        expected_result = {}
        # TODO abstraction test: need to update expected_result
        self.assertEqual(json.dumps(expected_result, sort_keys=True), json.dumps(result, sort_keys=True), f'mismatch query_dict vs result')
        return

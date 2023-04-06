# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
import tempfile
from unittest import TestCase

from pyspark.sql import SparkSession

lsmd_json_schema = {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Cloud-based Data Match-Up Service In Situ Schema",
  "description": "Schema for in situ data",
  "properties": {
    "MISSION": {
      "description": "",
      "type": "string"
    },
    "SPACECRAFT": {
      "description": "",
      "type": "string"
    },
    "VENUE": {
      "description": "",
      "type": "string"
    },
    "VENUE_NUMBER": {
      "description": "",
      "type": "string"
    },
    "RECORDS": {
      "type": "array",
      "items": {
        "$ref": "#/definitions/observation"
      },
      "minItems": 1
    }
  },
  "definitions": {
    "observation": {
      "description": "Description for each variable is copied from CF standard names table found at https://cfconventions.org/Data/cf-standard-names/77/build/cf-standard-name-table.html",
      "type": "object",
      "additionalProperties": False,
      "properties": {
        "channel": {
          "type": "string",
        },
          "ERT": {
              "type": "string",
              "format": "date-time"
          },
          "SCET": {
              "type": "string",
              "format": "date-time"
          },
          "eu": {
              "type": "number",
          },
          "dn": {
              "type": "number",
          },
      },
      "required": [
        "channel",
        "ERT",
        "SCET",
        "eu",
        "dn"
      ],
      "minProperties": 5
    }
  }
}
lsmd_structure_setting = {
  "data_schema_config": {
    "data_array_key": "RECORDS",
    "data_dict_key": "observation",
    "has_data_quality": False,
    "quality_key_postfix": ""
  },
  "parquet_ingestion_config": {
    "file_metadata_keys": [
        "MISSION",
        "SPACECRAFT",
        "VENUE",
        "VENUE_NUMBER",
    ],
    "time_columns": [
      "SCET"
    ],
    "partitioning_columns": [
        "MISSION", "SPACECRAFT", "VENUE", "VENUE_NUMBER", "channel", "year", "month", "job_id"
    ],
    "non_data_columns": [
    ],
    "derived_columns": {
        "SCET_obj": {
            "original_column": "SCET",
            "updated_type": "time"
        },
        "year": {
            "original_column": "SCET",
            "updated_type": "year"
        },
        "month": {
            "original_column": "SCET",
            "updated_type": "month"
        },
        "MISSION": {
            "original_column": "MISSION",
            "updated_type": "literal"
        },
        "SPACECRAFT": {
            "original_column": "SPACECRAFT",
            "updated_type": "literal"
        },
        "VENUE": {
            "original_column": "VENUE",
            "updated_type": "literal"
        },
        "VENUE_NUMBER": {
            "original_column": "VENUE_NUMBER",
            "updated_type": "literal"
        },
        "job_id": {
            "original_column": "job_id",
            "updated_type": "literal"
        },
    }
  },
  "parquet_file_metadata_extraction_config": [
    {
      "output_name": "depth",
      "column": "depth",
      "stat_type": "minmax",
      "min_excluded": -99999.0
    },
    {
      "output_name": "lat",
      "column": "latitude",
      "stat_type": "minmax"
    },
    {
      "output_name": "lon",
      "column": "longitude",
      "stat_type": "minmax"
    },
    {
      "output_name": "datetime",
      "column": "time_obj",
      "stat_type": "minmax",
      "special_data_type": "timestamp"
    },
    {
      "output_name": "observation_counts",
      "stat_type": "data_type_record_count"
    },
    {
      "output_name": "total",
      "stat_type": "record_count"
    }
  ],
  "query_statistics_instructions_config": {
    "group_by": [
      "provider",
      "project",
      "platform_code"
    ],
    "stats": {
      "min": [
        "min_datetime",
        "min_depth",
        "min_lat",
        "min_lon"
      ],
      "max": [
        "max_datetime",
        "max_depth",
        "max_lat",
        "max_lon"
      ],
      "sum": [
        "total"
      ]
    },
    "include_data_stats": True,
    "data_stats": {
      "is_included": True,
      "stats": "sum",
      "data_prefix": "observation_counts."
    }
  },
  "data_query_config": {
    "input_parameter_transformer_schema": {
    "type": "object",
    "properties": {
      "provider": {
        "type": "string"
      },
      "project": {
        "type": "string"
      },
      "platform": {
        "type": "array",
        "items": {
          "type": "string"
        }
      },
      "startTime": {
        "type": "string"
      },
      "endTime": {
        "type": "string"
      },
      "minDepth": {
        "type": "number"
      },
      "maxDepth": {
        "type": "number"
      },
      "bbox": {
        "type": "array",
        "items": {
          "type": "number"
        }
      },
      "variable": {
        "type": "array",
        "items": {
          "type": "string"
        }
      },
      "columns": {
        "type": "array",
        "items": {
          "type": "string"
        }
      }
    }
  },
    "metadata_search_instruction_config": {
      "provider": {
        "type": "string",
        "dsl_terms": [
          {
            "term": {
              "provider": "repr_value"
            }
          }
        ]
      },
      "project": {
        "type": "string",
        "dsl_terms": [
          {
            "term": {
              "project": "repr_value"
            }
          }
        ]
      },
      "platform": {
        "type": "string",
        "dsl_terms": [
          {
            "term": {
              "platform_code": "repr_value"
            }
          }
        ]
      },
      "minDepth": {
        "type": "float",
        "dsl_terms": [
          {
            "range": {
              "max_depth": {
                "gte": "repr_value"
              }
            }
          }
        ]
      },
      "maxDepth": {
        "type": "float",
        "dsl_terms": [
          {
            "range": {
              "min_depth": {
                "lte": "repr_value"
              }
            }
          }
        ]
      },
      "startTime": {
        "type": "datetime",
        "dsl_terms": [
          {
            "range": {
              "max_datetime": {
                "gte": "repr_value"
              }
            }
          }
        ]
      },
      "endTime": {
        "type": "datetime",
        "dsl_terms": [
          {
            "range": {
              "min_datetime": {
                "lte": "repr_value"
              }
            }
          }
        ]
      },
      "bbox": [
        {
          "type": "float",
          "dsl_terms": [
            {
              "range": {
                "max_lon": {
                  "gte": "repr_value"
                }
              }
            }
          ]
        },
        {
          "type": "float",
          "dsl_terms": [
            {
              "range": {
                "max_lat": {
                  "gte": "repr_value"
                }
              }
            }
          ]
        },
        {
          "type": "float",
          "dsl_terms": [
            {
              "range": {
                "min_lon": {
                  "lte": "repr_value"
                }
              }
            }
          ]
        },
        {
          "type": "float",
          "dsl_terms": [
            {
              "range": {
                "min_lat": {
                  "lte": "repr_value"
                }
              }
            }
          ]
        }
      ]
    },
    "sort_mechanism_config": {
      "sorting_columns": [
        "time_obj",
        "platform_code",
        "depth",
        "latitude",
        "longitude"
      ],
      "page_size_key": "itemsPerPage",
      "pagination_marker_key": "markerPlatform",
      "pagination_marker_time": "markerTime",
      "original_time": "startTime"
    },
    "column_filters_config": {
      "removing_columns": [
        "time_obj",
        "month",
        "year",
        "geo_spatial_interval"
      ],
      "default_columns": [
        "time",
        "latitude",
        "longitude",
        "depth"
      ],
      "mandatory_column_filter_key": "columns",
      "additional_column_filter_key": "variable"
    },
    "parquet_conditions_config": {
    "startTime": {
      "relationship": "1:1",
      "terms": {
        "constraint": "binary",
        "type": "string",
        "data_column": "time_obj",
        "comparator": "gte"
      }
    },
    "endTime": {
      "relationship": "1:1",
      "terms": {
        "constraint": "binary",
        "type": "string",
        "data_column": "time_obj",
        "comparator": "lte"
      }
    },
    "bbox": {
      "relationship": "n:n",
      "condition": "must",
      "terms": [
        {
          "constraint": "binary",
          "type": "float",
          "data_column": "longitude",
          "comparator": "gte"
        },
        {
          "constraint": "binary",
          "type": "float",
          "data_column": "latitude",
          "comparator": "gte"
        },
        {
          "constraint": "binary",
          "type": "float",
          "data_column": "longitude",
          "comparator": "lte"
        },
        {
          "constraint": "binary",
          "type": "float",
          "data_column": "latitude",
          "comparator": "lte"
        }
      ]
    },
    "minDepth": {
      "relationship": "1:n",
      "condition": "should",
      "terms": [
        {
          "constraint": "binary",
          "type": "float",
          "data_column": "depth",
          "comparator": "gte"
        },
        {
          "constraint": "binary_constant",
          "type": "float",
          "data_column": "depth",
          "comparator": "eq",
          "constant": -99999.0
        }
      ]
    },
    "maxDepth": {
      "relationship": "1:1",
      "terms": {
        "constraint": "binary",
        "type": "float",
        "data_column": "depth",
        "comparator": "lte"
      }
    },
    "variable": {
      "relationship": "n:1",
      "condition": "should",
      "terms": {
        "constraint": "unary",
        "comparator": "includes"
      }
    }
  },
    "statics_es_index_schema": {
      "settings": {
        "number_of_shards": 3,
        "number_of_replicas": 1
      },
      "mappings": {
        "properties": {
          "min_datetime": {
            "type": "double"
          },
          "max_datetime": {
            "type": "double"
          },
          "min_lat": {
            "type": "double"
          },
          "max_lat": {
            "type": "double"
          },
          "min_lon": {
            "type": "double"
          },
          "max_lon": {
            "type": "double"
          },
          "platform_code": {
            "type": "keyword"
          },
          "s3_url": {
            "type": "keyword"
          },
          "bucket": {
            "type": "keyword"
          },
          "geo_spatial_interval": {
            "type": "keyword"
          },
          "month": {
            "type": "keyword"
          },
          "name": {
            "type": "keyword"
          },
          "project": {
            "type": "keyword"
          },
          "provider": {
            "type": "keyword"
          },
          "year": {
            "type": "keyword"
          },
          "total": {
            "type": "long"
          },
          "min_depth": {
            "type": "double"
          },
          "max_depth": {
            "type": "double"
          }
        }
      }
    }
  }
}
lsmd_sample_data = {
    'MISSION': 'Sample1',
    'SPACECRAFT': 'Sample1',
    'VENUE': 'ATLO',
    'VENUE_NUMBER': '001',
    'RECORDS': [
        {
            'channel': 'A-0001',
            'ERT': '2021-01-01T00:00:00Z',
            'SCET': '2021-01-01T00:00:00Z',
            'eu': 0.001,
            'dn': 0.001,
        },
        {
            'channel': 'A-0001',
            'ERT': '2021-01-01T00:00:01Z',
            'SCET': '2021-01-01T00:00:01Z',
            'eu': 0.002,
            'dn': 0.002,
        },
        {
            'channel': 'A-0002',
            'ERT': '2021-01-01T00:00:00Z',
            'SCET': '2021-01-01T00:00:00Z',
            'eu': 0.001,
            'dn': 0.001,
        },

    ]
}

class TestGeneralUtilsV3(TestCase):
    def test_get_geospatial_interval(self):
        os.environ['geospatial_interval_by_project'] = json.dumps({
            "ICOADS Release 3.0": 100,
            "SAMOS": "50",
            "t1": "7.5",
            "SPURS": "75"
        })
        from parquet_flask.io_logic.ingest_new_file import get_geospatial_interval, IngestNewJsonFile
        self.assertEqual(get_geospatial_interval('SAMOS'), 50, 'wrong for SAMOS')
        self.assertEqual(get_geospatial_interval('SPURS'), 75, 'wrong for SPURS')
        self.assertEqual(get_geospatial_interval('ICOADS Release 3.0'), 100, 'wrong for ICOADS Release 3.0')
        self.assertEqual(get_geospatial_interval('t1'), 30, 'wrong for t1')
        return

    def test_ingest_01(self):
        os.environ['es_url'] = ''
        os.environ['master_spark_url'] = ''
        os.environ['spark_app_name'] = ''
        os.environ['in_situ_schema'] = '/Users/wphyo/Projects/access/parquet_test_1/in_situ_schema.json'
        os.environ['authentication_type'] = ''
        os.environ['authentication_key'] = ''
        os.environ['parquet_metadata_tbl'] = ''
        os.environ['file_structure_setting'] = '/Users/wphyo/Projects/access/parquet_test_1/insitu.file.structure.config.json'
        mock_data = {
            "project": "Sample-Project",
            "provider": "Sample-Provider",
            "observations": [
                {
                    "time": "2017-01-08T00:00:00Z",
                    "latitude": 13.4255,
                    "longitude": 144.6627,
                    "depth": -99999.0,
                    "platform": {
                        "code": "30"
                    },
                    "meta": "KAOU_20170108v20001_0000",
                    "sea_water_salinity": 0.168,
                    "sea_water_salinity_quality": 1,
                    "sea_water_temperature": 21.88,
                    "sea_water_temperature_quality": 3
                },
                {
                    "time": "2017-01-08T00:00:00Z",
                    "latitude": 13.4255,
                    "longitude": 144.6627,
                    "depth": -17.1,
                    "platform": {
                        "code": "30"
                    },
                    "meta": "KAOU_20170108v20001_0000",
                    "wind_from_direction": 74.9,
                    "wind_from_direction_quality": 1,
                    "air_pressure": 1008.1,
                    "air_pressure_quality": 1,
                    "relative_humidity": 66.6,
                    "relative_humidity_quality": 1,
                    "wind_speed": 7.8,
                    "wind_speed_quality": 1,
                    "eastward_wind": -7.5,
                    "northward_wind": -2.0,
                    "wind_component_quality": 1,
                    "air_temperature": 27.92,
                    "air_temperature_quality": 1
                },
                {
                    "time": "2017-01-08T00:00:00Z",
                    "latitude": 13.4255,
                    "longitude": 144.6627,
                    "depth": -20.1,
                    "platform": {
                        "code": "30"
                    },
                    "meta": "KAOU_20170108v20001_0000",
                    "downwelling_longwave_flux_in_air": 415.71,
                    "downwelling_longwave_flux_in_air_quality": 1,
                    "surface_downwelling_photosynthetic_photon_flux_in_air": 2838.66,
                    "surface_downwelling_photosynthetic_photon_flux_in_air_quality": 4,
                    "downwelling_shortwave_flux_in_air": 566.2,
                    "downwelling_shortwave_flux_in_air_quality": 1
                },
                {
                    "time": "2017-01-08T00:01:00Z",
                    "latitude": 13.4255,
                    "longitude": 144.6627,
                    "depth": -99999.0,
                    "platform": {
                        "code": "30"
                    },
                    "meta": "KAOU_20170108v20001_0001",
                    "sea_water_salinity": 0.168,
                    "sea_water_salinity_quality": 1,
                    "sea_water_temperature": 21.88,
                    "sea_water_temperature_quality": 3
                },
                {
                    "time": "2017-01-08T00:01:00Z",
                    "latitude": 13.4255,
                    "longitude": 144.6627,
                    "depth": -17.1,
                    "platform": {
                        "code": "32"
                    },
                    "meta": "KAOU_20170108v20001_0001",
                    "wind_from_direction": 71.2,
                    "wind_from_direction_quality": 1,
                    "air_pressure": 1008.09,
                    "air_pressure_quality": 1,
                    "relative_humidity": 66.9,
                    "relative_humidity_quality": 1,
                    "wind_speed": 7.6,
                    "wind_speed_quality": 1,
                    "eastward_wind": -7.2,
                    "northward_wind": -2.4,
                    "wind_component_quality": 1,
                    "air_temperature": 27.96,
                    "air_temperature_quality": 1
                },
                {
                    "time": "2017-01-08T00:01:00Z",
                    "latitude": 13.4255,
                    "longitude": 144.6627,
                    "depth": -20.1,
                    "platform": {
                        "code": "31"
                    },
                    "meta": "KAOU_20170108v20001_0001",
                    "downwelling_longwave_flux_in_air": 416.22,
                    "downwelling_longwave_flux_in_air_quality": 1,
                    "surface_downwelling_photosynthetic_photon_flux_in_air": 2740.37,
                    "surface_downwelling_photosynthetic_photon_flux_in_air_quality": 4,
                    "downwelling_shortwave_flux_in_air": 555.61,
                    "downwelling_shortwave_flux_in_air_quality": 1
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            os.environ['parquet_file_name'] = os.path.join(tmp_dir_name, 'parquet')
            data_file = os.path.join(tmp_dir_name, 'sample-file.json')
            with open(data_file, 'w') as ff:
                ff.write(json.dumps(mock_data))
            spark = SparkSession.builder.getOrCreate()
            from parquet_flask.io_logic.ingest_new_file import get_geospatial_interval, IngestNewJsonFile
            ingest_file = IngestNewJsonFile(False)
            ingest_file.ingest(data_file, 'test-id', spark)
            print('say hi')
        return

    def test_ingest_lsmd_01(self):
        os.environ['es_url'] = ''
        os.environ['master_spark_url'] = ''
        os.environ['spark_app_name'] = ''
        os.environ['in_situ_schema'] = 'set it in temp dir'
        os.environ['file_structure_setting'] = 'set it in temp dir'
        os.environ['authentication_type'] = ''
        os.environ['authentication_key'] = ''
        os.environ['parquet_metadata_tbl'] = ''
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            os.environ['in_situ_schema'] = os.path.join(tmp_dir_name, 'in_situ_schema.json')
            os.environ['file_structure_setting'] = os.path.join(tmp_dir_name, 'insitu.file.structure.config.json')
            os.environ['parquet_file_name'] = os.path.join(tmp_dir_name, 'parquet')
            with open(os.environ.get('in_situ_schema'), 'w') as ff:
                ff.write(json.dumps(lsmd_json_schema))
            with open(os.environ.get('file_structure_setting'), 'w') as ff:
                ff.write(json.dumps(lsmd_structure_setting))
            data_file = os.path.join(tmp_dir_name, 'sample-file.json')
            with open(data_file, 'w') as ff:
                ff.write(json.dumps(lsmd_sample_data))
            spark = SparkSession.builder.getOrCreate()
            from parquet_flask.io_logic.ingest_new_file import IngestNewJsonFile
            ingest_file = IngestNewJsonFile(False)
            ingest_file.ingest(data_file, 'test-id', spark)
            print('say hi')
        return

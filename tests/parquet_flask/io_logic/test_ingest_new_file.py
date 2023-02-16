import json
import os
from unittest import TestCase

from parquet_flask.io_logic.ingest_new_file import get_geospatial_interval


class TestGeneralUtilsV3(TestCase):
    def test_get_geospatial_interval(self):
        os.environ['geospatial_interval_by_project'] = json.dumps({
            "ICOADS Release 3.0": 100,
            "SAMOS": "50",
            "t1": "7.5",
            "SPURS": "75"
        })
        self.assertEqual(get_geospatial_interval('SAMOS'), 50, 'wrong for SAMOS')
        self.assertEqual(get_geospatial_interval('SPURS'), 75, 'wrong for SPURS')
        self.assertEqual(get_geospatial_interval('ICOADS Release 3.0'), 100, 'wrong for ICOADS Release 3.0')
        self.assertEqual(get_geospatial_interval('t1'), 30, 'wrong for t1')
        return

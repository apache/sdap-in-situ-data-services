import unittest

from parquet_flask.utils.spatial_utils import SpatialUtils


class TestSpatialUtils(unittest.TestCase):
    def test_generate_lat_lon_intervals_01(self):
        result = SpatialUtils.generate_lat_lon_intervals((-2.3, -4.7789), (21.39987, 25.00000), 5)
        expected_result = [(each_lat, each_lon) for each_lon in range(-5, 26, 5) for each_lat in range(-5, 22, 5)]
        self.assertEqual(expected_result, result, 'wrong output')
        return

    def test_generate_lat_lon_intervals_02(self):
        result = SpatialUtils.generate_lat_lon_intervals((-2.3, -4.7789), (0.0, 0.0), 5)
        expected_result = [(-5, -5), (0, -5), (-5, 0), (0, 0)]
        self.assertEqual(expected_result, result, 'wrong output')
        return

    def test_generate_lat_lon_intervals_03(self):
        result = SpatialUtils.generate_lat_lon_intervals((0, 0), (2, 2), 5)
        expected_result = [(0, 0)]
        self.assertEqual(expected_result, result, 'wrong output')
        return

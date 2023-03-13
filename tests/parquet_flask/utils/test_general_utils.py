import unittest

from parquet_flask.utils.general_utils import GeneralUtils


class TestGeneralUtils(unittest.TestCase):
    def test_is_float(self):
        self.assertTrue(GeneralUtils.is_float('1.2', False), f'wrong result for 1.2')
        self.assertTrue(GeneralUtils.is_float('-1.2', False), f'wrong result for -1.2')
        self.assertTrue(GeneralUtils.is_float('0', False), f'wrong result for 0')
        self.assertTrue(GeneralUtils.is_float('-0', False), f'wrong result for -0')
        self.assertTrue(GeneralUtils.is_float('-0.00000001', False), f'wrong result for -0.00000001')
        self.assertFalse(GeneralUtils.is_float('nan', False), f'wrong result for nan')
        self.assertTrue(GeneralUtils.is_float('nan', True), f'wrong result for nan. accept nan')
        self.assertFalse(GeneralUtils.is_float('0.23a', False), f'wrong result for 0.23a')
        self.assertFalse(GeneralUtils.is_float('0.23^5', False), f'wrong result for 0.23^5')
        return

    def test_gen_float_list_from_comma_sep_str(self):
        self.assertEqual(GeneralUtils.gen_float_list_from_comma_sep_str('-99.34, 23, 44.8765, 34', 4), [-99.34, 23, 44.8765, 34])
        self.assertEqual(GeneralUtils.gen_float_list_from_comma_sep_str('-99.34, 23, 44.8765, 34.987654321234567', 4), [-99.34, 23, 44.8765, 34.987654321234567])
        self.assertEqual(GeneralUtils.gen_float_list_from_comma_sep_str('0, 44.8765', 2), [0, 44.8765])
        self.assertRaises(ValueError, GeneralUtils.gen_float_list_from_comma_sep_str, '-99.34, 23, 44.8765e, 34', 4)
        self.assertRaises(ValueError, GeneralUtils.gen_float_list_from_comma_sep_str, '-99.34, 23, 44.8765, ', 4)
        self.assertRaises(ValueError, GeneralUtils.gen_float_list_from_comma_sep_str, '-99.34, 23', 3)
        return

    def test_floor_lat_long(self):
        self.assertEqual(GeneralUtils.floor_lat_long(-3, 5), '-5_5')
        self.assertEqual(GeneralUtils.floor_lat_long(0, -0), '0_0')
        self.assertEqual(GeneralUtils.floor_lat_long(22, -0), '20_0')
        self.assertEqual(GeneralUtils.floor_lat_long(-2, 4, 3), '-3_3')
        return

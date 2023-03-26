from unittest import TestCase

from parquet_flask.insitu.file_structure_setting import FileStructureSetting

from parquet_flask.utils.file_utils import FileUtils


class TestFileStructureSetting(TestCase):
    def test_01(self):
        data_json_schema = FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/in_situ_schema.json')
        structure_config = FileUtils.read_json('/Users/wphyo/Projects/access/parquet_test_1/insitu.file.structure.config.json')
        file_struct_setting = FileStructureSetting(data_json_schema=data_json_schema, structure_config=structure_config)
        data_columns = file_struct_setting.get_data_columns()
        expected_data_columns = [
            "meta",  # TODO abstraction : is included which might not be backward compatible.
            "air_pressure",
            "air_temperature",
            "dew_point_temperature",
            "downwelling_longwave_flux_in_air",
            "downwelling_longwave_radiance_in_air",
            "downwelling_shortwave_flux_in_air",
            "mass_concentration_of_chlorophyll_in_sea_water",
            "rainfall_rate",
            "relative_humidity",
            "sea_surface_salinity",
            "sea_surface_skin_temperature",
            "sea_surface_subskin_temperature",
            "sea_surface_temperature",
            "sea_water_density",
            "sea_water_electrical_conductivity",
            "sea_water_practical_salinity",
            "sea_water_salinity",
            "sea_water_temperature",
            "surface_downwelling_photosynthetic_photon_flux_in_air",
            "wet_bulb_temperature",
            "wind_speed",
            "wind_from_direction",
            "wind_to_direction",
            "eastward_wind",
            "northward_wind"
        ]
        self.assertEqual(set(expected_data_columns), set(data_columns), 'wrong data columns')
        return

from pyspark.sql.types import StructType, StructField, DoubleType, StringType, MapType, LongType, TimestampType, \
    IntegerType


class CdmsSchema:
    ALL_SCHEMA = StructType([
        StructField('depth', DoubleType(), True),
        StructField('latitude', DoubleType(), True),
        StructField('longitude', DoubleType(), True),
        StructField('meta', StringType(), True),
        StructField('platform', MapType(StringType(), StringType()), True),

        StructField('time', StringType(), True),
        StructField('time_obj', TimestampType(), True),

        StructField('provider', StringType(), True),
        StructField('project', StringType(), True),
        StructField('platform_code', IntegerType(), True),
        StructField('year', IntegerType(), True),
        StructField('month', IntegerType(), True),
        StructField('job_id', StringType(), True),

        StructField('air_pressure', DoubleType(), True),
        StructField('air_pressure_quality', LongType(), True),

        StructField('air_temperature', DoubleType(), True),
        StructField('air_temperature_quality', LongType(), True),

        StructField('dew_point_temperature', DoubleType(), True),
        StructField('dew_point_temperature_quality', LongType(), True),

        StructField('downwelling_longwave_flux_in_air', DoubleType(), True),
        StructField('downwelling_longwave_flux_in_air_quality', LongType(), True),

        StructField('downwelling_longwave_radiance_in_air', DoubleType(), True),
        StructField('downwelling_longwave_radiance_in_air_quality', LongType(), True),

        StructField('downwelling_shortwave_flux_in_air', DoubleType(), True),
        StructField('downwelling_shortwave_flux_in_air_quality', LongType(), True),

        StructField('mass_concentration_of_chlorophyll_in_sea_water', DoubleType(), True),
        StructField('mass_concentration_of_chlorophyll_in_sea_water_quality', LongType(), True),

        StructField('rainfall_rate', DoubleType(), True),
        StructField('rainfall_rate_quality', LongType(), True),

        StructField('relative_humidity', DoubleType(), True),
        StructField('relative_humidity_quality', LongType(), True),

        StructField('sea_surface_salinity', DoubleType(), True),
        StructField('sea_surface_salinity_quality', LongType(), True),

        StructField('sea_surface_skin_temperature', DoubleType(), True),
        StructField('sea_surface_skin_temperature_quality', LongType(), True),

        StructField('sea_surface_subskin_temperature', DoubleType(), True),
        StructField('sea_surface_subskin_temperature_quality', LongType(), True),

        StructField('sea_surface_temperature', DoubleType(), True),
        StructField('sea_surface_temperature_quality', LongType(), True),

        StructField('sea_water_density', DoubleType(), True),
        StructField('sea_water_density_quality', LongType(), True),

        StructField('sea_water_electrical_conductivity', DoubleType(), True),
        StructField('sea_water_electrical_conductivity_quality', LongType(), True),

        StructField('sea_water_practical_salinity', DoubleType(), True),
        StructField('sea_water_practical_salinity_quality', LongType(), True),

        StructField('sea_water_salinity', DoubleType(), True),
        StructField('sea_water_salinity_quality', LongType(), True),

        StructField('sea_water_temperature', DoubleType(), True),
        StructField('sea_water_temperature_quality', LongType(), True),

        StructField('surface_downwelling_photosynthetic_photon_flux_in_air', DoubleType(), True),
        StructField('surface_downwelling_photosynthetic_photon_flux_in_air_quality', LongType(), True),

        StructField('wet_bulb_temperature', DoubleType(), True),
        StructField('wet_bulb_temperature_quality', LongType(), True),

        StructField('wind_speed', DoubleType(), True),
        StructField('wind_speed_quality', LongType(), True),

        StructField('wind_from_direction', DoubleType(), True),
        StructField('wind_from_direction_quality', LongType(), True),

        StructField('wind_to_direction', DoubleType(), True),
        StructField('wind_to_direction_quality', LongType(), True),

        StructField('eastward_wind', DoubleType(), True),
        StructField('northward_wind', DoubleType(), True),
        StructField('wind_component_quality', LongType(), True),

        StructField('device', StringType(), True),
    ])

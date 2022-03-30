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

        StructField('relative_humidity', DoubleType(), True),
        StructField('relative_humidity_quality', LongType(), True),


        StructField('air_temperature', DoubleType(), True),
        StructField('air_temperature_quality', LongType(), True),

        StructField('eastward_wind', DoubleType(), True),
        StructField('northward_wind', DoubleType(), True),
        StructField('wind_component_quality', LongType(), True),

        StructField('wind_from_direction', DoubleType(), True),
        StructField('wind_from_direction_quality', LongType(), True),

        StructField('wind_speed', DoubleType(), True),
        StructField('wind_speed_quality', LongType(), True),

        StructField('air_pressure', DoubleType(), True),
        StructField('air_pressure_quality', LongType(), True),
    ])

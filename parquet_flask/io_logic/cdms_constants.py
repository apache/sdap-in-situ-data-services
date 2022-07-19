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

class CDMSConstants:
    meta_col = 'meta'
    time_col = 'time'
    provider_col = 'provider'
    project_col = 'project'
    platform_code_col = 'platform_code'
    platform_col = 'platform'
    code_col = 'code'
    job_id_col = 'job_id'
    time_obj_col = 'time_obj'
    year_col = 'year'
    month_col = 'month'
    observations_key = 'observations'
    lat_col = 'latitude'
    lon_col = 'longitude'
    depth_col = 'depth'
    geo_spatial_interval_col = 'geo_spatial_interval'

    s3_url_key = 's3_url'
    uuid_key = 'uuid'
    ingested_date_key = 'ingested_date'
    checksum_key = 'checksum'
    file_size_key = 'file_size'
    records_count_key = 'records_count'
    job_start_key = 'job_start_time'
    job_end_key = 'job_end_time'
    checksum_validation = 'checksum_validation'
    checksum_cause = 'checksum_cause'

    missing_depth_value = -99999
    config_key_flask_prefix = 'flask_prefix'

    es_index_parquet_stats = 'parquet_stats_v1'

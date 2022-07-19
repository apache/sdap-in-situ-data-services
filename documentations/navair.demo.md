## Parquet Data Structure
- existing CDMS structure
```angular2
(base) MT-212437:~ wphyo$ aws --profile saml-pub s3 ls 'cdms-dev-in-situ-parquet/CDMS_insitu.geo2.parquet/provider=Florida State University, COAPS/'
                           PRE project=SAMOS/
(base) MT-212437:~ wphyo$ aws --profile saml-pub s3 ls 'cdms-dev-in-situ-parquet/CDMS_insitu.geo2.parquet/provider=Florida State University, COAPS/project=SAMOS/'
                           PRE platform_code=30/
(base) MT-212437:~ wphyo$ aws --profile saml-pub s3 ls 'cdms-dev-in-situ-parquet/CDMS_insitu.geo2.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/'
                           PRE geo_spatial_interval=-10_-110/
                           PRE geo_spatial_interval=-10_-155/
                           PRE geo_spatial_interval=-10_-25/
                           PRE geo_spatial_interval=-10_-30/
                           PRE geo_spatial_interval=-10_-35/
...
(base) MT-212437:~ wphyo$ aws --profile saml-pub s3 ls 'cdms-dev-in-situ-parquet/CDMS_insitu.geo2.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/geo_spatial_interval=-10_-110/'
                           PRE year=2017/
(base) MT-212437:~ wphyo$ aws --profile saml-pub s3 ls 'cdms-dev-in-situ-parquet/CDMS_insitu.geo2.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/geo_spatial_interval=-10_-110/year=2017/'
                           PRE month=1/
(base) MT-212437:~ wphyo$ aws --profile saml-pub s3 ls 'cdms-dev-in-situ-parquet/CDMS_insitu.geo2.parquet/provider=Florida State University, COAPS/project=SAMOS/platform_code=30/geo_spatial_interval=-10_-110/year=2017/month=1/'
                           PRE job_id=55d5a981-e6cc-49f9-8afb-7fe937ae9819/
                           PRE job_id=59caf20a-811d-4175-a7fd-f261e29a4eea/
```
- partition levels: `<provider>/<project>/<platform>/<geo_hash>/<year>/<month>`
    - provider
    - project
    - platform
    - geo_hash
    - year
    - month
- proposed LSMD / NavAir structure
- `<S3 bucket>/<spacecraft>/<venue>/<venue_number>/<sub_system>/<channel>/<year>/<month>/<data file>`
    - spacecraft: required
    - venue: `OPS` if missing
    - venue_number: `NA` if missing
    - sub_system: `NA` if missing
    - channel: required
    - year: extracted from timestamp
    - month: extracted from timestamp
- data file: (columns)
    - DN: float
    - EU: float
    - SCLK: float
    - ERT: long
    - SCET: long
    - OUT_OF_BOUND: float (NEW)
- Elasticsearch Registry: (CPT + Channel metadata)
    - spacecraft: extracted from ECSV / CPT
    - venue: extracted from ECSV / CPT
    - venue_number: extracted from ECSV / CPT
    - sub_system: extracted from ECSV / CPT
    - channel: extracted from ECSV / CPT
    - channel_type: extracted from ECSV / CPT
        - signed float
        - unsigned float
        - enum
        - text
        - Digital (HEX)
    - dn_unit: extracted from ECSV / CPT
    - eu_unit: extracted from ECSV / CPT
    - out_of_bound_metadata: extracted from ECSV / CPT
    - total_record: computed from parquet
    - min_scet: computed from parquet
    - max_scet: computed from parquet
    - min_ert: computed from parquet
    - max_ert: computed from parquet
    
### Pros
#### Easier Ingestion Process
- input: daily / multiple day ECSV files with X channels
    - we can use AWS batch job to ingest this. or we can use our normal ECS / K8S  job
    - resulting in tiny chunks of parquet data
    - update Elasticsearch registry after each file to update its volatile metadata like time ranges and record counts
- monthly job to combine tiny data into larger chunks of data for better performance

#### No intermediate step to cache the data in EBS for retrieval. 
- Currently, HDF5 is downloaded from S3 to EBS. Then retrieval process is started. 
- There is a DDB to keep track of the cache info, and a simple LRU algorithm is used to delete older files. 
- This is no longer needed.  

#### Maintaining chunk info in DB
- in HDF5, the chunked boundary is maintained so that the query can be faster. 
- That info is stored in DB (Currently Elasticsearch)
- Updating that info whenever it's updated is time consuming. 

### Cons
#### Querying uniformed data is requires more work in Parquet
- Currently, we can retrieve a sample of the subset of data uniformly thanks to the chunk info. 
    - from DB, the number of valid chunks is computed based on the time range. 
    - the number of valid chunks is reduced further if there are a lot of chunks. 
    - a small number of records from each chunk is retrieved from HDF5 to limit the result to a certain number (example: 1000)
- This logic needs to be reworked in Parquet with different approach
    
#### Downloading entire data will take time. 
- Currently, HDF5 file is stored as an object in S3. 
    - if a user wants the entire dataset, it can be retrieved straight away
- In Parquet, HDF5 file need to be generated.
    - It can be cached once generated, but that would mean doubling the storage cost

#### Maintaining Apache Spark / AWS EMR cluster
- Currently, there are no massive servers running since the computation for data retrieval is done on the fly. 
    - Based on the user load, more servers can be spun up if needed
- If Parquet is used, an Apache Spark / EMR cluster needs to be kept running. 
    - Alternative is to use AWS Athena and Glue.
    

 
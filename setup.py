from setuptools import find_packages, setup

# install_requires = [
#     'pyspark===3.1.1',
#     # 'fastparquet===0.5.0',  # not using it. sticking to pyspark with spark cluster according to Nga
#     'findspark===1.4.2',
#     'flask===1.1.2', 'flask_restful===0.3.8', 'flask-restx===0.3.0',  # to create Flask server
#     'gevent===1.4.0', 'greenlet===0.4.16',  # to run flask server
#     'werkzeug===0.16.1',
#     'jsonschema',  # to verify json objects
#     'fastjsonschema===2.15.1',
#     'boto3', 'botocore',
# ]

install_requires = [
    'pyspark===3.1.2',
    # 'fastparquet===0.5.0',  # not using it. sticking to pyspark with spark cluster according to Nga
    'findspark===1.4.2',
    'flask===2.0.1', 'flask_restful===0.3.9', 'flask-restx===0.5.0',  # to create Flask server
    'gevent===21.8.0', 'greenlet===1.1.1',  # to run flask server
    'werkzeug===2.0.1',
    'jsonschema',  # to verify json objects
    'fastjsonschema===2.15.1',
    'boto3', 'botocore',
]

setup(
    name="parquet_ingestion_search",
    version="0.0.1",
    # url="https://github.jpl.nasa.gov/MSLEO/msl-datalytics/wiki",
    packages=find_packages(),
    install_requires=install_requires,
    author=['Wai Phyo',],
    author_email=['wai.phyo@jpl.nasa.gov', 'Matt.D.Lenda@jpl.nasa.gov'],
    python_requires="==3.7",
    license='NONE',
    include_package_data=True,
)

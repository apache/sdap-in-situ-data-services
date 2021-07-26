from setuptools import find_packages, setup

install_requires = [
    'pyspark===3.1.1',
    # 'fastparquet===0.5.0',  # not using it. sticking to pyspark with spark cluster according to Nga
    'findspark===1.4.2',
    'flask===1.1.2', 'flask_restful', 'flask-restx',  # to create Flask server
    'gevent===1.4.0', 'greenlet===0.4.16',  # to run flask server
    'werkzeug===0.16.1',
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
    license='NONE',
    include_package_data=True,
)

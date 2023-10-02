import logging
import os

from aqacf.airnow.airnow_wrapper import AirNowWrapper


# os.environ['LOG_LEVEL'] = '10'
# os.environ['provider'] = 'AirNow'
# os.environ['project'] = 'air_quality'
# os.environ['staging_bucket'] = 'aq-in-situ-data-staging'
# os.environ['year'] = '2017'
if __name__ == '__main__':
    logging.basicConfig(level=int(os.getenv('LOG_LEVEL', '20')),
                        format="%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s")
    required_env = ['provider', 'project', 'staging_bucket', 'year']
    if not all([k in os.environ for k in required_env]):
        raise EnvironmentError(f'one or more missing env: {required_env}')
    AirNowWrapper(os.environ.get('provider'),
                  os.environ.get('project'),
                  os.environ.get('staging_bucket')).start(int(os.environ.get('year')))
# CI/CD is successful.
# https://hub.docker.com/layers/waiphyojpl/cdms.parquet.flask/build-aqacf-3/images/sha256-754d57d785e8a8723e95eaf98aec7c9dee88a25f1899135a0344cf1e2fc04f6c?context=repo
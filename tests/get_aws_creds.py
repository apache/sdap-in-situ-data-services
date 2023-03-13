import configparser

cred_file_path = '/Users/wphyo/.aws/credentials'
SAML_GOV = 'saml-gov'
SAML_PUB = 'saml-pub'


def export_as_env(saml_type = 'saml-pub'):
    config = configparser.ConfigParser()
    config.read(cred_file_path)
    saml_cred = config[saml_type]
    return {
        'aws_access_key_id': saml_cred['aws_access_key_id'],
        'aws_secret_access_key': saml_cred['aws_secret_access_key'],
        'aws_session_token': saml_cred['aws_session_token'] if 'aws_session_token' in saml_cred else '',
    }
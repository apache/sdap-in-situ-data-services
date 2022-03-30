class SparkConstants:
    SIMPLE_CRED = 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'
    TEMP_CRED = 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider'
    CRED_PROVIDER_KEY = 'spark.hadoop.fs.s3a.aws.credentials.provider'

    CRED_ACCESS_KEY = 'spark.hadoop.fs.s3a.access.key'
    CRED_SECRET_KEY = 'spark.hadoop.fs.s3a.secret.key'
    CRED_TOKEN_KEY = 'spark.hadoop.fs.s3a.session.token'

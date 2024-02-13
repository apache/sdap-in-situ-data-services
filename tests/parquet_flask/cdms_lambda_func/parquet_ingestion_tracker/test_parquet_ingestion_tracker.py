import json
import os
from unittest import TestCase

from parquet_flask.cdms_lambda_func.parquet_ingestion_tracker.parquet_ingestion_tracker import ParquetIngestionTracker


class TestParquetIngestionTracker(TestCase):
    def test_01(self):
        os.environ['es_url'] = 'https://search-ideas-api-dev-1-f62xltsguioft2hpjepkrhln3e.us-west-2.es.amazonaws.com'
        os.environ['CDMS_BEARER_TOKEN'] = 'Mock-CDMS-Flask-Token'
        os.environ['CDMS_DOMAIN'] = 'https://ideas-digitaltwin.jpl.nasa.gov/insitu_airnow'
        sns_msg = {
            'Type': 'Notification',
            'MessageId': '0249a7b9-598f-5925-9588-f990c54949eb',
            'TopicArn': 'arn:aws:sns:us-west-2:125113240993:ideas-insitu-ideas_insitu_s3_pipeline',
            'Subject': 'Amazon S3 Notification',
            'Message': '{"s3_url": "s3://aq-in-situ-data-staging/GMU/sample-file-1.txt"}',
            'Timestamp': '2024-01-23T10:52:54.395Z', 'SignatureVersion': '1', 'Signature': 'iMwsdKPUUEMgwqzb0J0ZFO4nwzSl41+swRJwx7Dde8yTGAFqQsTODPHckvumqkY5HjhEiA1+4V4brSZdfinEauzUCdFUr1xsbCh0FqtOWHACASmfZSAs4vBnzEqkJVB6zC7qtHRqzu5Ho7plyE9uYF2oX9yatvr8yHHqnuJT5XZhRhl2uQOZH2dEriyKHNfJdS3DPCf03UMtlAGVDNnxZLgsupllUxOMtJ/FYJp4DB22jrDch5diWaulPK2ZnYm4ppXf3ZQgth2qFlC9y2zjRhjo0uL8xBfDOrMXN123NMSTGA7WXFci6a98VSEntqCqNjEGVnSHcPY72W+LH+vagg==', 'SigningCertURL': 'https://sns.us-west-2.amazonaws.com/SimpleNotificationService-60eadc530605d63b8e62a523676ef735.pem', 'UnsubscribeURL': 'https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:125113240993:ideas-insitu-ideas_insitu_s3_pipeline:c9fdbc99-f2ef-464a-9917-49d4f3ed1cf5'}

        sample_event = {'Records': [
            {'messageId': '245e69f0-5b3e-4507-a78d-69f1a3ba8331',
             'receiptHandle': 'AQEBPlCLDtGCyqsOIcYdFColw9DJPcqm/UD9xgd6mljqObB8woOG5QRNboKFqN6G2NWRfzU3bjv0IerRvBlX8oac1Cny+wV7gpSTiE3/dB7UaEM4aW8gzS6yH03bW+6D4On5x7oDrJKO5oY9KfeyRAaJu1YhjFQVhVS+gvOAyJW27YEbX3CYU6WbYEhOemI/5VLILwaVaj4+gR1mRZnz3TvsXZ+dANLc2NNwv4V3kkOrFekQmkQ/ELXt3hcknXtrfwXAHlYzVBCwjeV8qjHzsi5/A3GKlBJykE35NKNdfH7dAbNGlRSPRZpO2rdQD1+mL5zf2OeNHz8s4XCAlrPNIVFyx+S6wQ1KLrLx85GWN0GUznbZdx1PXxrlB6HAquuY51dQ/eihfbWeutAPCHbc16cWn6H7tYGVte6ATtsCAILtGYkzSAMsIAs8NenlL/f6ddpM',
             'body': json.dumps(sns_msg),
             'attributes': {'ApproximateReceiveCount': '1108', 'SentTimestamp': '1706007174438',
                            'SenderId': 'AIDAIYLAVTDLUXBIEIX46', 'ApproximateFirstReceiveTimestamp': '1706007174447'},
             'messageAttributes': {}, 'md5OfBody': 'cfb75229f351e48eb6e501e47c0f2a37', 'eventSource': 'aws:sqs',
             'eventSourceARN': 'arn:aws:sqs:us-west-2:125113240993:ideas-insitu-ideas_insitu_s3_pipeline',
             'awsRegion': 'us-west-2'}]}
        tracker = ParquetIngestionTracker().start(sample_event)
        return

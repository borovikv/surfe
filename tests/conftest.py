import boto3
import pytest
from moto import mock_aws

print('kaktus')


@pytest.fixture
def bucket():
    bucket_name = 'a-bucket'
    mock = mock_aws()
    try:
        mock.start()
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=bucket_name)
        yield bucket_name
    finally:
        mock.stop()

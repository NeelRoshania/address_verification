import boto3

from celery_template import logger

"""
Notes
    - aws cli: https://awscli.amazonaws.com/v2/documentation/api/latest/index.html
    - client = boto3.client('s3')
    - s3 = boto3.resource('s3')

"""

def get_s3_client(access_key: str, secret_key: str) -> boto3.client:
    return boto3.client(
        's3',
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key 
    )

def get_s3_resource(access_key: str, secret_key: str) -> boto3.resource:
    return boto3.resource(
        's3',
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key 
    )

def get_buckets(s3: boto3.resource) -> list:
    return [bucket for bucket in s3.buckets.all()]

def get_objects(client: boto3.client, bucket: str) -> boto3.client:
    return client.list_objects_v2(
        Bucket=bucket,
        MaxKeys=100
)

def list_buckets(s3: boto3.resource) -> list:
    return [bucket.name for bucket in s3.buckets.all()]
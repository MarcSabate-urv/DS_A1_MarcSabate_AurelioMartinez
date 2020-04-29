import ibm_boto3
import ibm_botocore
from pywren_ibm_cloud.utils import sizeof_fmt


class COSBackend:
    def __init__(self):
        service_endpoint = '___'
        secret_key = '____'
        access_key = '_____'
        client_config = ibm_botocore.client.Config(max_pool_connections=200, user_agent_extra='pywren-ibm-cloud')

        self.cos_client = ibm_boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                                           config=client_config, endpoint_url=service_endpoint)

    def put_object(self, bucket_name, key, data):

        try:
            res = self.cos_client.put_object(Bucket=bucket_name, Key=key, Body=data)
            status = 'OK' if res['ResponseMetadata']['HTTPStatusCode'] == 200 else 'Error'
            try:
                print('PUT Object {} - Size: {} - {}'.format(key, sizeof_fmt(len(data)), status))
            except:
                print('PUT Object {} {}'.format(key, status))
        except ibm_botocore.exceptions.ClientError as e:
            raise e

    def get_object(self, bucket_name, key, stream=False, extra_get_args={}):

        try:
            r = self.cos_client.get_object(Bucket=bucket_name, Key=key, **extra_get_args)
            if stream:
                data = r['Body']
            else:
                data = r['Body'].read()
            return data
        except ibm_botocore.exceptions.ClientError as e:
            raise e

    def head_object(self, bucket_name, key):
        try:
            metadata = self.cos_client.head_object(Bucket=bucket_name, Key=key)
            return metadata['ResponseMetadata']['HTTPHeaders']
        except ibm_botocore.exceptions.ClientError as e:
            raise e

    def delete_object(self, bucket_name, key):
        return self.cos_client.delete_object(Bucket=bucket_name, Key=key)

    def list_objects(self, bucket_name, prefix=None):
        paginator = self.cos_client.get_paginator('list_objects_v2')
        try:
            if (prefix is not None):
                page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            else:
                page_iterator = paginator.paginate(Bucket=bucket_name)

            object_list = []

            for page in page_iterator:
                if 'Contents' in page:
                    for item in page['Contents']:
                        object_list.append(item)
            return object_list

        except ibm_botocore.exceptions.ClientError as e:
            raise e

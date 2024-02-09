
import os
from azure.storage.blob import BlobServiceClient

class AZUREUTIL(object):

    connect_str = "DefaultEndpointsProtocol=https;AccountName=tradebommodels;AccountKey=OSFHiy2hdM9YC+Uv8ROqWi7Z614yeBZegaf7nhFu2GI29nwqraFQEZW4xmQKl036wRVsPzqx45B3+AStU0jz6A==;EndpointSuffix=core.windows.net"

    @staticmethod
    def is_blob_exists(file_name, container_name):
        blob_service_client = BlobServiceClient.from_connection_string(AZUREUTIL.connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        return blob_client.exists()

    @staticmethod
    def get_file(file_name, container_name):
        blob_service_client = BlobServiceClient.from_connection_string(AZUREUTIL.connect_str)

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        with open(file=os.path.join('/tmp', file_name), mode="wb") as file:
            download_stream = blob_client.download_blob()
            file.write(download_stream.readall())

        return file

    @staticmethod
    def save_file(file_name, container_name, to_delete=True):
        blob_service_client = BlobServiceClient.from_connection_string(AZUREUTIL.connect_str)

        container_client = blob_service_client.get_container_client(container=container_name)
        with open(file=os.path.join('/tmp', file_name), mode="rb") as data:
            container_client.upload_blob(name=file_name, data=data, overwrite=True)

        if to_delete:
            os.remove(os.path.join('', '/tmp/' + file_name))

import sys

import model
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

credential = DefaultAzureCredential()

sys.path.append("..")


def getOcrParameters(customerID, db):
    try:
        configs = (
            db.query(model.FRConfiguration)
            .filter(model.FRConfiguration.idCustomer == customerID)
            .first()
        )
        return configs
    except Exception as e:
        return {}


def upload_to_blob(file_name, file_content, db):
    try:
        configs = getOcrParameters(1, db)
        containername = configs.ContainerName
        connection_str = configs.ConnectionString
        account_name = connection_str.split("AccountName=")[1].split(";AccountKey")[0]
        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=credential
        )
        container_client = blob_service_client.get_container_client(containername)
        file_path = "JourneyDocs/" + file_name
        container_client.upload_blob(name=file_path, data=file_content, overwrite=True)
        return "success"
    except Exception as e:
        print(e)
        return "exception"
    finally:
        db.close()

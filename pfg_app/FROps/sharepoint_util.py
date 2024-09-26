import base64
import json
import os
import sys

import jwt
import model
import requests
from auth import AuthHandler
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext

sys.path.append("..")


auth_handler = AuthHandler()


def getfile_as_base64(file_name, file_type, file_content):
    try:
        base64_str = base64.b64encode(file_content)
        attachment_dict = {
            "filename": file_name,
            "filetype": file_type,
            "content": base64_str.decode("utf-8"),
        }
        return json.dumps(attachment_dict)
    except Exception as e:
        print(e)
        return "exception"


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


def uploadutility_file(file_name, file_content):
    try:
        list_title = "Shared Documents/Utility Bills"
        ctx = ClientContext(os.getenv("SHAREPOINT_URL")).with_credentials(
            ClientCredential(
                os.getenv("SHAREPOINT_CLIENT_ID"), os.getenv("SHAREPOINT_SECRET")
            )
        )
        target_folder = ctx.web.get_folder_by_server_relative_url(list_title)
        target_folder.upload_file(file_name, file_content).execute_query()
        return "success"
    except Exception as e:
        return "exception"


def sharepoint_creds():
    tenant_id = os.getenv("SERVICE_TENANT_ID")
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    body = {
        "grant_type": "client_credentials",
        "client_id": os.getenv("SERVICE_PRINCIPAL"),
        "scope": "https://vault.azure.net/.default",
        "client_secret": os.getenv("SERVICE_CLIENT_SECRET"),
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    token_resp = requests.post(token_url, data=body, headers=headers)
    access_token = token_resp.json()["access_token"]
    keyvault_url = os.getenv("KEYVAULT")
    secret_name = os.getenv("KV_SECRET1")
    vault_url = f"{keyvault_url}/secrets/{secret_name}?api-version=7.2"
    configresp = requests.get(
        vault_url, headers={"Authorization": f"Bearer {access_token}"}
    )
    if configresp.status_code == 200:
        config = configresp.json()["value"]
        config = jwt.decode(config, auth_handler.secret, algorithms=["HS256"])
        message = "success"
    else:
        message = "Fail to get Sharepoint config"
        config = {"client_id": "", "client_secret": ""}
    return message, config["site_url"], config["client_id"], config["client_secret"]

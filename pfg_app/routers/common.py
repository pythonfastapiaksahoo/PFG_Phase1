import time
import traceback

import requests
from azure.ai.formrecognizer import DocumentModelAdministrationClient
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from fastapi import APIRouter, Depends, Response

from pfg_app import settings
from pfg_app.azuread.auth import get_admin_user

# from pfg_app.core import azure_fr as core_fr
from pfg_app.core.utils import get_blob_securely
from pfg_app.logger_module import logger

router = APIRouter(
    prefix="/apiv1.1/Common",
    tags=["Common"],
    dependencies=[Depends(get_admin_user)],
    responses={404: {"description": "Not found"}},
)


@router.get("/get-blob-file")
def get_blob_file(container_name: str, blob_path: str):
    """API route to retrieve a file from Azure Blob Storage.

    Parameters:
    ----------
    file_name : str
        Name of the file to retrieve from Azure Blob Storage.

    Returns:
    -------
    File object from Azure Blob Storage.
    """

    blob_name = blob_path.split("/")[-1]

    blob_data, content_type = get_blob_securely(container_name, blob_path)

    headers = {
        "Content-Disposition": f"inline; filename={blob_name}",
        "Content-Type": content_type,
    }
    return Response(content=blob_data, headers=headers, media_type=content_type)


# @router.get("/call-azure-document-intelligence")
# def call_azure_document_intelligence(container_name: str, blob_path: str):
#     """API route to call the Azure Document Intelligence API.

#     Returns:
#     -------
#     Response from the Azure Document Intelligence API.
#     """
#     file_data, content_type = get_blob_securely(
#         container_name=container_name, blob_path=blob_path
#     )
#     response = core_fr.call_form_recognizer(
#         file_data=file_data,
#         endpoint=settings.form_recognizer_endpoint,
#         api_version=settings.api_version,
#     )
#     return response


@router.get("/iics-status")
def iics_status():
    try:
        response = requests.post(
            settings.erp_invoice_status_endpoint,
            json={
                "RequestBody": {
                    "INV_STAT_RQST": {
                        "BUSINESS_UNIT": "MERCH",
                        "INVOICE_ID": "9999999",
                        "INVOICE_DT": "2023-06-10",
                        "VENDOR_SETID": "GLOBL",
                        "VENDOR_ID": "97879",
                    }
                }
            },
            headers={"Content-Type": "application/json"},
            auth=(settings.erp_user, settings.erp_password),
            timeout=60,  # Set a timeout of 60 seconds
        )

        return response.json()
    except Exception:
        return {"error": traceback.format_exc()}


# Route to Move Azure DI models from Source to Destination
@router.get("/move-azure-di-models")
def move_azure_di_models(source_di_name, target_di_name):
    source_endpoint = f"https://{source_di_name}.cognitiveservices.azure.com/"
    target_endpoint = f"https://{target_di_name}.cognitiveservices.azure.com/"

    try:
        # check if source is accessible with System Identity
        source_credential = DefaultAzureCredential()
        # Initialize the Form Recognizer client
        source_document_model_admin_client = DocumentModelAdministrationClient(
            source_endpoint, source_credential
        )
        source_document_model_admin_client.get_document_model(
            model_id="prebuilt-invoice"
        )
    except Exception:
        # Check if source is accessible with Service Principal
        try:
            source_credential = ClientSecretCredential(
                settings.tenant_id, settings.client_id, settings.client_secret
            )
            # Initialize the Form Recognizer client
            source_document_model_admin_client = DocumentModelAdministrationClient(
                source_endpoint, source_credential
            )
            source_document_model_admin_client.get_document_model(
                model_id="prebuilt-invoice"
            )
        except ClientAuthenticationError as e:
            return {
                "error": f"ClientAuthenticationError:accessing source DI: {str(e)}",
                "tenant_id": settings.tenant_id,
                "client_id": settings.client_id,
                "client_secret": settings.client_secret,
            }
        except Exception as e:
            return {
                "error": f"Error accessing source DI: {str(e)}",
                "tenant_id": settings.tenant_id,
                "client_id": settings.client_id,
                "client_secret": settings.client_secret,
            }

    try:
        # check if target is accessible with System Identity
        target_credential = DefaultAzureCredential()
        # Initialize the Form Recognizer client
        target_document_model_admin_client = DocumentModelAdministrationClient(
            target_endpoint, target_credential
        )
        target_document_model_admin_client.get_document_model(
            model_id="prebuilt-invoice"
        )
    except Exception:
        # Check if target is accessible with Service Principal
        try:
            target_credential = ClientSecretCredential(
                settings.tenant_id, settings.client_id, settings.client_secret
            )
            # Initialize the Form Recognizer client
            target_document_model_admin_client = DocumentModelAdministrationClient(
                target_endpoint, target_credential
            )
            target_document_model_admin_client.get_document_model(
                model_id="prebuilt-invoice"
            )
        except ClientAuthenticationError as e:
            return {
                "error": f"ClientAuthenticationError:accessing target DI: {str(e)}",
                "tenant_id": settings.tenant_id,
                "client_id": settings.client_id,
                "client_secret": settings.client_secret,
            }
        except Exception as e:
            return {
                "error": f"Error accessing target DI: {str(e)}",
                "tenant_id": settings.tenant_id,
                "client_id": settings.client_id,
                "client_secret": settings.client_secret,
            }

    try:
        # Get all models from Source
        source_models = source_document_model_admin_client.list_document_models()

        # Convert the list to a readable format
        source_model_list = [
            {
                "model_id": model.model_id,
                "description": model.description,
                "created_on": model.created_on,
            }
            for model in source_models
        ]

        # Get all models from Target
        target_models = target_document_model_admin_client.list_document_models()

        # Convert the list to a readable format
        target_model_list = [
            {
                "model_id": model.model_id,
                "description": model.description,
                "created_on": model.created_on,
            }
            for model in target_models
        ]
        target_model_id_list = [model["model_id"] for model in target_model_list]
        copy_process_info = []
        for source_model in source_model_list:
            if not source_model["model_id"].startswith("prebuilt-"):
                try:
                    # copy only if the model is not already present in the target
                    if source_model["model_id"] not in target_model_id_list:
                        # Generate copy authorization
                        copy_auth = (
                            target_document_model_admin_client.get_copy_authorization(
                                model_id=source_model["model_id"]
                            )
                        )

                        # Copy the model from source to target
                        copy_result = source_document_model_admin_client.begin_copy_document_model_to(  # noqa: E501
                            model_id=source_model["model_id"],
                            target={
                                "targetResourceId": copy_auth["targetResourceId"],
                                "targetResourceRegion": copy_auth[
                                    "targetResourceRegion"
                                ],
                                "targetModelId": copy_auth["targetModelId"],
                                "accessToken": copy_auth["accessToken"],
                                "expirationDateTime": copy_auth["expirationDateTime"],
                                "targetModelLocation": copy_auth["targetModelLocation"],
                            },
                        )
                        # If copy not done in 1 minute, print status as copy failed
                        start_time = time.time()
                        while not copy_result.done():
                            logger.info(
                                f"Copying model {source_model['model_id']} from "
                                + f"{source_endpoint} to {target_endpoint}"
                            )
                            time.sleep(5)  # Polling interval
                            if time.time() - start_time > 60:
                                logger.info(
                                    "Copy operation taking longer than expected. Aborting."  # noqa: E501
                                )
                                break
                        else:
                            logger.info(
                                f"Model {source_model['model_id']} copied successfully"
                            )

                        if copy_result.done():
                            copy_process_info.append(
                                {
                                    "model_id": source_model["model_id"],
                                    "status": "success",
                                }
                            )  # copy_result.status()
                        else:
                            copy_process_info.append(
                                {
                                    "model_id": source_model["model_id"],
                                    "status": "failed",
                                }
                            )  # copy_result.status()
                    else:
                        copy_process_info.append(
                            {
                                "model_id": source_model["model_id"],
                                "status": "already exists",
                            }
                        )

                except Exception as e:
                    logger.error(
                        f"Error copying model {source_model['model_id']}: {str(e)}"
                    )
                    copy_process_info.append(
                        {"model_id": source_model["model_id"], "status": "failed"}
                    )
                    continue
            else:
                copy_process_info.append(
                    {"model_id": source_model["model_id"], "status": "prebuilt model"}
                )

        # Get all models from Target
        target_models = target_document_model_admin_client.list_document_models()

        # Convert the list to a readable format
        target_model_list = [
            {
                "model_id": model.model_id,
                "description": model.description,
                "created_on": model.created_on,
            }
            for model in target_models
        ]

        return {
            "source_models": source_model_list,
            "target_models": target_model_list,
            "copy_process_info": copy_process_info,
        }
    except Exception as e:
        return {"error": f"Error copying models: {str(e)}"}

import traceback
import uuid

import requests
from azure.storage.blob import BlobServiceClient
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Response
from starlette.status import HTTP_201_CREATED

from pfg_app import settings
from pfg_app.azuread.auth import get_admin_user

# from pfg_app.core import azure_fr as core_fr
from pfg_app.core.utils import get_blob_securely, get_credential
from pfg_app.crud.commonCrud import (
    acquire_lock,
    copy_models_in_background,
    get_task_status,
    set_task_status,
)

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
@router.get("/move-azure-di-models", status_code=HTTP_201_CREATED)
def move_azure_di_models(
    source_di_name, target_di_name, background_tasks: BackgroundTasks
):

    # Get the credential
    credential = get_credential()

    account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient(
        account_url=account_url, credential=credential
    )
    container_name = "task-status-container"
    container_client = blob_service_client.get_container_client(container_name)

    # Ensure the container exists
    if not container_client.exists():
        container_client.create_container()

    # Attempt to acquire the lock
    if not acquire_lock(container_client, "copy-process-lock"):
        return HTTPException(
            status_code=208,
            detail="A copy process is already running. Please try again later.",
        )

    task_id = str(uuid.uuid4())
    set_task_status(container_client, task_id, "initiated", "Task initiated")

    # Start the copying process in a background task
    background_tasks.add_task(
        copy_models_in_background,
        container_client,
        task_id,
        source_di_name,
        target_di_name,
    )

    return {"message": "Model copying started", "task_id": task_id}


@router.get("/task-status/{task_id}")
def get_task_status_route(task_id: str):
    # Get the credential
    credential = get_credential()

    account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient(
        account_url=account_url, credential=credential
    )
    container_name = "task-status-container"
    container_client = blob_service_client.get_container_client(container_name)

    # Ensure the container exists
    if not container_client.exists():
        return HTTPException(status_code=400, detail="No task status container found")
    status = get_task_status(container_client, task_id)
    return status


@router.delete("/task-status/{task_id}")
def delete_task_status_route(task_id: str):
    # Get the credential
    credential = get_credential()

    account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient(
        account_url=account_url, credential=credential
    )
    container_name = "task-status-container"
    container_client = blob_service_client.get_container_client(container_name)

    # Ensure the container exists
    if not container_client.exists():
        return HTTPException(status_code=400, detail="No task status container found")

    STOP_SIGNAL_BLOB_NAME = "stop-signal"
    stop_blob_client = container_client.get_blob_client(STOP_SIGNAL_BLOB_NAME)
    stop_blob_client.upload_blob("stop", overwrite=True)  # Set the stop signal
    return {"message": "Stop signal sent to the copy process"}

from datetime import datetime
import traceback
import uuid

import requests
from apscheduler.triggers.interval import IntervalTrigger
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Response
from starlette.status import HTTP_201_CREATED

from pfg_app import model, scheduler, scheduler_container_client, settings
from pfg_app.azuread.auth import get_admin_user

# from pfg_app.core import azure_fr as core_fr
from pfg_app.azuread.schemas import AzureUser
from pfg_app.core.utils import get_blob_securely, get_credential
from pfg_app.crud.commonCrud import (
    acquire_lock,
    copy_models_in_background,
    get_task_status,
    set_task_status,
)
from pfg_app.crud.ERPIntegrationCrud import (
    bulkProcessVoucherData,
    newbulkupdateInvoiceStatus,
)
from pfg_app.logger_module import logger
from pfg_app.session.session import get_db

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


@router.post("/run-job")
async def run_job(background_tasks: BackgroundTasks, job_name: str):
    """Endpoint to trigger the job manually, with locking."""
    logger.info(f"Manually triggering the job {job_name}")
    if job_name == "bulk_update_invoice_status":
        # check if the job is already scheduled by looking at the Blob Lease
        blob_client = scheduler_container_client.get_blob_client("status-job-lock")
        try:
            lease = blob_client.acquire_lease()
        except Exception as e:
            logger.error(f"Error acquiring lease: {e}")
            return {"error": "Error acquiring lease | Possible job already running"}
        lease.break_lease()
        job_id = background_tasks.add_task(newbulkupdateInvoiceStatus)
        return {
            "message": "Job triggered manually | use Job ID to track the job",
            "job_id": job_id,
        }
    elif job_name == "bulk_update_invoice_creation":
        # check if the job is already scheduled by looking at the Blob Lease
        blob_client = scheduler_container_client.get_blob_client("creation-job-lock")
        try:
            lease = blob_client.acquire_lease()
        except Exception as e:
            logger.error(f"Error acquiring lease: {e}")
            return {"error": "Error acquiring lease | Possible job already running"}
        lease.break_lease()
        job_id = background_tasks.add_task(bulkProcessVoucherData)
        return {
            "message": "Job triggered manually | use Job ID to track the job",
            "job_id": job_id,
        }
    else:
        return {"error": "Invalid job name"}


@router.post("/update-schedule")
async def update_schedule(
    minutes: int,
    job_name: str,
    user: AzureUser = Depends(get_admin_user)
    ):
    """Endpoint to update the recurring job interval dynamically."""
    db = next(get_db())
    if minutes < 5:
        logger.info(f"Updating job schedule to every {minutes} minutes")
        return HTTPException(
            status_code=400, detail="Interval must be at least 5 minutes"
        )
    if job_name == "bulk_update_invoice_status":
        # check if the job is already scheduled by looking at the Blob Lease
        blob_client = scheduler_container_client.get_blob_client("status-job-lock")
        try:
            lease = blob_client.acquire_lease()
        except ResourceExistsError as e:
            logger.error(f"Error acquiring lease: {e.error_code} - {e.reason}")
            return {"error": "Error acquiring lease | Possible job already running"}
        except Exception as e:
            logger.error(f"Error acquiring lease: {e}")
            return {"error": "Error acquiring lease | Possible job already running"}
        finally:
            if "lease" in locals():
                lease.break_lease()
        # Fetching the first name of the user performing the rejection
        first_name = (
            db.query(model.User.firstName).filter(model.User.idUser == user.idUser).scalar()
        )
        # Fetch the currently active job
        active_task = (
            db.query(model.TaskSchedular)
            .filter(
                model.TaskSchedular.task_name == "bulk_update_invoice_status",
                model.TaskSchedular.is_active == 1
            )
            .first()
        )

        # If an active task exists with the same interval, return early
        if active_task and active_task.time_interval == minutes:
            return {"message": f"Job schedule already set to every {minutes} minutes"}

        # Deactivate all previous entries for this job
        db.query(model.TaskSchedular).filter_by(
            task_name="bulk_update_invoice_status", is_active=1
        ).update({"is_active": 0})

        # Create a new active task
        new_task = model.TaskSchedular(
            task_name="bulk_update_invoice_status",
            time_interval=minutes,
            is_active=1,
            user_id=user.idUser,
            updated_at=datetime.utcnow(),
            updated_by=first_name,
        )
        db.add(new_task)
        db.commit()  # Commit all changes
        scheduler.reschedule_job(
            "bulk_update_invoice_status", trigger=IntervalTrigger(minutes=minutes)
        )
        return {"message": f"Job schedule updated to every {minutes} minutes"}
    elif job_name == "bulk_update_invoice_creation":
        # check if the job is already scheduled by looking at the Blob Lease
        blob_client = scheduler_container_client.get_blob_client("creation-job-lock")
        try:
            lease = blob_client.acquire_lease()
        except ResourceExistsError as e:
            logger.error(f"Error acquiring lease: {e.error_code} - {e.reason}")
            return {"error": "Error acquiring lease | Possible job already running"}
        except Exception as e:
            logger.error(f"Error acquiring lease: {e}")
            return {"error": "Error acquiring lease | Possible job already running"}
        finally:
            if "lease" in locals():
                lease.break_lease()
        # Fetching the first name of the user performing the rejection
        first_name = (
            db.query(model.User.firstName).filter(model.User.idUser == user.idUser).scalar()
        )
        # Fetch the currently active job
        active_task = (
            db.query(model.TaskSchedular)
            .filter(
                model.TaskSchedular.task_name == "bulk_update_invoice_creation",
                model.TaskSchedular.is_active == 1
            )
            .first()
        )

        # If an active task exists with the same interval, return early
        if active_task and active_task.time_interval == minutes:
            return {"message": f"Job schedule already set to every {minutes} minutes by {first_name}"}

        # Deactivate all previous entries for this job
        db.query(model.TaskSchedular).filter_by(
            task_name="bulk_update_invoice_creation", is_active=1
        ).update({"is_active": 0})

        # Create a new active tas
        new_task = model.TaskSchedular(
            task_name="bulk_update_invoice_creation",
            time_interval=minutes,
            is_active=1,
            user_id=user.idUser,
            updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            updated_by=first_name,
        )
        db.add(new_task)
        db.commit()  # Commit all changes
        scheduler.reschedule_job(
            "bulk_update_invoice_creation", trigger=IntervalTrigger(minutes=minutes)
        )
        return {"message": f"Job schedule updated to every {minutes} minutes by {first_name}"}
    else:
        logger.error(f"Recurring job [{job_name}] not found")
        return {"error": f"Recurring job [{job_name}] not found"}


@router.get("/current-schedule")
async def get_current_schedule(job_name: str):
    """Endpoint to get the current job schedule."""
    job = scheduler.get_job(job_name)
    if job:
        trigger = job.trigger
        return {
            "job_id": job.id,
            "next_run_time": job.next_run_time,
            "interval": trigger.interval / 60,
        }
    else:
        return {"message": "No job scheduled"}


@router.post("/update-retry-count")
async def update_retry_count(count: int, job_name: str, user: AzureUser = Depends(get_admin_user)):
    """Endpoint to update the retry count for a given invoice."""
    db = next(get_db())
    if job_name == "retry_invoice_creation":
        # Fetching the first name of the user performing the rejection
        first_name = (
            db.query(model.User.firstName).filter(model.User.idUser == user.idUser).scalar()
        )
        
        # Fetch the currently active job
        active_task = (
            db.query(model.SetRetryCount)
            .filter(
                model.SetRetryCount.task_name == "retry_invoice_creation",
                model.SetRetryCount.is_active == 1
            )
            .first()
        )

        # If an active task exists with the same interval, return early
        if active_task and active_task.frequency == count:
            return {"message": f"Retry Frequency already set to {count} by {first_name}"}

        # Deactivate all previous entries for this job
        db.query(model.SetRetryCount).filter_by(
            task_name="retry_invoice_creation", is_active=1
        ).update({"is_active": 0})

        # Create a new active tas
        new_task = model.SetRetryCount(
            task_name="retry_invoice_creation",
            frequency=count,
            is_active=1,
            user_id=user.idUser,
            updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            updated_by=first_name,
        )
        db.add(new_task)
        db.commit()  # Commit all changes
        return {"message": f"Retry count updated to {count} by {first_name}"}
    else:
        logger.error(f"Recurring job [{job_name}] not found")
        return {"error": f"Recurring job [{job_name}] not found"}

@router.get("/get-retry-count")
async def get_retry_count(job_name: str):
    """Endpoint to get the current job schedule."""
    db = next(get_db())
    job_name = job_name.lower()
    if job_name == "retry_invoice_creation":
        job = db.query(model.SetRetryCount).filter_by(task_name=job_name, is_active=1).first()
        return {"retry_count": f"{job.frequency}"}
    else:
        logger.error(f"Recurring job [{job_name}] not found")

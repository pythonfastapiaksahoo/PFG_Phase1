import threading
import traceback
import uuid
from datetime import datetime

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobLeaseClient
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind
from sqlalchemy.exc import IntegrityError
from pfg_app import scheduler, scheduler_container_client, settings
from pfg_app.crud.commonCrud import (
    schedule_bulk_update_corp_invoice_creation_job,
    schedule_bulk_update_corp_invoice_status_job,
    schedule_bulk_update_invoice_creation_job,
    schedule_bulk_update_invoice_status_job,
)
from pfg_app.graph_api.manage_subscriptions import subscription_renewal_loop
from pfg_app.logger_module import logger, set_operation_id, tracer
from pfg_app.model import BackgroundTask, QueueTask, CorpQueueTask, corp_trigger_tab
from pfg_app.routers import (
    FR,
    OCR,
    ERPIntegrationapi,
    batchexception,
    common,
    invoice,
    modelonboarding,
    vendor,
    CorpIntegrationapi,
    mailListener,
)
from pfg_app.session.session import get_db
from pfg_app.graph_api.retry_unprocessed_corp_mails import fetch_and_process_recent_graph_mails

app = FastAPI(
    title="IDP",
    version="1.0",
    swagger_ui_oauth2_redirect_url="/oauth2-redirect",
    swagger_ui_init_oauth={
        "usePkceWithAuthorizationCodeGrant": True,
        "clientId": settings.swagger_ui_client_id,
        "scopes": [f"api://{settings.api_client_id}/access_as_user"],
    },
)


@app.on_event("startup")
async def app_startup():
    # try:
    #     operation_id = uuid.uuid4().hex
    #     set_operation_id(operation_id)
    #     db = next(get_db())
    #     current_schema = db.execute("SELECT current_schema();").scalar()
    #     logger.info(f"Current schema before setting: {current_schema}")
    #     db.execute("SET search_path TO pfg_schema;")
    #     current_schema = db.execute("SELECT current_schema();").scalar()
    #     logger.info(f"Current schema after setting: {current_schema}")
    #     return True
    # except Exception:
    #     logger.error(traceback.format_exc())
    #     return True
    if settings.build_type != "debug":
        operation_id = uuid.uuid4().hex
        set_operation_id(operation_id)
        logger.info("Starting FastAPI application")

        try:
            if not scheduler_container_client.exists():
                scheduler_container_client.create_container()
        except Exception as e:
            logger.error(f"Error: {e}" + traceback.format_exc())

        try:
            # Create the STATUS_BLOB_NAME blob
            blob_client = scheduler_container_client.get_blob_client("status-job-lock")
            # Check if the blob exists
            if not blob_client.exists():
                blob_client.create_append_blob()
                logger.info("Blob `status-job-lock` created successfully")

            # Check if the blob has an active lease
            properties = blob_client.get_blob_properties()
            lease_state = properties.lease.state

            # If a lease exists, break it
            if lease_state == "leased":
                lease_client = BlobLeaseClient(blob_client)
                logger.info("Breaking existing lease...")
                lease_client.break_lease()

            # Acquire a lease on the blob to act as a distributed lock.
            lease = blob_client.acquire_lease()

            # check if the blob meta dat ato see if any job is running from the metadata
            blob_metadata = blob_client.get_blob_properties().metadata

            blob_metadata.update({"last_run_time": str(datetime.now())})
            blob_client.append_block(operation_id + "\n")
            blob_client.set_blob_metadata(metadata=blob_metadata, lease=lease)

            logger.info(
                f"Metadata updated with last run time: {blob_metadata['last_run_time']}"
            )
            # release the lock
            lease.break_lease()
            # Set up a Timer triggered Background Job with ap-scheduler
            schedule_bulk_update_invoice_status_job()
        except ResourceExistsError as e:
            logger.warning(f"Error: {e.error_code} - {e.reason}")
        except Exception as e:
            logger.info(f"Exception: {e}" + traceback.format_exc())

        try:
            # Create the CREATION_BLOB_NAME blob `creation-job-lock`
            blob_client = scheduler_container_client.get_blob_client(
                "creation-job-lock"
            )
            # Check if the blob exists
            if not blob_client.exists():
                blob_client.create_append_blob()
                logger.info("Blob `creation-job-lock` created successfully")

            # Check if the blob has an active lease
            properties = blob_client.get_blob_properties()
            lease_state = properties.lease.state

            # If a lease exists, break it
            if lease_state == "leased":
                lease_client = BlobLeaseClient(blob_client)
                logger.info("Breaking existing lease...")
                lease_client.break_lease()

            # Acquire a lease on the blob to act as a distributed lock.
            lease = blob_client.acquire_lease()

            # check if the blob meta dat ato see if any job is running from the metadata
            blob_metadata = blob_client.get_blob_properties().metadata

            blob_metadata.update({"last_run_time": str(datetime.now())})
            blob_client.append_block(operation_id + "\n")
            blob_client.set_blob_metadata(metadata=blob_metadata, lease=lease)

            logger.info(
                f"Metadata updated with last run time: {blob_metadata['last_run_time']}"
            )
            # release the lock
            lease.break_lease()
            # Set up a Timer triggered Background Job with ap-scheduler
            schedule_bulk_update_invoice_creation_job()
        except ResourceExistsError as e:
            logger.warning(f"Error: {e.error_code} - {e.reason}")
        except Exception as e:
            logger.info(f"Exception: {e}" + traceback.format_exc())

        try:
            # Create the CORP_STATUS_BLOB_NAME blob
            blob_client = scheduler_container_client.get_blob_client("corp-status-job-lock")
            # Check if the blob exists
            if not blob_client.exists():
                blob_client.create_append_blob()
                logger.info("Blob `corp-status-job-lock` created successfully")

            # Check if the blob has an active lease
            properties = blob_client.get_blob_properties()
            lease_state = properties.lease.state

            # If a lease exists, break it
            if lease_state == "leased":
                lease_client = BlobLeaseClient(blob_client)
                logger.info("Breaking existing lease...")
                lease_client.break_lease()

            # Acquire a lease on the blob to act as a distributed lock.
            lease = blob_client.acquire_lease()

            # check if the blob meta dat ato see if any job is running from the metadata
            blob_metadata = blob_client.get_blob_properties().metadata

            blob_metadata.update({"last_run_time": str(datetime.now())})
            blob_client.append_block(operation_id + "\n")
            blob_client.set_blob_metadata(metadata=blob_metadata, lease=lease)

            logger.info(
                f"Metadata updated with last run time: {blob_metadata['last_run_time']}"
            )
            # release the lock
            lease.break_lease()
            # Set up a Timer triggered Background Job with ap-scheduler
            schedule_bulk_update_corp_invoice_status_job()
        except ResourceExistsError as e:
            logger.warning(f"Error: {e.error_code} - {e.reason}")
        except Exception as e:
            logger.info(f"Exception: {e}" + traceback.format_exc())
        
        try:
            # Create the CORP_CREATION_BLOB_NAME blob `creation-job-lock`
            blob_client = scheduler_container_client.get_blob_client(
                "corp-creation-job-lock"
            )
            # Check if the blob exists
            if not blob_client.exists():
                blob_client.create_append_blob()
                logger.info("Blob `corp-creation-job-lock` created successfully")

            # Check if the blob has an active lease
            properties = blob_client.get_blob_properties()
            lease_state = properties.lease.state

            # If a lease exists, break it
            if lease_state == "leased":
                lease_client = BlobLeaseClient(blob_client)
                logger.info("Breaking existing lease...")
                lease_client.break_lease()

            # Acquire a lease on the blob to act as a distributed lock.
            lease = blob_client.acquire_lease()

            # check if the blob meta dat ato see if any job is running from the metadata
            blob_metadata = blob_client.get_blob_properties().metadata

            blob_metadata.update({"last_run_time": str(datetime.now())})
            blob_client.append_block(operation_id + "\n")
            blob_client.set_blob_metadata(metadata=blob_metadata, lease=lease)

            logger.info(
                f"Metadata updated with last run time: {blob_metadata['last_run_time']}"
            )
            # release the lock
            lease.break_lease()
            # Set up a Timer triggered Background Job with ap-scheduler
            schedule_bulk_update_corp_invoice_creation_job()
        except ResourceExistsError as e:
            logger.warning(f"Error: {e.error_code} - {e.reason}")
        except Exception as e:
            logger.info(f"Exception: {e}" + traceback.format_exc())
    
        logger.info("Resetting all queues before starting the application")
        db = next(get_db())
        db.query(QueueTask).filter(QueueTask.status == "processing").update(
            {"status": "queued"}
        )
        db.commit()
        logger.info("All DSD queues reset to queued state")
        worker_thread = threading.Thread(target=OCR.queue_worker, daemon=True, kwargs={
            "operation_id": operation_id
        })
        worker_thread.start()
        logger.info("OCR Worker thread started")
        
        # check if the background task is present and if not create a new one
        background_task = db.query(BackgroundTask).filter(BackgroundTask.task_name == "subscription_renewal_loop").first()
        if not background_task:
            try:
                background_task = BackgroundTask(status="active", task_name="subscription_renewal_loop")
                db.add(background_task)
                db.commit()
            except IntegrityError:
                logger.error("Background task already exists")
        logger.info("All Background Tasks reset to active state")

        background_task_thread = threading.Thread(target=subscription_renewal_loop, daemon=True, kwargs={"operation_id": operation_id})
        background_task_thread.start()
        logger.info("Subscription Renewal Loop thread started")
        
        # # Resetting Corp Queue task before starting the application
        # db.query(CorpQueueTask).filter(CorpQueueTask.status == "processing").update(
        #     {"status": "queued"}
        # )
        # db.commit()
        # logger.info("All Corp queues reset to queued state")
        corp_worker_thread = threading.Thread(target=CorpIntegrationapi.corp_queue_worker, daemon=True,
                    kwargs={"operation_id": operation_id})
        corp_worker_thread.start()
        logger.info("CorpIntegration Worker thread started")
        
        # try:
        #     logger.info("Fetching and processing unprocessed Graph messages...")
        #     fetch_and_process_recent_graph_mails(db=db, operation_id=operation_id)
        #     logger.info("Completed initial sync of new Graph messages.")
        # except Exception as e:
        #     logger.error(f"Error during startup Graph sync: {e}\n{traceback.format_exc()}")
    else:
        operation_id = uuid.uuid4().hex
        set_operation_id(operation_id)
        logger.info("Resetting all queues before starting the application")
        db = next(get_db())
        # db.query(QueueTask).filter(
        #     QueueTask.status == f"{settings.local_user_name}-processing"
        # ).update({"status": f"{settings.local_user_name}-queued"})
        # db.commit()
        # logger.info("All DSD queues reset to queued state")
        # worker_thread = threading.Thread(target=OCR.queue_worker, daemon=True, kwargs={
        #     "operation_id": operation_id
        # })
        # worker_thread.start()
        # logger.info("OCR Worker thread started")

        # # check if the background task is present and if not create a new one
        # background_task = db.query(BackgroundTask).filter(BackgroundTask.task_name == f"{settings.local_user_name}-subscription_renewal_loop").first()
        # if not background_task:
        #     try:
        #         background_task = BackgroundTask(status=f"{settings.local_user_name}-active", task_name=f"{settings.local_user_name}-subscription_renewal_loop")
        #         db.add(background_task)
        #         db.commit()
        #     except IntegrityError:
        #         logger.error("Background task already exists")
        # logger.info("All Background Tasks reset to active state")

        # background_task_thread = threading.Thread(target=subscription_renewal_loop, daemon=True, kwargs={"operation_id": operation_id})
        # background_task_thread.start()
        # logger.info("Subscription Renewal Loop thread started")
        
        # # Resetting Corp Queue task before starting the application
        # db.query(CorpQueueTask).filter(
        #     CorpQueueTask.status == f"{settings.local_user_name}-processing"
        # ).update({"status": f"{settings.local_user_name}-queued"})
        # db.commit()
        # logger.info("All Corp queues reset to queued state")
        # corp_worker_thread = threading.Thread(target=CorpIntegrationapi.corp_queue_worker,
        #             daemon=True,kwargs={"operation_id": operation_id})
        # corp_worker_thread.start()
        # logger.info("CorpIntegration Worker thread started")
    logger.info("Application is ready to process requests")


@app.on_event("shutdown")
async def app_shutdown():
    if settings.build_type != "debug":
        logger.info("Shutting down FastAPI application")
        scheduler.shutdown()
        logger.info("Application is shut down")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Middleware to set Operation ID from request headers
@app.middleware("http")
async def add_operation_id(request: Request, call_next):
    if settings.build_type != "debug":
        operation_id = request.headers.get("x-operation-id")
        if operation_id:
            set_operation_id(operation_id)
        else:
            # Create a new Operation ID if not provided
            operation_id = uuid.uuid4().hex
            set_operation_id(operation_id)

        with tracer.start_as_current_span(
            "FastAPIRequest", context=extract(request.headers), kind=SpanKind.SERVER
        ) as span:
            logger.info(
                "Received request in FastAPI"
            )  # Automatically includes Operation ID
            span.set_attribute("operation_id", operation_id or "unknown")

            response = await call_next(request)
            response.headers["x-operation-id"] = operation_id or "unknown"

            response.headers["api-version"] = "0.109.26"

            logger.info(
                "Sending response from FastAPI"
            )  # Automatically includes Operation ID
            return response
    else:
        return await call_next(request)


# Define routers in the main
app.include_router(vendor.router)
app.include_router(invoice.router)
app.include_router(FR.router)
app.include_router(OCR.router)
app.include_router(modelonboarding.router)
app.include_router(ERPIntegrationapi.router)
app.include_router(batchexception.router)
app.include_router(common.router)
app.include_router(CorpIntegrationapi.router)
app.include_router(mailListener.router)

@app.get("/")
async def root():
    return {"message": "Hello! This is IDP"}
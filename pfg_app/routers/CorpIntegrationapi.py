from datetime import datetime, timedelta
import json
import os
import re
import io
import time
import traceback
from typing import List, Optional
from urllib.parse import unquote, urlparse
from email import policy
from email.parser import BytesParser
import uuid
from fastapi import APIRouter, File, Form, HTTPException, Query, Depends, Response, UploadFile
from fastapi.responses import FileResponse
import pytz
from pfg_app import model
from pfg_app.FROps import pdfcreator
from pfg_app.FROps.coding_lineMatch import getCode_map_status
from pfg_app.FROps.corp_postpro import corp_postPro
from pfg_app.FROps.corp_validations import validate_corpdoc
from pfg_app.azuread.auth import get_user, get_user_dependency
from pfg_app.azuread.schemas import AzureUser
from pfg_app.core.openai_data import extract_approver_details_using_openai, extract_invoice_details_using_openai
# from azure.identity import (
#     ClientSecretCredential
# )
from pfg_app.auth import AuthHandler
from pfg_app.core.utils import get_blob_securely, get_credential, upload_blob_securely
# from pfg_app.graph_api.mark_processed_mail_to_read import mark_processed_mail_as_read
from pfg_app.logger_module import get_operation_id, logger, set_operation_id
from pfg_app.model import CorpQueueTask
from functools import wraps
from pfg_app.schemas.CorpIntegrationSchema import CorpMetadataCreate, CorpMetadataDelete, ProcessResponse
from pfg_app.session.session import SQLALCHEMY_DATABASE_URL, get_db
from sqlalchemy.orm import Session
from pfg_app import settings
from sqlalchemy.exc import InvalidRequestError, OperationalError, IntegrityError
from sqlalchemy.orm import Session
from pfg_app.crud import CorpIntegrationCrud as crud
from pfg_app.schemas import CorpIntegrationSchema as schema
from azure.storage.blob import (
    AccountSasPermissions,
    BlobServiceClient,
    generate_container_sas,
)
# Initialize FastAPI app
# app = FastAPI()
from sqlalchemy import select, or_
from fastapi.responses import StreamingResponse
import pandas as pd
from pydantic import BaseModel
from typing import List, Dict, Any
from sqlalchemy.dialects.postgresql import insert

auth_handler = AuthHandler()
router = APIRouter(
    prefix="/apiv1.1/CorpIntegration",
    tags=["CorpIntegration"],
    # dependencies=[Depends(get_user_dependency(["CORP_ConfigPortal_User","CORP_APPortal_User"]))],
    responses={404: {"description": "Not found"}},
)

# Base directory for CE-1001
# BASE_DIR = "./routers/CE-1001"

def retry_on_exception(max_retries=5, delay=3):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    logger.warning(
                        f"Retry {retries}/{max_retries} after exception: {e}"
                    )
                    time.sleep(delay)
                    if retries == max_retries:
                        raise
        return wrapper
    return decorator


@retry_on_exception(max_retries=5, delay=3)
def save_to_database(new_task):
    try:
        db = next(get_db())
        db.add(new_task)
        db.flush()  # Generate the ID without committing
        task_id = new_task.id

        # Check if mail_row_key is None (since it's part of the `new_task` object)
        if new_task.request_data.get("mail_row_key") is None:
            # Calculate the mail_row_key
            mail_row_key = f"CE-{10000000 + task_id}"

            # Update the field in the JSONB column
            new_task.request_data["mail_row_key"] = mail_row_key

        # Commit the changes (flush already staged the changes for this session)
        db.commit()
        return task_id
    
    except IntegrityError:
        db.rollback()
        
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
        
        
# @retry_on_exception(max_retries=5, delay=3)
# def save_to_database(new_task):
#     db = next(get_db())
#     try:
#         insert_stmt = insert(CorpQueueTask).values(
#             request_data=new_task.request_data,
#             status=new_task.status,
#             mail_row_key=new_task.mail_row_key,
#         ).on_conflict_do_nothing(
#             index_elements=["mail_row_key"]  # ensures uniqueness
#         ).returning(CorpQueueTask.id)

#         result = db.execute(insert_stmt)
#         db.commit()

#         task_id = result.scalar()
#         if task_id is None:
#             logger.info(f"Duplicate detected: mail_row_key={new_task.mail_row_key}. No insertion.")
#             return None  # Or whatever your logic wants here

#         return task_id

#     except Exception as e:
#         db.rollback()
#         logger.error(f"Unexpected DB error: {str(e)}")
#         raise
#     finally:
#         db.close()

@router.post("/corp-process-stream")
def runStatus(
    eml_path: str = Form(...),
    file_type: str = Form(...),
    invoice_type: str = Form(...),
    sender: str = Form(...),
    subject: str = Form(...),
    email_listener_ts: str = Form(...)
):
    try:
        # Regular expression pattern to find "CE-" followed by digits
        match = re.search(r"CE-\d+", eml_path)
        # Extract mail_row_key if pattern is found, else assign None
        mail_row_key = match.group(0) if match else None

        # Log error if mail_row_key is None for better traceability
        if not mail_row_key:
            logger.warning(f"No mail_row_key found in eml_path: {eml_path}")
        db = next(get_db())
        existing_task = db.query(CorpQueueTask).filter_by(mail_row_key=mail_row_key).first()
        if existing_task:
            logger.info(f"Duplicate mail_row_key detected: {mail_row_key}. Skipping DB save.")
            return {
                "message": f"QueueTask with mail_row_key {mail_row_key} already exists. Skipping."
            }
        request_data = {
            "eml_path": eml_path,
            "mail_row_key": mail_row_key,
            "sender": sender,
            "subject": subject,
            "file_type": file_type,
            "invoice_type": invoice_type,
            "email_listener_ts": email_listener_ts,
            "operation_id": get_operation_id()
        }

        if settings.build_type == "debug":
            queued_status = f"{settings.local_user_name}-queued"
        else:
            queued_status = "queued"

        # Create a new CorpQueueTask with the extracted mail_row_key
        new_task = CorpQueueTask(
            request_data=request_data, status=queued_status, mail_row_key=mail_row_key
        )

        # Retry logic encapsulated in save_to_database
        task_id = save_to_database(new_task)

        return {
            "message": "QueueTask submitted successfully",
            "queue_task_id": task_id,
        }

    except (OperationalError, InvalidRequestError) as e:
        logger.error(f"Database error: {str(e)}")
        return {"message": "Failed to submit QueueTask due to a database issue"}

    except Exception as exc:
        logger.error(f"Unexpected error: {traceback.format_exc()}")
        return {"message": f"Failed to submit QueueTask: {str(exc)}"}



@router.get("/task_status/{queue_task_id}")
def get_task_status(
    queue_task_id: int,
    db=Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User"])),
    ):

    try:
        queue_task = db.query(CorpQueueTask).filter(CorpQueueTask.id == queue_task_id).first()
        if not queue_task:
            raise HTTPException(status_code=404, detail="QueueTask not found")
        return {
            "task_id": queue_task.id,
            "status": queue_task.status,
            "updated_at": queue_task.updated_at,
            "user_id": user.idUser
        }
    except Exception:
        logger.error(f"{traceback.format_exc()}")
        return {"message": "Failed to get QueueTask status"}


def parse_eml_from_data(data):
    """Parses an .eml file from raw binary data."""
    msg = BytesParser(policy=policy.default).parsebytes(data)
    return msg


# def extract_and_upload_attachments(email_msg, subfolder_name):
#     """Extracts only PDF attachments, uploads them to Azure Blob Storage with unique filenames, and returns a list of PDF filenames."""
#     pdf_filenames = []
#     seen_filenames = set()
    
#     for part in email_msg.iter_attachments():
#         if part.get_content_type() in ['application/pdf', 'application/octet-stream']:
#             filename = part.get_filename()
#             if filename:
#                 base, ext = os.path.splitext(filename)
#                 unique_filename = filename
#                 counter = 1
                
#                 while unique_filename in seen_filenames:
#                     unique_filename = f"{base}_{counter}{ext}"
#                     counter += 1
                
#                 seen_filenames.add(unique_filename)
#                 blob_path = f"{subfolder_name}/{unique_filename}"
                
#                 try:
#                     blob_data = part.get_payload(decode=True)
#                     upload_blob_securely(
#                         container_name="email-pdf-container",
#                         blob_path=blob_path,
#                         data=blob_data,
#                         content_type="application/pdf"
#                     )
#                     pdf_filenames.append(unique_filename)
#                 except Exception as e:
#                     logger.error(f"Error uploading blob: {e}")
    
#     logger.info(f"Total PDF attachments uploaded: {len(pdf_filenames)}")
#     return pdf_filenames if pdf_filenames else []

def sanitize_filename(filename, max_length=25):
    """Removes special characters from filename, replaces spaces with underscores, and limits length."""
    base, ext = os.path.splitext(filename)
    
    # Remove special characters, keep only letters, numbers, and underscores
    base_clean = re.sub(r'[^a-zA-Z0-9_]', '_', base)
    
    # Replace multiple underscores with a single underscore
    base_clean = re.sub(r'_+', '_', base_clean).strip('_')
    
    # Trim to max_length while keeping the extension
    if len(base_clean) > max_length:
        base_clean = base_clean[:max_length]
    
    return f"{base_clean}{ext}"

def extract_and_upload_attachments(email_msg, subfolder_name):
    """Extracts only PDF attachments, uploads them to Azure Blob Storage with unique filenames, and returns a list of PDF filenames."""
    pdf_filenames = []
    seen_filenames = set()
    
    for part in email_msg.iter_attachments():
        if part.get_content_type() in ['application/pdf', 'application/octet-stream']:
            filename = part.get_filename()
            if filename:
                sanitized_filename = sanitize_filename(filename)
                base, ext = os.path.splitext(sanitized_filename)
                
                unique_filename = sanitized_filename
                counter = 1
                
                while unique_filename in seen_filenames:
                    unique_filename = f"{base}_{counter}{ext}"
                    counter += 1
                
                seen_filenames.add(unique_filename)
                blob_path = f"{subfolder_name}/{unique_filename}"
                
                try:
                    blob_data = part.get_payload(decode=True)
                    upload_blob_securely(
                        container_name="email-pdf-container",
                        blob_path=blob_path,
                        data=blob_data,
                        content_type="application/pdf"
                    )
                    pdf_filenames.append(unique_filename)
                except Exception as e:
                    logger.error(f"Error uploading blob: {e}")
    
    logger.info(f"Total PDF attachments uploaded: {len(pdf_filenames)}")
    return pdf_filenames if pdf_filenames else []

def corp_queue_process_task(queue_task: CorpQueueTask):
    try:
        db = next(get_db())
        # Fetch the latest task status from the database
        existing_task = db.query(CorpQueueTask).filter(CorpQueueTask.id == queue_task.id).first()
        
        if existing_task and existing_task.status in ["completed"]:
            logger.info(f"Queue task {queue_task.id} is already {existing_task.status}. Skipping duplicate execution.")
            return existing_task.status  # Return the existing status to avoid duplicate processing
        
        operation_id = queue_task.request_data.get("operation_id", None)
        if operation_id:
            set_operation_id(operation_id)
        else:
            # set_operation_id(uuid.uuid4().hex)
            operation_id = uuid.uuid4().hex  # Generate a new operation_id
            set_operation_id(operation_id)  # Set the new operation_id
            logger.info(f"Generated new operation ID: {operation_id}")  # Log the new operation_id
            
        logger.info(f"Starting Queue task {queue_task.id} with operation ID: {operation_id}")
        # Define the container name and extract base_dir from queue_task
        # container_name = "apinvoice-mail-container"
        file_path = queue_task.request_data["eml_path"]
        sender = queue_task.request_data["sender"]
        mail_row_key = queue_task.request_data["mail_row_key"]

        # Parse the URL
        parsed_url = urlparse(file_path)
        # Extract the path and split it
        path_parts = parsed_url.path.strip("/").split("/", 1)
        # Get the container name and the rest of the path
        container_name = path_parts[0]
        
        # Remove the protocol and domain part
        blob_path = file_path.split('apinvoice-mail-container/')[1]
        # if "%20" in blob_path:
        #     logger.info("Replacing '%20' with space in the blob path.")
        #     blob_path = blob_path.replace("%20", " ")
        # logger.info(f"Retriveing blob data for {blob_path}")
        
        try:
            blob_path = unquote(blob_path)
            directory = os.path.dirname(blob_path)
            filename, ext = os.path.splitext(os.path.basename(blob_path))

            # Remove special characters (keep letters, numbers, and underscores)
            filename_clean = re.sub(r'[^a-zA-Z0-9_]', '_', filename)

            # Replace multiple underscores with a single underscore
            filename_clean = re.sub(r'_+', '_', filename_clean).strip('_')

            # Trim to max 50 characters while keeping the extension
            filename_clean = filename_clean[:25]

            # Construct the final blob path
            clean_blob_path = f"{directory}/{filename_clean}{ext}"
            logger.info(f"Retriveing blob data for {blob_path}")
            # Retrieve blob data and content type using get_blob_securely
            blob_data, content_type = get_blob_securely(container_name, blob_path)
            if blob_data:
                upload_blob_securely(
                        container_name="email-pdf-container",
                        blob_path=clean_blob_path,
                        data=blob_data,
                        content_type=content_type
                        )
        except Exception:
            logger.error(f"Error retrieving blob data for {blob_path}")
            logger.info(traceback.format_exc())
            status = "failed"
            return status
        # Check if the content type matches an email file (.eml)
        # if not content_type or not content_type.endswith("message/rfc822"):
        if not content_type or (not content_type.endswith("message/rfc822") and not content_type.endswith("application/octet-stream")):
            raise HTTPException(status_code=400, detail="The blob is not a valid .eml file")

        # Parse the .eml file directly from the blob data
        msg = parse_eml_from_data(blob_data)
        
        # Remove the file extension to get the subfolder
        subfolder_name = clean_blob_path.rsplit('.', 1)[0]
        try:
            # Extract attachments and upload them to Azure Blob Storage
            pdf_files = extract_and_upload_attachments(msg, subfolder_name)
        except Exception as e:
            logger.error(f"Error while extracting & uploading attachments: {traceback.format_exc()}")

        # # List all PDF files in the subfolder
        # blob_service_client = BlobServiceClient(account_url=f"https://{settings.storage_account_name}.blob.core.windows.net", credential=get_credential())
        # container_client = blob_service_client.get_container_client(container_name)

        # pdf_files = [
        # blob.name.split("/")[-1]
        # for blob in container_client.list_blobs(name_starts_with=f"{subfolder_name}/")  # Ensure it's inside the subfolder
        # if blob.name.endswith(".pdf")
        # ]
        invoice_detail_list = []
        mail_rw_dt ={}
        
        if not pdf_files:
            logger.info(f"No PDF files found in blob subfolder: {subfolder_name}")
            invoice_detail_list = []  # Return an empty list since no PDFs exist
            
            # # Insert initial record into corp_trigger_tab
            # new_trigger = model.corp_trigger_tab(
            #     corp_queue_id=queue_task.id,
            #     blobpath="",
            #     status="Invoice Missing",
            #     sender = sender,
            #     created_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            #     mail_row_key=mail_row_key
            # )
            # db.add(new_trigger)
            # db.commit()
            # db.refresh(new_trigger)
        else:
            # Loop through all PDF files and extract invoice details
            for pdf_filename in pdf_files:
                pdf_blob_path = f"{subfolder_name}/{pdf_filename}"
                try:
                    pdf_blob_data, _ = get_blob_securely("email-pdf-container", pdf_blob_path)
                    
                    # Insert initial record into corp_trigger_tab
                    new_trigger = model.corp_trigger_tab(
                        corp_queue_id=queue_task.id,
                        blobpath=pdf_blob_path,
                        status="File received",
                        sender = sender,
                        created_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        mail_row_key=mail_row_key
                    )
                    db.add(new_trigger)
                    db.commit()
                    db.refresh(new_trigger)
                except Exception as e:
                    print(f"Error getting blob data: {e}")
                    # Insert initial record into corp_trigger_tab
                    new_trigger = model.corp_trigger_tab(
                        corp_queue_id=queue_task.id,
                        blobpath=pdf_blob_path,
                        status="Blob Error",
                        sender = sender,
                        created_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        mail_row_key=mail_row_key
                    )
                    db.add(new_trigger)
                    db.commit()
                    db.refresh(new_trigger)
                    # continue  # Skip this file and move to the next one
                try:
                    corp_trigger_id = new_trigger.corp_trigger_id
                    mail_rw_dt[pdf_filename] = {"pdf_blob_path":pdf_blob_path,
                                                "corp_trigger_id":corp_trigger_id,
                                                "mail_row_key":mail_row_key}
                    mail_rw_dt[pdf_filename][corp_trigger_id] = mail_row_key

                    logger.info(f"Processing {pdf_filename} for OpenAI...")
                    invoice_data, total_pages, file_size_mb, status = extract_invoice_details_using_openai(pdf_blob_data)
                    logger.info(f"OpenAI Extracting invoice details completed for {pdf_filename}")
                    logger.info(f"OpenAI Status: {status}")
                    invoice_detail_list.append({pdf_filename: invoice_data})
                    # Update corp_trigger_tab record upon successful processing
                    new_trigger.pagecount = total_pages
                    new_trigger.filesize = file_size_mb
                    new_trigger.status = status
                    new_trigger.updated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    db.commit()
                except Exception as e:
                    print(f"Error processing {pdf_filename}: {e}")
                    # Update corp_trigger_tab record with OpenAI Error
                    new_trigger.status = "OpenAI Error"
                    new_trigger.updated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    db.commit()

        try:
            # Get Approver details from OpenAI
            approval_details, approval_status = extract_approver_details_using_openai(msg)
            logger.info(f"OpenAI Approval Details extraction Status: {approval_status}")
        except Exception:
            logger.error("Error while extracting approver details: ", traceback.format_exc())
            status = "error"

        # Extracting coding details from email content
        try:
            # Extract content from email
            extracted_data = crud.extract_content_from_eml_file(msg)

        except Exception:
            logger.error("Error while extracting content from email: ", traceback.format_exc())
            status = "error"

        # uncomment it later once wkhtmltoimage_path is set in azure
        # Convert a eml file to a html content 
        html_content = crud.extract_eml_to_html(blob_data)
        
        try:
            # Convert the html content to a base64 image
            base64_image = crud.html_to_base64_image(html_content)
            # base64_image = crud.html_to_base64_image(html_content, settings.wkhtmltoimage_path)
        except Exception as e:
            logger.error(f"Error converting html to base64 image: {traceback.format_exc()}")
        
        # Convert the base64 image to a pdf file and upload it blob container
        crud.dynamic_split_and_convert_to_pdf(base64_image, clean_blob_path)
        
        parsed_data = json.loads(extracted_data)
        # Clean all occurrences of '$\xa0' from the parsed_data
        parsed_data = crud.clean_parsed_data(parsed_data)
        logger.info(f"parsed_data after cleaning: {parsed_data}")
        # Check if any extra cleanup is needed, and only clean if necessary
        if crud.has_extra_empty_strings(parsed_data):
            parsed_data = crud.clean_tables_data(parsed_data)
        parsed_data = crud.filter_valid_tables(parsed_data)
        # Identify and format data based on template
        template_type = crud.identify_template(parsed_data)
        if template_type == 'Template 1':
            result = crud.format_data_for_template1(parsed_data)
        elif template_type == 'Template 2':
            result = crud.format_data_for_template2(parsed_data)
        elif template_type == 'Template 3':
            result = crud.format_data_for_template3(parsed_data)
        elif template_type == "Unknown Template":
            result = {
                        "email_metadata": { 
                            "from": msg['From'],
                            "sent": msg['date'],
                            "to": msg['To']
                        }, 

                        "invoiceDetails": { 
                            "store": [''], 
                            "dept": [''], 
                            "account": [''], 
                            "SL": [''],
                            "project": [''],
                            "activity": [''], 
                            "amount": [''], 
                            "invoice#": '', 
                            "GST": '', 
                            "invoicetotal": '', 
                        }, 
                        "approverDetails": { 
                            "approverName": '', 
                            "TMID": '', 
                            "title": '',
                        }
                    }
        try:
            final_json = json.loads(result)
        except Exception:
            final_json = result
        logger.info(f"final_json: {final_json}")
        content = {
            "template_type": template_type,
            "coding_details": final_json,
            "invoice_detail_list": invoice_detail_list,
            "all_attachments": pdf_files,
            "approval_details": approval_details,
        }
        logger.info(f"content for queue id: {queue_task.id}: {content}")
        
        # post processing:

        corp_postPro(content,mail_row_key,clean_blob_path,sender,mail_rw_dt,queue_task.id)

        # status = "success"
        # Fetch all statuses related to the queue_task.id
        trigger_records = db.query(model.corp_trigger_tab.status).filter(
            model.corp_trigger_tab.corp_queue_id == queue_task.id
        ).all()

        if trigger_records:
            # Extract statuses from the records
            # Extract statuses from the records
            statuses = [record.status for record in trigger_records]

            # Determine final status
            if all(status == "Processed" for status in statuses):
                status = "success"
            elif any(status == "Processed" for status in statuses):
                status = "partial-success"
            else:
                status = "failed"

        else:
            status = "failed"
        
        logger.info(f"Final status for queue_task {queue_task.id}: {status}")
        return status

    except Exception as e:
        logger.error(f"Error while processing corp_queue_task {queue_task.id}: {traceback.format_exc()}")
        status = "failed"
        return status
        # raise HTTPException(status_code=500, detail=str(e))

    

def corp_queue_worker(operation_id):
    while True:
        # logger.debug(f"Corp Queue worker started: {operation_id}")
        set_operation_id(operation_id)
        # logger.debug(f"Operation ID is set to {get_operation_id()}")
        try:
            # logger.debug("Attempting to get DB session...")
            db = next(get_db())
            # logger.debug("DB session acquired successfully.")
            # get the correct queue sattus for `queued` and lock it
            if settings.build_type == "debug":
                queued_status = f"{settings.local_user_name}-queued"
                processing_status = f"{settings.local_user_name}-processing"
                completed_status = f"{settings.local_user_name}-completed"
                partial_status = f"{settings.local_user_name}-partially-completed"
                failed_status = f"{settings.local_user_name}-failed"
            else:
                queued_status = "queued"
                processing_status = "processing"
                partial_status = "partially-completed"
                completed_status = "completed"
                failed_status = "failed"
                
            # logger.debug(f"Corp Queue statuses set: queued={queued_status}, processing={processing_status}")
            
            # Fetch a queue_task with status 'queued' and lock it
            # logger.debug("Fetching corp_queue_task from database...")
            # queue_task = (
            #     db.query(CorpQueueTask)
            #     .filter(CorpQueueTask.status == queued_status)
            #     .with_for_update(skip_locked=True)
            #     .first()
            # )
            lock_expiry_time = timedelta(minutes=5)  # Prevent tasks from being locked forever

            queue_task = (
                db.query(CorpQueueTask)
                .filter(CorpQueueTask.status == queued_status)
                .filter(
                    (CorpQueueTask.locked_at == None) | 
                    (CorpQueueTask.locked_at < datetime.utcnow() - lock_expiry_time)  # Prevent stale locks
                )
                .with_for_update(skip_locked=True)
                .first()
            )

            if queue_task:
                # Update the queue_task status to 'processing'
                # logger.debug(f"CorpQueueTask found: {queue_task.id}, updating status to {processing_status}")
                queue_task.status = processing_status
                queue_task.locked_at = datetime.utcnow()  # Update lock timestamp
                db.add(queue_task)
                db.commit()
                logger.info(f"CorpQueueTask {queue_task.id} locked at {queue_task.locked_at} and set to {processing_status}.")
                # logger.debug(f"QueueTask {queue_task.id} committed to database with status {processing_status}")
                # Process the queue_task
                try:
                    # logger.debug(f"Processing CorpQueueTask {queue_task.id}...")
                    status = corp_queue_process_task(queue_task)
                    logger.info(f"CorpQueueTask {queue_task.id} => {status}")
                    
                    queue_task_status = (
                        db.query(CorpQueueTask)
                        .filter(CorpQueueTask.id == queue_task.id)
                        .first()
                    )
                    
                    if status == "success":
                        queue_task_status.status = completed_status
                    elif status == "partial-success":
                        queue_task_status.status = partial_status
                    elif status == "failed":
                        queue_task_status.status = failed_status
                    else:
                        queue_task_status.status = failed_status
                    
                    # try:
                    #     mail_row_key = queue_task_status.mail_row_key
                    #     marked_status = mark_processed_mail_as_read(mail_row_key)
                    #     if marked_status == "success":
                    #         logger.info(f"Mail marked as read for CorpQueueTask {queue_task_status.id} successfully.")
                    #     else:
                    #         logger.error(f"Failed to mark mail as read for CorpQueueTask {queue_task_status.id} successfully.")    

                    # except Exception as e:
                    #     logger.error(f"Failed to mark mail as read for task {queue_task_status.id}: {e}")
                    # logger.info(f"CorpQueueTask {queue_task.id} updated to {queue_task.status}")
                    # load the queue task from db again to check if reflected
                    
                    logger.info(
                        f"CorpQueueTask {queue_task.id} => {queue_task.status} => {queue_task_status.status}"
                    )
                    db.add(queue_task_status)
                    db.commit()
                except Exception:
                    queue_task_failed_status = (
                        db.query(CorpQueueTask)
                        .filter(CorpQueueTask.id == queue_task.id)
                        .first()
                    )
                    queue_task_failed_status.status = failed_status
                    db.add(queue_task_failed_status)
                    db.commit()
                    logger.error(
                        f"CorpQueueTask {queue_task.id} failed: {traceback.format_exc()}"
                    )
        except Exception:
            logger.info(f"CorpQueueWorker failed: {traceback.format_exc()}")
        finally:
            db.close()
        time.sleep(1)  # Polling interval


@router.post("/corp_metadata/{v_id}")
def add_corp_metadata(
    v_id: str,
    metadata: CorpMetadataCreate,
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user),
    user: AzureUser = Depends(get_user_dependency(["CORP_ConfigPortal_User","Admin"])),
    ):
    if not metadata.dateformat:
        raise HTTPException(status_code=400, detail="dateformat is mandatory")
    return crud.create_or_update_corp_metadata(user.idUser, v_id, metadata, db)

@router.get("/getCorpMetaData/Vendor_id/{v_id}")
async def get_corp_metadata(
    v_id: str,
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user),
    user: AzureUser = Depends(get_user_dependency(["CORP_ConfigPortal_User","Admin"])),
):
    """API route to retrieve corp meta data based on the Vendor ID.

    Parameters:
    ----------
    v_id : int
        Vendor ID used to select the metadata and return its data.
    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    Dictionary containing the metadata data.
        
    """
    return await crud.get_metadata_data(user.idUser, v_id, db)

# API to check if a synonym (name or address) already exists across all corp_metadata records
@router.get("/check_duplicate_synonym/{synonym:path}")
async def check_duplicate_synonym(
    synonym: str,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["CORP_ConfigPortal_User","Admin"])),
    ):
    """
    API to check if a synonym (name or address) already exists across all corp_metadata records.
    :param synonym: The synonym to check.
    :param db: Database session.
    :return: Message indicating if synonym exists or not.
    """
    try:
        # Query all records where synonyms_name or synonyms_address is not NULL
        all_metadata = (
            db.query(model.corp_metadata)
            .filter(
                or_(
                    model.corp_metadata.synonyms_name.isnot(None),
                    model.corp_metadata.synonyms_address.isnot(None),
                )
            )
            .all()
        )

        for metadata in all_metadata:
            # Convert JSON string fields to lists if necessary
            synonyms_name_list = json.loads(metadata.synonyms_name) if isinstance(metadata.synonyms_name, str) else []
            synonyms_address_list = json.loads(metadata.synonyms_address) if isinstance(metadata.synonyms_address, str) else []

            # Normalize case for comparison
            synonym_lower = synonym.strip().lower()
            synonyms_name_lower = [s.strip().lower() for s in synonyms_name_list]
            synonyms_address_lower = [s.strip().lower() for s in synonyms_address_list]

            # Check if synonym exists in either list
            if synonym_lower in synonyms_name_lower or synonym_lower in synonyms_address_lower:
                vendor_name = metadata.vendorname if metadata.vendorname else "Unknown Vendor"
                return {
                    "status": "exists",
                    "message": f"Synonym '{synonym}' already exists for {vendor_name}.",
                }

        # If no match was found, synonym does not exist
        return {"status": "not exists", "message": "Synonym does not exist in corp_metadata."}
    except Exception as e:
        logger.error(f"Error in check_duplicate_synonyms: {e} => {traceback.format_exc()}")
        return {
            "status": "error",
            "message": f"Error in checking duplicate synonyms: {e}",
        }
# API to read paginated vendor list
@router.get("/paginatedcorpvendorlist")
async def read_paginated_corp_vendor_details(
    ven_code: Optional[str] = None,
    onb_status: Optional[str] = None,
    offset: int = 1,
    limit: int = 10,
    ven_status: Optional[str] = None,
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user),
    user: AzureUser = Depends(get_user_dependency(["CORP_ConfigPortal_User","Admin"])),
):
    """API route to retrieve a paginated list of vendors based on various
    filters.

    Parameters:
    ----------
    
    ven_code : str, optional
        Vendor code to filter by (default is None).
    onb_status : str, optional
        Onboarding status to filter vendors (default is None).
    offset : int
        The page number for pagination (default is 1).
    limit : int
        Number of records per page (default is 10).
    ven_status : str, optional
        Vendor status to filter vendors (default is None).
    vendor_type : str, optional
        Type of vendor to filter by (default is None).
    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    List of vendor data filtered and paginated according to the input parameters.
    """
    data = await crud.readpaginatedcorpvendorlist(
        user.idUser,
        db,
        (offset, limit),
        {"ven_code": ven_code, "onb_status": onb_status},
        ven_status,
    )
    return data

@router.get("/download-corp-vendors-excel/")
async def download_corp_vendors_excel(
    db: Session = Depends(get_db),
    ven_code: str = Query(None, description="Filter by vendor code"),
    onb_status: str = Query(None, description="Filter by onboarding status"),
    ven_status: str = Query(None, description="Filter by vendor status"),
    # user: AzureUser = Depends(get_user),
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
):
    """
    Endpoint to download vendor list as an Excel file with applied filters.
    """
    api_filter = {"ven_code": ven_code, "onb_status": onb_status}

    result = await crud.download_corp_vendor_list(user.idUser, db, api_filter, ven_status)
    
    if "error" in result:
        return Response(content=result["error"], status_code=400)
    
    df = pd.DataFrame([
        {
            **row["Vendor"],
            "OnboardedStatus": row.get("OnboardedStatus", "")
        } for row in result["data"]
    ])
    
    # Create an Excel file in memory
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Vendors")
    output.seek(0)

    headers = {
        "Content-Disposition": "attachment; filename=vendor_list.xlsx",
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }
    return Response(content=output.getvalue(), headers=headers, media_type=headers["Content-Type"])

@router.delete("/deleteSynonyms/{v_id}")
async def delete_synonyms(
    v_id: int,
    delmetadata: CorpMetadataDelete,
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user)
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
    
    ):
    """
    FastAPI endpoint to delete specific values from the synonyms_name or synonyms_address column.

    Parameters:
    ----------
    v_id : int
        Vendor ID used to identify the metadata record.
    column : str
        Column name ('synonyms_name' or 'synonyms_address').
    values_to_remove : list
        List of values to be removed from the specified column.
    db : Session
        Database session object.

    Returns:
    -------
    Dictionary with status message.
    """
    result = await crud.delete_metadata_values(user.idUser, v_id, delmetadata, db)
    if result:
        return {"message": "Synonyms deleted successfully"}
    return {"message": "Vendor metadata not found"}, 404


@router.get("/getEmailRowAssociatedFiles")
async def get_email_row_associated_files(
    offset: int = 1,
    limit: int = 10,
    uni_api_filter: Optional[str] = None,
    date_range: Optional[str] = None,
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user)
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
):
    """API route to retrieve a paginated list of invoice documents with line
    item details as optional when filters is applied  .

    Parameters:
    ----------

    offset : int
        The page number for pagination (default is 1).

    limit : int
        Number of records per page (default is 10).

    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    List of invoice documents filtered and paginated according to the input parameters.
    """

    docs = await crud.get_mail_row_key_summary(
        user.idUser, (offset, limit), db, uni_api_filter, date_range)
    return docs


# Checked - used in the frontend
@router.get("/readCorpInvoiceFile/idInvoice/{inv_id}")
def read_invoice_file_item(
    inv_id: int,
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user)
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
):
    """API route to retrieve invoice file data based on the invoice ID.

    Parameters:
    ----------
    inv_id : int
        Invoice ID used to select the document and return its data.
    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    Dictionary containing the following:
        - Base64-encoded PDF string
        - content_type
    """
    return crud.read_corp_doc_invoice_file(user.idUser, inv_id, db)

# Checked - used in the frontend
@router.get("/readCorpEmailPdfFile/idInvoice/{inv_id}")
def read_corp_email_pdf_file_item(
    inv_id: int,
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user)
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
):
    """API route to retrieve invoice file data based on the invoice ID.

    Parameters:
    ----------
    inv_id : int
        Invoice ID used to select the document and return its data.
    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    Dictionary containing the following:
        - Base64-encoded PDF string
        - content_type
    """
    return crud.read_corp_doc_email_pdf_file(user.idUser, inv_id, db)

@router.get("/readCorpInvoiceData/idInvoice/{inv_id}")
async def read_corp_invoice_data_item(
    inv_id: int,
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user)
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
):
    """API route to retrieve invoice document data based on the invoice ID.

    Parameters:
    ----------
    inv_id : int
        Invoice ID used to select the document and return its data.
    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    Dictionary containing the invoice document data, including:
        - Vendor details
        - Invoice Header details
        - Coding line details
        - upload time
    """
    return await crud.read_corp_invoice_data(user.idUser, inv_id, db)


# Checked - used in the frontend
@router.post("/updateCorpColumnPos")
async def update_corp_column_pos_item(
    col_data: List[schema.corpcolumnpos],
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user)
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
):
    """API route to update the column position for a user.

    Parameters:
    ----------
    bg_task : BackgroundTasks
        Background task manager for handling asynchronous tasks.
    col_data : List[columnpos]
        Body parameter containing a list of column positions
        represented as a Pydantic model.
    db : Session
        Database session object used to interact with the backend database.
    user : Depends(get_user)
        User object retrieved from the authentication system
        to identify the user making the request.

    Returns:
    -------
    dict
        A dictionary containing the result of the update operation,
        indicating success or failure.
    """
    return await crud.update_corp_column_pos(user.idUser, 1, col_data, db)


# Checked - used in the frontend
@router.get("/readCorpColumnPos")
async def read_corp_column_pos_item(
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user)
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
):
    """API route to read the column position for a user.

    Parameters:
    ----------
    db : Session
        Database session object used to interact with the backend database.
    user : Depends(get_user)
        User object retrieved from the authentication system
        to identify the user making the request.

    Returns:
    -------
    dict
        A dictionary containing the column position data for the specified tab.
    """
    data = await crud.read_corp_column_pos(user.idUser, 1, db)
    return data

@router.post("/updateCorpInvoiceData/idInvoice/{inv_id}")
async def update_corp_invoice_data_item(
    inv_id: int,
    inv_data: List[schema.UpdateCorpInvoiceData],
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user)
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
):
    """API route to update invoice document data.

    Parameters:
    ----------
    inv_id : int
        Invoice ID provided as a path parameter to identify
        which document to update.
    inv_data : List[UpdateServiceAccountInvoiceData]
        Body parameter containing a list of updated invoice
        data represented as a Pydantic model.
    db : Session
        Database session object used to interact with the
        backend database.
    user : Depends(get_user)
        User object retrieved from the authentication system
        to identify the user making the request.

    Returns:
    -------
    dict
        A dictionary containing the result of the update
        operation, indicating success or failure.
    """
    return await crud.update_corp_docdata(user.idUser, inv_id, inv_data, db)


@router.post("/upsertCorpCodingData/idInvoice/{inv_id}")
def upsert_corp_coding_data_item(
    inv_id: int,
    code_data: List[schema.UpdateCodinglineData],
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user)
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
):
    """API route to update invoice document data.

    Parameters:
    ----------
    inv_id : int
        Invoice ID provided as a path parameter to identify
        which document to update.
    inv_data : List[UpdateServiceAccountInvoiceData]
        Body parameter containing a list of updated invoice
        data represented as a Pydantic model.
    db : Session
        Database session object used to interact with the
        backend database.
    user : Depends(get_user)
        User object retrieved from the authentication system
        to identify the user making the request.

    Returns:
    -------
    dict
        A dictionary containing the result of the update
        operation, indicating success or failure.
    """
    return crud.upsert_coding_line_data(user.idUser, inv_id, code_data, db)


@router.get("/corp_validation/{inv_id}/{skipConf}")
async def corp_validation(
    inv_id: int,
    skipConf: str = "0",
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user),
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),

    # customCall: int = 0,
    # skipConf: int = 0,
):
    
    overall_status = validate_corpdoc(inv_id,user.idUser,skipConf,db)
    return overall_status


# API endpoint to handle the invoice creation request
@router.post(
    "/pushCorpPayloadToPST/{inv_id}"
)
async def push_corp_payload_to_pst(
    inv_id: int,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User", "Admin"])),
    ):
    try:
        # Process the request using the mock CRUD function
        logger.info(f"Processing invoice by user: {user.idUser}")
        response = crud.processCorpInvoiceVoucher(inv_id, db)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# API endpoint to handle the invoice status request
@router.post(
    "/updateCorpInvoiceStatus/{inv_id}",
    # response_model=InvoiceResponse
)
async def update_corp_invoice_status(
    inv_id: int,
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user)
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User", "User"])),
    ):
    try:
        # Process the request using the mock CRUD function
        response = crud.updateCorpInvoiceStatus(user.idUser, inv_id, db)
        return response
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}

@router.get("/corpjourneydoc/docid/{inv_id}")
async def download_corp_journey_doc(
    inv_id: int,
    download: bool = False,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User"])),
):
    """### API route to download journey document.

    It contains following parameters.
    :param inv_id: It is an path parameter for selecting document id to
        return its data, it is of int type.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: journey doc as pdf.
    """
    try:
        for f in os.listdir():
            if os.path.isfile(f) and f.endswith(".pdf"):
                os.unlink(f)
        all_status = await crud.read_corp_doc_history(inv_id, download, db)
        if download:
            filename = pdfcreator.createcorpdoc(all_status, inv_id)
            return FileResponse(
                path=filename, filename=filename, media_type="application/pdf"
            )
        else:
            return all_status
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error in downloading journey document: {e}",
        }
        


# API endpoint to handle the invoice creation request
@router.post(
    "/uploadMissingFile/{inv_id}"
)
async def upload_missing_file(
    inv_id: int,
    # filename: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user),
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
    ):
    # Get the container name and blob name from the request
    try:
        # Process the request using the mock CRUD function
        response = await crud.uploadMissingFile(user.idUser, inv_id, file, db)
        return response 
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# API endpoint to handle the invoice creation request
@router.post(
    "/uploadMissingEmailFile/{inv_id}"
)
async def upload_missing_email_file(
    inv_id: int,
    # filename: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user),
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
    ):
    # Get the container name and blob name from the request
    try:
        # Process the request using the mock CRUD function
        response = await crud.uploadMissingEmailFile(user.idUser, inv_id, file, db)
        return response 
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post(
    "/processMissingInvoiceFile/{inv_id}"
)
async def process_missing_invoice_file(
    inv_id: int,
    # filename: str = Form(...),
    blob_path: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user),
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
    ):
    # Get the container name and blob name from the request
    try:
        # Process the request using the mock CRUD function
        response = crud.processInvoiceFile(user.idUser, inv_id, blob_path, file, db)
        return response 
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Checked (new) - used in the frontend
@router.get("/readCorpPaginatedDocumentINVList")
async def read_corp_paginate_doc_inv_list(
    ven_id: Optional[int] = None,
    status: Optional[str] = None,  # Accept a colon-separated string
    offset: int = 1,
    limit: int = 10,
    date_range: Optional[str] = None,  # New parameter for start date
    uni_search: Optional[str] = None,
    ven_status: Optional[str] = None,
    sort_column: Optional[str] = None,  # New parameter for sorting column
    sort_order: Optional[str] = None,  # New parameter for sorting order
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user)
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
):
    """API route to retrieve a paginated list of invoice documents with line
    item details as optional when filters is applied  .

    Parameters:
    ----------
    ven_id : int, optional
        Vendor ID for filtering documents (default is None).
    status : Literal, optional
        Status of the invoice document to filter by.
        Options: 'posted', 'rejected', 'exception', 'VendorNotOnboarded',
        'VendorUnidentified' (default is None).
    offset : int
        The page number for pagination (default is 1).
    limit : int
        Number of records per page (default is 10).
    uni_search : str, optional
        Universal search term to filter documents (default is None).
    ven_status : str, optional
        Vendor status to filter documents (default is None).
    sort_column : str, optional
        The column to sort the results by (default is None).
        Available columns: 'docheaderID', 'VendorCode', 'VendorName', 'JournalNumber',
        'Store', 'Department', 'Status', 'SubStatus'.
    sort_order : str
        The sorting order ('asc' or 'desc', default is 'asc').
    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    List of invoice documents filtered and paginated according to the input parameters.
    """

    docs = await crud.read_corp_paginate_doc_inv_list(
        user.idUser,
        ven_id,
        status,
        (offset, limit),
        db,
        uni_search,
        ven_status,
        date_range,
        sort_column,
        sort_order,
    )
    return docs

@router.get("/downloadCorpDocumentList")
async def download_corp_document_list(
    ven_id: Optional[int] = None,
    status: Optional[str] = None,  # Accept a colon-separated string
    date_range: Optional[str] = None,
    uni_search: Optional[str] = None,
    ven_status: Optional[str] = None,
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user)
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
):
    """Endpoint to fetch document invoice data, convert it to Excel, and allow
    download.

    Parameters:
    -----------
    u_id : int
        User ID
    ven_id : int
        Vendor ID to filter
    inv_type : str
        Type of invoice ("ser" for service, "ven" for vendor)
    stat : Optional[str]
        Status filter
    uni_api_filter : Optional[str]
        Universal search filter for the invoice
    ven_status : Optional[str]
        Vendor status ("A" for active, "I" for inactive)
    db : Session
        Database session injected by FastAPI

    Returns:
    --------
    StreamingResponse
        An Excel file download of the filtered document data.
    """
    try:
        
        # Fetch the document data using the existing function
        result = await crud.download_corp_paginate_doc_inv_list(
            user.idUser, ven_id, status, date_range, db, uni_search, ven_status
        )

        # Check if result was successful
        if "ok" not in result or not result["ok"]["Documentdata"]:
            return {"error": "No document data found."}

        document_data = result["ok"]["Documentdata"]
        pst = pytz.timezone("America/Los_Angeles")
        # Extract data into a list of dictionaries to create the DataFrame
        extracted_data = []
        for doc in document_data:
            created_on_raw = doc.corp_document_tab.created_on
            updated_on_raw = doc.corp_document_tab.updated_on
            
            # created_on = pd.to_datetime(created_on_raw).tz_localize(None) if created_on_raw else None
            # updated_on = pd.to_datetime(updated_on_raw).tz_localize(None) if updated_on_raw else None
            # Convert to PST
            # Convert directly to PST if datetime is already aware
            created_on = (
                created_on_raw.astimezone(pst).replace(tzinfo=None)
                if created_on_raw else None
            )
            updated_on = (
                updated_on_raw.astimezone(pst).replace(tzinfo=None)
                if updated_on_raw else None
            )
            extracted_data.append(
                {
                    "Invoice Number": doc.corp_document_tab.invoice_id if doc.corp_document_tab else None,
                    "Vendor Name": doc.Vendor.VendorName if doc.Vendor else None,
                    "Vendor Code": doc.Vendor.VendorCode if doc.Vendor else None,
                    "Amount": doc.corp_document_tab.invoicetotal if doc.corp_document_tab else None,
                    "Invoice Date": doc.corp_document_tab.invoice_date if doc.corp_document_tab else None,
                    "Status": doc.DocumentStatus.status if doc.DocumentStatus else None,
                    "Sub Status": doc.DocumentSubStatus.status if doc.DocumentSubStatus else None,
                    "Sender": doc.corp_document_tab.sender if doc.corp_document_tab else None,
                    "Invoice Type": doc.corp_document_tab.document_type if doc.corp_document_tab else None,
                    "Approver Name": doc.corp_coding_tab.approver_name if doc.corp_coding_tab else None,
                    "Last Updated By": doc.last_updated_by if doc.last_updated_by else None,
                    "Updated On": updated_on,
                    "Mail Row Key": doc.corp_document_tab.mail_row_key if doc.corp_document_tab else None,
                    "Voucher ID": doc.corp_document_tab.voucher_id if doc.corp_document_tab else None,
                    "Uploaded On": created_on,
                }
            )

        # Convert the extracted data to a pandas DataFrame
        df = pd.DataFrame(extracted_data)

        # Create an in-memory Excel file using pandas and io
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="DocumentInvoices")

        output.seek(0)

        # Return the Excel file as a StreamingResponse for download
        headers = {"Content-Disposition": "attachment; filename=document_invoices.xlsx"}
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )
    except Exception:
        logger.error(f"Error in downloading document invoice list: {traceback.format_exc()}")
        return {"error": "Error in downloading document invoice list."}
    
    
@router.post("/retry-task/{task_id}")
def retry_task(
    task_id: int,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
    ):
    
    if settings.build_type == "debug":
        queued_status = f"{settings.local_user_name}-queued"
        processing_status = f"{settings.local_user_name}-processing"
    else:
        queued_status = "queued"
        processing_status = "processing"
    # Fetch the task from the database
    task = db.query(CorpQueueTask).filter(CorpQueueTask.id == task_id).first()

    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    if task.status == processing_status:
        raise HTTPException(status_code=400, detail="Task is already being processed")

    try:
        # Fetch previous records from corp_trigger_tab
        previous_records = db.query(model.corp_trigger_tab).filter(model.corp_trigger_tab.corp_queue_id == task.id).all()

        if previous_records:
            for record in previous_records:
                db.delete(record)
            db.commit()

        # Ensure task is in session before updating
        task = db.merge(task)
        task.status = queued_status
        db.commit()
        db.refresh(task)

        # Debugging logs to verify update
        logger.info(f"Task {task.id} updated to queued")
        print(f"Updated status: {task.status}")  # Debugging

        return {"message": "Task reset successfully", "task_id": task.id}

    except Exception as e:
        db.rollback()
        logger.error(f"Error retrying task: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Error retrying task: {str(e)}")



# Checked - used in the frontend
@router.post("/updateCorpRejectedDocumentStatus/{inv_id}")
async def update_corp_rejected_invoice_status(
    inv_id: int,
    reason: str,
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user),
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
):
    """API route to update the status of a rejected invoice.

    Parameters:
    ----------
    inv_id : int
        The ID of the invoice to update the status for.
    reason : str
        The reason for rejecting the invoice.
    db : Session
        Database session object used to interact with the backend database.
    user : AzureUser
        User object retrieved from the authentication system, used to identify the
        user making the request.

    Returns:
    -------
    dict
        A response indicating the success or failure of the operation.
    """
    return await crud.reject_corp_invoice(user.idUser, inv_id, reason, db)
class CodeMapRequest(BaseModel):
    lt: List[str]

# API route for getting code mapping status
@router.post("/pfg/get-code-map-status")
async def get_code_map_status_api(
    request_data: CodeMapRequest,  # Accepts JSON input
    db: Session = Depends(get_db),  # Injects DB session
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User", "User"]))  # Authenticated user
):
    # Call the function with the list of keys
    final_overallStats, cod_re_data = getCode_map_status(request_data.lt, db)

    return {
        "status": "success",
        "overall_stats": final_overallStats,
        "detailed_data": cod_re_data
    }

@router.get("/getUnmappedCodingTabDetails")
def get_unmapped_coding_tab_details(
    invoice_id: str,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User", "User"])),
):
    """
    Retrieve all records from corp_coding_tab filtered by mail_rw_key and map_type = "unmapped".
    
    Args:
        mail_rw_key (str): The mail_rw_key to filter records.
        db (Session): Database session dependency.
        user (AzureUser): The authenticated user dependency.
    
    Returns:
        List[corp_coding_tab]: A list of matching corp_coding_tab records.
    """
    return crud.get_associated_coding_tab_details(user.idUser, invoice_id, db)
    
@router.post("/mapCodingDetailsByCorpDocId")
def mapCodingDetailsByCorpDocId(
    inv_id: int,
    corp_coding_id: int,
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user)
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
):
    """
    API endpoint to map coding details to invoice
    Args:
        inv_id (int): The invoice ID.
        corp_coding_id (int): The corp_coding_id to map.
        db (Session): Database session dependency.
        user (AzureUser): The authenticated user dependency.
    
    Returns:
        Message: A message indicating success or failure.

    
    """
    return crud.map_coding_details_by_corp_doc_id(user.idUser, inv_id, corp_coding_id, db)


@router.post("/setMapTypeToUserReviewed")
def setMapTypeToUserReviewed(
    corp_coding_id: int,
    db: Session = Depends(get_db),
    # user: AzureUser = Depends(get_user)
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"])),
):
    """
    API endpoint to map coding details to invoice
    Args:
        inv_id (int): The invoice ID.
        corp_coding_id (int): The corp_coding_id to map.
        db (Session): Database session dependency.
        user (AzureUser): The authenticated user dependency.
    
    Returns:
        Message: A message indicating success or failure.

    
    """
    return crud.set_map_type_to_user_reviewed(user.idUser, corp_coding_id, db)


# API to read all vendor names
@router.get("/corpvendornamelist")
async def get_corp_vendor_names_list(
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["DSD_ConfigPortal_User","DSD_APPortal_User","CORP_APPortal_User"])),
    ):
    """API route to retrieve a list of all active vendor names.

    Parameters:
    ----------

    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    List of active vendor names.
    """
    return await crud.readcorpvendorname(user.idUser, db)


@router.post("/set_corp_metadata_status/{v_id}")
def setCorpMetadataStatus(
    v_id: str,
    action: str,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["CORP_ConfigPortal_User","Admin"])),
    ):
    
    return crud.set_corp_metadata_status(user.idUser, v_id, db, action)

@router.post("/reset-processing-tasks")
def reset_processing_tasks(
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["CORP_APPortal_User","User"]))
    ):
    """
    Reset 'processing' corp queue tasks (last 2 days) back to 'queued'
    if none of their corp_trigger_tab records are in 'processed' status.
    """
    return crud.reset_stuck_corp_queue_tasks(user.idUser, db)

# # API endpoint to handle the invoice status request
# @router.post(
#     "/bulkupdatecorpinvoicestatus",
#     # response_model=InvoiceResponse
# )
# async def bulk_update_corp_invoice_status(db: Session = Depends(get_db)):
#     try:
#         # Process the request using the mock CRUD function
#         response = crud.bulkupdateCorpInvoiceStatus(db)
#         return response
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# # API endpoint to handle the invoice status request
# @router.post(
#     "/bulkprocesscorpvoucherdata",
#     # response_model=InvoiceResponse
# )
# async def bulk_process_corp_voucher_data(db: Session = Depends(get_db)):
#     try:
#         # Process the request using the mock CRUD function
#         response = crud.bulkProcessCorpVoucherData(db)
#         return response
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
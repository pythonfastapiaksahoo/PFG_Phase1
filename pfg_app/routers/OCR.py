import io
import json
import re
import time
import traceback
from datetime import datetime, timezone
from functools import wraps
from urllib.parse import urlparse
import uuid

import Levenshtein
import pandas as pd

# import psycopg2
import pytz as tz
from fastapi import APIRouter, Depends, File, Form, HTTPException, Response, UploadFile
from PIL import Image

# from psycopg2 import extras
from pypdf import PdfReader
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import func, update
from sqlalchemy.exc import InvalidRequestError, OperationalError
from sqlalchemy.orm import Load

import pfg_app.model as model
from pfg_app import settings
from pfg_app.auth import AuthHandler

# from pfg_app.azuread.auth import get_user
# from pfg_app.azuread.schemas import AzureUser
from pfg_app.core.azure_fr import get_fr_data
from pfg_app.core.stampData import VndMatchFn_2, is_valid_date
from pfg_app.core.utils import get_blob_securely
from pfg_app.FROps.pfg_trigger import (
    IntegratedvoucherData,
    nonIntegratedVoucherData,
    pfg_sync,
)
from pfg_app.FROps.postprocessing import getFrData_MNF, postpro
from pfg_app.FROps.preprocessing import fr_preprocessing
from pfg_app.FROps.SplitDoc import splitDoc
from pfg_app.FROps.validate_currency import validate_currency
from pfg_app.logger_module import get_operation_id, logger, set_operation_id
from pfg_app.model import QueueTask
from pfg_app.crud.InvoiceCrud import update_docHistory

from pfg_app.session.session import SQLALCHEMY_DATABASE_URL, get_db

auth_handler = AuthHandler()
tz_region = tz.timezone("US/Pacific")

router = APIRouter(
    prefix="/apiv1.1/ocr",
    tags=["Live OCR"],
    # dependencies=[Depends(auth_handler.auth_wrapper)],
    responses={404: {"description": "Not found"}},
)

docLabelMap = {
    "InvoiceTotal": "totalAmount",
    "InvoiceId": "docheaderID",
    "InvoiceDate": "documentDate",
    "PurchaseOrder": "PODocumentID",
}


status_stream_delay = 1  # second
status_stream_retry_timeout = 30000  # milisecond

common_terms = {
    "ltd",
    "inc",
    "co",
    "corp",
    "corporation",
    "company",
    "limited",
}


# Clean the vendor names by removing punctuation, converting to lowercase, and filtering out common terms
def clean_vendor_name(name):
    # Convert to lowercase
    name = name.lower()
    # Remove punctuation
    name = re.sub(r"[^\w\s]", "", name)
    # Split name into words, remove common terms, and rejoin
    name = " ".join(word for word in name.split() if word not in common_terms)
    return name


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
            mail_row_key = f"DSD-{10000000 + task_id}"

            # Update the field in the JSONB column
            new_task.request_data["mail_row_key"] = mail_row_key

        # Commit the changes (flush already staged the changes for this session)
        db.commit()
        return task_id
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@router.post("/status/stream_pfg")
def runStatus(
    file_path: str = Form(...),
    filename: str = Form(...),
    file_type: str = Form(...),
    source: str = Form(...),
    invoice_type: str = Form(...),
    sender: str = Form(...),
    file: UploadFile = File(...),
    email_path: str = Form("Test Path"),
    subject: str = Form(...),
    # user: AzureUser = Depends(get_user),
    # db=Depends(get_db),
):
    
    try:
        # Regular expression pattern to find "DSD-" followed by digits
        match = re.search(r"/DSD-\d+/", file_path)
        # Extract mail_row_key if pattern is found, else assign None
        mail_row_key = match.group(0).strip("/") if match else None

        request_data = {
            "file_path": file_path,
            "filename": filename,
            "file_type": file_type,
            "source": source,
            "invoice_type": invoice_type,
            "sender": sender,
            "email_path": email_path,
            "mail_row_key": mail_row_key,
            "subject": subject,
            "operation_id": get_operation_id()
        }
        if settings.build_type == "debug":
            queued_status = f"{settings.local_user_name}-queued"
        else:
            queued_status = "queued"
        new_task = QueueTask(request_data=request_data, status=queued_status)
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
def get_task_status(queue_task_id: int, db=Depends(get_db)):

    try:
        queue_task = db.query(QueueTask).filter(QueueTask.id == queue_task_id).first()
        if not queue_task:
            raise HTTPException(status_code=404, detail="QueueTask not found")
        return {
            "task_id": queue_task.id,
            "status": queue_task.status,
            "updated_at": queue_task.updated_at,
        }
    except Exception:
        logger.error(f"{traceback.format_exc()}")
        return {"message": "Failed to get QueueTask status"}


def queue_process_task(queue_task: QueueTask):
    try:
        operation_id = queue_task.request_data.get("operation_id", None)
        if operation_id:
            set_operation_id(operation_id)
        else:
            set_operation_id(uuid.uuid4().hex)
        # Simulate task processing

        logger.info(f"Starting Queue task: {queue_task.id}")

        # from the request data of queue_task, extract the file_path and other required data
        file_path = queue_task.request_data["file_path"]
        filename = queue_task.request_data["filename"]
        file_type = queue_task.request_data["file_type"]
        source = queue_task.request_data["source"]
        invoice_type = queue_task.request_data["invoice_type"]
        sender = queue_task.request_data["sender"]
        email_path = queue_task.request_data["email_path"]
        subject = queue_task.request_data["subject"]
        mail_row_key = queue_task.request_data["mail_row_key"]

        vendorAccountID = 0
        vendorID = 0
        CreditNote = "Invoice Document"

        db = next(get_db())
        # Create a new instance of the SplitDocTab model
        new_split_doc = model.SplitDocTab(
            invoice_path=file_path,
            status="File Received without Check",
            emailbody_path=email_path,
            email_subject=subject,
            sender=sender,
            mail_row_key=mail_row_key,
            updated_on=datetime.now(tz_region),
            created_on=datetime.now(tz_region),
        )

        # Add the new entry to the session
        db.add(new_split_doc)

        # Commit the transaction to save it to the database
        db.commit()

        # Refresh the instance to get the new ID if needed
        db.refresh(new_split_doc)
        splitdoc_id = new_split_doc.splitdoc_id
        # if the execution is from `debug` mode, then get the file from the local path
        if settings.build_type == "debug":
            with open(file_path, "rb") as f:
                blob_data = f.read()
        else:
            # Download the file from the blob storage using get_blob_securely function
            # Parse the URL
            parsed_url = urlparse(file_path)
            # Extract the path and split it
            path_parts = parsed_url.path.strip("/").split("/", 1)
            # Get the container name and the rest of the path
            container_name = path_parts[0]
            rest_of_path = path_parts[1] if len(path_parts) > 1 else ""
            blob_data, content_type = get_blob_securely(container_name, rest_of_path)

        fl_type = filename.split(".")[-1]

        if fl_type in ["png", "jpg", "jpeg", "jpgx"]:
            image = Image.open(io.BytesIO(blob_data))

            # Convert the image to RGB if it's not in RGB mode
            # (important for saving as PDF)
            if image.mode in ("RGBA", "P", "L"):
                image = image.convert("RGB")

            pdf_bytes = io.BytesIO()

            image.save(pdf_bytes, format="PDF")
            pdf_bytes.seek(0)

            # Read the PDF using PyPDF2 (or any PDF reader you prefer)
            pdf_stream = PdfReader(pdf_bytes)
        elif fl_type in ["pdf"]:
            pdf_stream = PdfReader(io.BytesIO(blob_data))
        else:
            raise Exception(f"Unsupported File Format: {fl_type}")
        
        
        try:
            modelData = None
            IsUpdated = 0
            invoId = ""
            userID = 1

            logger.info(
                f"file_path: {file_path}, filename: {filename}, file_type: {file_type},\
                source: {source}, invoice_type: {invoice_type}"
            )

            containername = "invoicesplit-test"  # TODO move to settings
            subfolder_name = "DSD/splitInvo"  # TODO move to settings
            destination_container_name = "apinvoice-container"  # TODO move to settings
            fr_API_version = "2023-07-31"  # TODO move to settings

            prompt = """
                The provided image contains invoice ID, vendor name, vendor address, and a stamp sealed with handwritten or stamped information, possibly
                including a receiver's stamp. Extract the relevant information and format it as a list of JSON objects, adhering strictly to the structure provided below:

                {
                    "StampFound": "Yes/No",
                    "CreditNote": "Yes/No",
                    "NumberOfPages": "Total number of pages in the document",
                    "MarkedDept": "Extracted Circled department only",
                    "Confirmation": "Extracted confirmation number",
                    "ReceivingDate": "Extracted receiving date",
                    "Receiver": "Extracted receiver information",
                    "Department": "Extracted department code or name",
                    "Store Number": "Extracted store number",
                    "VendorName": "Extracted vendor name",
                    "VendorAddress": "Extracted vendor address",
                    "InvoiceID": "Extracted invoice ID",
                    "Currency": "Extracted currency",
                    "GST_HST_Found": "Yes/No",
                    "GST_HST_Amount": "extracted amount"
                }

                ### Instructions:
            1. **Orientation Correction**: Check if the invoice orientation is portrait or landscape. If its landscape, rotate it to portrait to extract stamp data correctly.
            2. **Data Extraction**: Extract only the information specified:
            - **Invoice Document**: Yes/No
            - **CreditNote**: Yes/No
            - **Invoice ID**: Extracted Invoice ID from invoice document (excluding 'Sold To', 'Ship To', or 'Bill To' sections)
            - **Vendor Name**:  Extracted vendor name from invoice document (excluding 'Sold To', 'Ship To', or 'Bill To' sections).
                                Ensure to capture the primary vendor name typically found at the top of the document. If "Starbucks Coffee Canada, Inc" is present on the invoice with any other vendor name, extract "Starbucks Coffee Canada, Inc" only.
                                If "Starbucks Coffee Canada, Inc" is not present on the invoice, do not guess or assume it.
                                if "Freshpoint Nanaimo" is present on the invoice with any other vendor name, extract "Freshpoint Nanaimo" only not "Freshpoint Vancouver".
                                if "Centennial" is present on the invoice with any other vendor name, extract "Centennial FoodService"
                                if "Alsco Canada Corporation 2992 88 Ave Surrey" is present on the invoice with any other vendor name, extract "Alsco Canada Corporation" only
                                if "Alsco Canada Corporation 91 Comox Rd" is present on the invoice with any other vendor name, extract "Alsco Canada Corp" only.
                                if SYSCO Canada, Inc Vancouver is present on the invoice with any other vendor name, extract "SYSCO FOOD SERVICES" only.
                                if SYSCO Canada, Inc Edmonton is present on the invoice with any other vendor name, extract "SYSCO FOOD (EDMONTON)" only.
                                if SYSCO Canada, Inc Calgary is present on the invoice with any other vendor name, extract "SYSCO CALGARY" only.
                                if SYSCO Canada, Inc Victoria is present on the invoice with any other vendor name, extract "SYSCO" only.
                                Return "N/A" if the vendor name is not present in the invoice document.
            - **Vendor Address**: Extracted vendor address from invoice document.
            - **Stamp Present**: Yes/No
            - If a stamp is present, extract the following information:
            - **Store Number**: extract the store number only if its clearly visible and starting with either 'STR#' or '#' or 'Urban Fare #'.Ensure the store number can be three or four digits only. If it is more than four digits, return "N/A"
            - **MarkedDept**: Extract the clearly circled marked keyword "Inventory" or "INV" or "Supplies" or "SUP" from the stamp image.
            Ensure that it must return either "Inventory" or "Supplies" and should not extract anything else. If no department is circled or it's missing, then return "Inventory" as the default only. Ensure not to return "N/A" or any other value.
            - **Department**: Extract either a department code or department name, handwritten
                    and possibly starting with "Dept"
            - **Receiving Date**: extract the date of receipt from the stamp image, if it is visible and in a recognizable format.
                    If not, leave it blank.
            - **Receiver**: The name or code of the person who received the goods (may appear as "Receiver#" or by name).
            - **Confirmation Number**: A 9-digit number, usually handwritten and labeled as "Confirmation"., if it is visible.
                    If not, leave it as "N/A".
            - **Currency**: Identified by currency symbols (e.g., CAD, USD). If the currency is not explicitly identified as USD, default to CAD.
            - **GST_HST_Found** : Yes/No
            - **GST_HST_Amount**: extract the value listed next to 'GST/HST [any amount] @ 5%' from the bottom left side of the Starbucks's invoice .
                For example, if the line 'GST/HST 44.69 @ 5%' is present, extract the value '2.23'. Note that the amount before '@ 5%'  and next to it can vary.

            3. **Special Notes**:
            - *Marked Department*: The department may be labeled as "Inventory," "INV," "Supplies," or "SUP." Ensure that you identify the circled text accurately.
                    - If the circle starts with the word "Inventory" and ends with the starting character of "Supplies", extract "Inventory".
                    - If the circle starts with the last character of "Inventory" and covers "Supplies" completely, extract "Supplies".
                    - If the circle exactly encloses the word "Inventory" extract "Inventory"
                    - If the circle exactly encloses the word "Supplies" extract "Supplies"
                    - If the circle encloses the word "INV," extract "Inventory"
                    - If the circle encloses the word "SUP," extract "Supplies"
                - **Confirmation Number** : Extract the confirmation number labeled as 'Confirmation' from the stamp seal in the provided invoice image.
                    - The confirmation number must be a handwritten number only.
                    - Please account for potential obstacles such as table description lines that may cut through the number, poor handwriting, or low-quality stamp impressions.
                    - Apply image enhancement techniques such as increasing contrast to clarify obscured digits.
                    - Ensure the extracted number is accurate and complete. If there are any ambiguities or unclear digits.
                    - if the confirmation number is not present, return "N/A"
                - **Receiver** : Extract it only if keyword "Receiver#" exist before the receiver code or name.
                - **Vendor Name:** : Don't consider the vendor name from 'Sold To' or 'Ship To' or 'Bill To' section.
                    - Ensure to capture the primary vendor name typically found at the top of the document.
                    - If "Starbucks Coffee Canada, Inc" is present on the invoice with any other vendor name, extract "Starbucks Coffee Canada, Inc" only.
                    - If "Starbucks Coffee Canada, Inc" is not present on the invoice, do not guess or assume it.
                    - Return "N/A" if the vendor name is not present in the invoice document.
                - **Vendor Address:** : Don't consider the vendor address from 'Sold To' or 'Ship To' or 'Bill To' section
                - **Currency**: Must be three character only as 'CAD' or 'USD'. If it's unclear kept it as 'CAD' as default.
                - **Credit Note**:  May have 'CREDIT MEMO' written on the invoice with or without Negative Amount.
                    - If the invoice has 'CREDIT MEMO' written on it or if total amounts are negative, return 'Yes'
                    - If the invoice does not have 'CREDIT MEMO' written on it, return 'No'
                    - If the invoice has 'CREDIT MEMO' written on it, but the amount is negative, return 'Yes'
                    - Don't consider Discounts as it is not a Credit Note.
                - **GST_HST_Found** : Only if Vendor Name is 'Starbucks Coffee Canada, Inc' then identify if 'GST/HST [any amount] @ 5%' is present in the bottom left side of the Starbucks's invoice return 'Yes'.
                    -  if its other than @ 5% then return 'No'. For example, 'GST/HST [any amount] @ 0%' or 'GST/HST [any amount] @ 10%'.
            	    - **GST_HST_Amount**: extract the value listed next to 'GST/HST [any amount] @ 5%' from the bottom left side of the Starbucks's invoice . For example, if the line 'GST/HST 44.69 @ 5%' is present, extract the value '2.23'.
		            - Note that the amount before '@ 5%'  and next to it can vary.
            4. **Output Format**: Ensure that the JSON output is precise and clean, without any extra text or commentary like ```json```,  it will be processed using json.loads.

            ### Example Output:
            If the extracted text includes:
            - MarkedDept: "Inventory"
            - Confirmation: "123456789"
            - ReceivingDate: "2023-01-01"
            - Receiver: "John Doe"
            - Department: "30"
            - Store Number: "1981"
            - VendorName: "ABC Company"
            - VendorAddress: "123 Main St, Anytown CANADA"
            - InvoiceID: "INV-12345"
            - Currency: "CAD"
            - GST_HST_Found: "No"  
            - GST_HST_Amount: "0.0" 

            The expected output should be:
            {
                "StampFound": "Yes",
                "CreditNote": "Yes/No",
                "NumberOfPages": "1",
                "MarkedDept": "Inventory",
                "Confirmation": "123456789",
                "ReceivingDate": "2023-01-01",
                "Receiver": "John Doe",
                "Department": "30",
                "Store Number": "1981",
                "VendorName": "ABC Company",
                "VendorAddress": "123 Main St, Anytown USA",
                "InvoiceID": "INV-12345",
                "Currency": "CAD",
                "GST_HST_Found": "No",  
                "GST_HST_Amount": "0.0" 
            }

            """

            try:
                (
                    prbtHeaders,
                    grp_pages,
                    splitfileNames,
                    num_pages,
                    StampDataList,
                    rwOcrData,
                    fr_model_status,
                    fr_model_msg,
                    fileSize,
                ) = splitDoc(
                    pdf_stream,
                    subfolder_name,
                    destination_container_name,
                    prompt,
                    settings.form_recognizer_endpoint,
                    fr_API_version,
                )
            except Exception:
                logger.error(f"Error in splitDoc: {traceback.format_exc()}")
                # splitdoc_id = new_split_doc.splitdoc_id
                split_doc = (
                    db.query(model.SplitDocTab)
                    .filter(model.SplitDocTab.splitdoc_id == splitdoc_id)
                    .first()
                )
                if split_doc:
                    # Update the fields
                    split_doc.status = "Error - Attachment Processing Failed"
                    split_doc.updated_on = datetime.now(tz_region)  # Update the timestamp

                    # Commit the update
                    db.commit()
                #raise Exception("Failed to split the document")
        
            if fr_model_status == 1:

                query = db.query(
                    model.Vendor.idVendor,
                    model.Vendor.VendorName,
                    model.Vendor.Synonyms,
                    model.Vendor.Address,
                    model.Vendor.VendorCode,
                ).filter(
                    func.jsonb_extract_path_text(
                        model.Vendor.miscellaneous, "VENDOR_STATUS"
                    )
                    == "A"
                )
                rows = query.all()
                columns = ["idVendor", "VendorName", "Synonyms", "Address", "VendorCode"]

                vendorName_df = pd.DataFrame(rows, columns=columns)

                # splitdoc_id = new_split_doc.splitdoc_id
                split_doc = (
                    db.query(model.SplitDocTab)
                    .filter(model.SplitDocTab.splitdoc_id == splitdoc_id)
                    .first()
                )
                print("grp_pages: ", grp_pages)
                if split_doc:
                    # Update the fields
                    split_doc.pages_processed = grp_pages
                    split_doc.status = "File received but not processed"
                    split_doc.totalpagecount = num_pages
                    split_doc.num_pages = num_pages
                    split_doc.updated_on = datetime.now(tz_region)  # Update the timestamp

                    # Commit the update
                    db.commit()

                fl = 0
                spltinvorange = []
                for m in range(len(splitfileNames)):
                    spltinvorange.append(m)

                splt_map = []
                for splt_i, (start, end) in enumerate(grp_pages):
                    splt_ = spltinvorange[splt_i]
                    splt_map.append(splt_)

                grp_pages = sorted(grp_pages)
                for spltInv in grp_pages:
                    vectorizer = TfidfVectorizer()
                    # hdr = [spltInv[0] - 1][0]  # TODO: Unused variable
                    # ltPg = [spltInv[1] - 1][0]  # TODO: Unused variable
                    vdrFound = 0
                    spltFileName = splitfileNames[splt_map[fl]]
                    logger.info(f"spltFileName: {spltFileName}")
                    try:
                        InvofileSize = fileSize[spltFileName]
                        logger.info(f"InvofileSize: {InvofileSize}")
                    except Exception:
                        logger.error(f"{traceback.format_exc()}")
                        InvofileSize = ""
                    try:

                        frtrigger_insert_data = {
                            "blobpath": spltFileName,
                            "status": "File received but not processed",
                            "sender": sender,
                            "splitdoc_id": splitdoc_id,
                            "page_number": spltInv,
                            "filesize": str(InvofileSize),
                        }
                        fr_db_data = model.frtrigger_tab(**frtrigger_insert_data)
                        db.add(fr_db_data)
                        db.commit()

                    except Exception:
                        logger.error(f"{traceback.format_exc()}")

                    if "VendorName" in prbtHeaders[splt_map[fl]]:
                        logger.info(f"DI prbtHeaders: {prbtHeaders}")
                        di_inv_vendorName = prbtHeaders[splt_map[fl]]["VendorName"][0]
                        # di_inv_vendorName = inv_vendorName
                        logger.info(f" DI inv_vendorName: {di_inv_vendorName}")
                    else:
                        di_inv_vendorName = ""

                    if "VendorName" in StampDataList[splt_map[fl]]:

                        stamp_inv_vendorName = StampDataList[splt_map[fl]]["VendorName"]
                        logger.info(f" openAI inv_vendorName: {stamp_inv_vendorName}")
                    else:
                        stamp_inv_vendorName = ""

                    try:
                        # output_data = rwOcrData[hdr]  # TODO: Unused variable
                        # Dictionary to store similarity scores
                        similarity_scores = {}
                        spltFileName = splitfileNames[fl]

                        try:
                            stop = False
                            vdrFound = 0
                            try:
                                for v_id, vendorName in zip(
                                    vendorName_df["idVendor"],
                                    vendorName_df["VendorName"],
                                ):
                                    if stop:
                                        break

                                    vName_lower = str(stamp_inv_vendorName)
                                    vendorName_lower = str(vendorName)
                                    # Cleaned versions of the vendor names
                                    vName_lower = clean_vendor_name(vName_lower).lower()
                                    vendorName_lower = clean_vendor_name(
                                        vendorName_lower
                                    ).lower()  # noqa: E501
                                    similarity = Levenshtein.ratio(
                                        vName_lower, vendorName_lower
                                    )
                                    # logger.info("Similarity
                                    # (vName_lower vs vendorName_lower):", similarity)
                                    # Check if similarity is 80% or greater
                                    if similarity * 100 >= 90:
                                        similarity_scores[v_id] = {
                                            "vendor_name": vendorName,
                                            "similarity": similarity,
                                        }
                                # Check for the vendor with the highest similarity
                                if similarity_scores:
                                    logger.info(f"similarity_scores: {similarity_scores}")
                                    best_match_id = max(
                                        similarity_scores,
                                        key=lambda x: similarity_scores[x]["similarity"],
                                    )  # noqa: E501
                                    best_match_info = similarity_scores[best_match_id]
                                    best_vendor = best_match_info["vendor_name"]
                                    best_similarity_score = best_match_info["similarity"]

                                    # Check if the best similarity is 95% or greater
                                    if best_similarity_score * 100 >= 90:
                                        vdrFound = 1
                                        vendorID = best_match_id
                                        logger.info(
                                            f"Vendor match found: {best_vendor} using Levenshtein similarity with accuracy: {best_similarity_score * 100:.2f}%"  # noqa: E501
                                        )
                                        stop = True
                                else:
                                    logger.info(
                                        "Vendor Name match not found using Levenshtein model"
                                    )
                            except Exception:
                                try:
                                    fr_trigger = db.query(model.frtrigger_tab).filter
                                    (model.frtrigger_tab.blobpath == spltFileName)

                                    # Step 2: Perform the update operation
                                    fr_trigger.update(
                                        {
                                            model.frtrigger_tab.status: "OpenAI Timeout Error",  # noqa: E501
                                        }
                                    )
                                    # Step 3: Commit the transaction
                                    db.commit()

                                except Exception:
                                    logger.error(f"{traceback.format_exc()}")

                            try:
                                for syn, v_id, vendorName in zip(
                                    vendorName_df["Synonyms"],
                                    vendorName_df["idVendor"],
                                    vendorName_df["VendorName"],
                                ):
                                    if stop:
                                        break
                                        # print("syn: ",syn,"   v_id: ",v_id)

                                    # if (syn is not None or str(syn) != "None") and (vdrFound == 0):
                                    if syn and syn.strip().lower() != "none" and vdrFound == 0:
                                        synlt = json.loads(syn)
                                        if isinstance(synlt, list):
                                            for syn1 in synlt:
                                                if stop:
                                                    break
                                                syn_1 = syn1.split(",")

                                                for syn2 in syn_1:
                                                    if stop:
                                                        break
                                                    if len(di_inv_vendorName) > 0:
                                                        tfidf_matrix_di = (
                                                            vectorizer.fit_transform(
                                                                [syn2, di_inv_vendorName]
                                                            )
                                                        )
                                                        cos_sim_di = cosine_similarity(
                                                            tfidf_matrix_di[0],
                                                            tfidf_matrix_di[1],
                                                        )

                                                    tfidf_matrix_stmp = (
                                                        vectorizer.fit_transform(
                                                            [syn2, stamp_inv_vendorName]
                                                        )
                                                    )
                                                    cos_sim_stmp = cosine_similarity(
                                                        tfidf_matrix_stmp[0],
                                                        tfidf_matrix_stmp[1],
                                                    )
                                                    if len(di_inv_vendorName) > 0:
                                                        if cos_sim_di[0][0] * 100 >= 95:
                                                            vdrFound = 1
                                                            vendorID = v_id
                                                            logger.info(
                                                                f"cos_sim:{cos_sim_di} , \
                                                                    vendor:{v_id}"
                                                            )
                                                            stop = True
                                                            break
                                                    elif cos_sim_stmp[0][0] * 100 >= 95:
                                                        vdrFound = 1
                                                        vendorID = v_id
                                                        logger.info(
                                                            f"cos_sim:{cos_sim_stmp} , \
                                                                vendor:{v_id}"
                                                        )
                                                        stop = True
                                                        break
                                                    else:
                                                        vdrFound = 0

                                                    if (vdrFound == 0) and (
                                                        di_inv_vendorName != ""
                                                    ):
                                                        if syn2 == di_inv_vendorName:

                                                            vdrFound = 1
                                                            vendorID = v_id
                                                            stop = True
                                                            break
                                                        elif (
                                                            syn2.replace("\n", " ")
                                                            == di_inv_vendorName
                                                        ):

                                                            vdrFound = 1
                                                            vendorID = v_id
                                                            stop = True
                                                            break
                                                    elif stamp_inv_vendorName != "":
                                                        if syn2 == stamp_inv_vendorName:

                                                            vdrFound = 1
                                                            vendorID = v_id
                                                            stop = True
                                                            break
                                                        elif (
                                                            syn2.replace("\n", " ")
                                                            == stamp_inv_vendorName
                                                        ):

                                                            vdrFound = 1
                                                            vendorID = v_id
                                                            stop = True
                                                            break
                            except Exception:
                                try:
                                    fr_trigger = db.query(model.frtrigger_tab).filter
                                    (model.frtrigger_tab.blobpath == spltFileName)

                                    # Step 2: Perform the update operation
                                    fr_trigger.update(
                                        {
                                            model.frtrigger_tab.status: "Vendor Mapping Failed",  # noqa: E501
                                        }
                                    )
                                    # Step 3: Commit the transaction
                                    db.commit()

                                except Exception:
                                    logger.error(f"{traceback.format_exc()}")

                        except Exception:
                            logger.error(f"{traceback.format_exc()}")

                            vdrFound = 0

                    except Exception:

                        logger.error(f"{traceback.format_exc()}")
                        vdrFound = 0

                    if vdrFound == 1:
                        # Retrieve the vendor name for the specified vendorID
                        try:
                            metaVendorName = vendorName_df.loc[
                                vendorName_df["idVendor"] == vendorID, "VendorName"
                            ].values[0]
                        except Exception:
                            logger.error(
                                f"Vendor with ID {vendorID} not found. {traceback.format_exc()}"
                            )
                            metaVendorName = ""

                        # Proceed only if vendor name was found
                        if metaVendorName:
                            # Group VendorCode and Address by VendorName
                            address_dict = (
                                vendorName_df[vendorName_df["VendorName"] == metaVendorName]
                                .set_index("idVendor")["Address"]
                                .to_dict()
                            )

                            # Format as a list of dictionaries with VendorCode as keys
                            metaVendorAdd = [address_dict]

                            # Log the retrieved information or assign as needed
                            logger.info(f"Vendor Name: {metaVendorName}")
                            logger.info(
                                f"Addresses for Vendor Name '{metaVendorName}': {metaVendorAdd}"  # noqa: E501
                            )
                        else:
                            # Assign empty list if vendor name is not found
                            metaVendorAdd = []

                        # Extract the required values from StampDataList
                        try:
                            doc_VendorAddress = StampDataList[splt_map[fl]]["VendorAddress"]
                        except Exception:
                            logger.error(
                                f"Error retrieving VendorAddress from StampDataList: {traceback.format_exc()}"
                            )
                            doc_VendorAddress = ""

                        # Initialize vndMth_address_ck to handle scenarios w
                        # here no match function is called
                        vndMth_address_ck = 0
                        matched_id_vendor = None
                        try:
                            # Extract the required values from StampDataList
                            doc_VendorAddress = StampDataList[splt_map[fl]]["VendorAddress"]
                            if doc_VendorAddress:
                                if len(metaVendorAdd[0]) > 1:
                                    vndMth_address_ck, matched_id_vendor = VndMatchFn_2(
                                        doc_VendorAddress, metaVendorAdd
                                    )
                                    if vndMth_address_ck == 1:
                                        vendorID = matched_id_vendor
                                        vdrFound = 1
                                        logger.info(
                                            "Vendor Address Matching with Master Data"
                                        )
                                    else:
                                        vdrFound = 0
                                        logger.info(
                                            "Vendor Address MisMatched with Master Data"
                                        )
                            else:
                                logger.warning(
                                    "'VendorAddress' missing in StampDataList[splt_map[fl]]"
                                )
                        except Exception as e:
                            logger.error(f"Unexpected error: {traceback.format_exc()}")
                    if vdrFound == 1:

                        try:
                            metaVendorAdd = list(
                                vendorName_df[vendorName_df["idVendor"] == vendorID][
                                    "Address"
                                ]
                            )

                        except Exception:
                            logger.error(f"{traceback.format_exc()}")
                            metaVendorAdd = ""

                        try:
                            # Filter the DataFrame for rows matching vendorID
                            vendorName_list = list(
                                vendorName_df[vendorName_df["idVendor"] == vendorID][
                                    "VendorName"
                                ]
                            )

                            # Check if the list is not empty
                            if vendorName_list:
                                metaVendorName = vendorName_list[0]
                            else:
                                # Handle the case where no match is found
                                metaVendorName = ""

                        except Exception:
                            logger.error(f"Error occurred: {traceback.format_exc()}")
                        vendorAccountID = vendorID
                        poNumber = "nonPO"
                        VendoruserID = 1
                        # configs = getOcrParameters(customerID, db)
                        metadata = getMetaData(vendorAccountID, db)
                        entityID = 1
                        modelData, modelDetails = getModelData(vendorAccountID, db)

                        if modelData is None:
                            try:
                                preBltFrdata, preBltFrdata_status = getFrData_MNF(
                                    rwOcrData[grp_pages[fl][0] - 1 : grp_pages[fl][1]]
                                )

                                invoId = push_frdata(
                                    preBltFrdata,
                                    999999,
                                    spltFileName,
                                    entityID,
                                    1,
                                    vendorAccountID,
                                    "nonPO",
                                    spltFileName,
                                    userID,
                                    0,
                                    num_pages,
                                    source,
                                    sender,
                                    filename,
                                    file_type,
                                    invoice_type,
                                    25,
                                    106,
                                    db,
                                    mail_row_key,
                                )
                                
                                if len(str(invoId)) == 0:
                                    logger.error(f"push_frdata returned None for invoId")
                                    try:
                                        fr_trigger = db.query(model.frtrigger_tab).filter(
                                            model.frtrigger_tab.blobpath == spltFileName
                                        )
                                        fr_trigger.update(
                                            {
                                                model.frtrigger_tab.status: "Error",
                                            }
                                        )
                                        db.commit()
                                    except Exception:
                                        logger.error(f"Failed to update error status in frtrigger_tab: {traceback.format_exc()}")
                                                
                                logger.info(
                                    f" Onboard vendor Pending: invoice_ID: {invoId}"
                                )
                                status = "success"

                            except Exception:
                                logger.error(f"{traceback.format_exc()}")

                                status = traceback.format_exc()

                            logger.info("Vendor Not Onboarded")
                        else:

                            logger.info(f"got Model {modelData}, model Name {modelData}")
                            ruledata = getRuleData(modelData.idDocumentModel, db)
                            # folder_name = modelData.folderPath  # TODO: Unused variable
                            # id_check = modelData.idDocumentModel  # TODO: Unused variable

                            entityBodyID = 1
                            file_size_accepted = 100
                            accepted_file_type = metadata.InvoiceFormat.split(",")
                            date_format = metadata.DateFormat
                            endpoint = settings.form_recognizer_endpoint
                            inv_model_id = modelData.modelID
                            API_version = settings.api_version
                            # API_version = configs.ApiVersion

                            generatorObj = {
                                "spltFileName": spltFileName,
                                "accepted_file_type": accepted_file_type,
                                "file_size_accepted": file_size_accepted,
                                "API_version": API_version,
                                "endpoint": endpoint,
                                "inv_model_id": inv_model_id,
                                "entityID": entityID,
                                "entityBodyID": entityBodyID,
                                "vendorAccountID": vendorAccountID,
                                "poNumber": poNumber,
                                "modelDetails": modelDetails,
                                "date_format": date_format,
                                "file_path": spltFileName,
                                "VendoruserID": VendoruserID,
                                "ruleID": ruledata.ruleID,
                                "filetype": file_type,
                                "filename": spltFileName,
                                "db": db,
                                "source": source,
                                "sender": sender,
                                "containername": containername,
                                "pdf_stream": pdf_stream,
                                "destination_container_name": destination_container_name,
                                "StampDataList": StampDataList,
                                "UploadDocType": invoice_type,
                                "metaVendorAdd": metaVendorAdd,
                                "metaVendorName": metaVendorName,
                                "mail_row_key": mail_row_key,
                                # "pre_data": "",
                                # "pre_status": "",
                                # "pre_model_msg": "",
                            }

                            try:
                                invoId = live_model_fn_1(generatorObj)
                                logger.info(f"DocumentID:{invoId}")
                                if len(str(invoId)) == 0:
                                    invoId = ""
                                    logger.error("Custom model failed")
                                    #---------------
                                    try:
                                        preBltFrdata, preBltFrdata_status = getFrData_MNF(
                                            rwOcrData[grp_pages[fl][0] - 1 : grp_pages[fl][1]]
                                        )

                                        invoId = push_frdata(
                                            preBltFrdata,
                                            999999,
                                            spltFileName,
                                            entityID,
                                            1,
                                            vendorAccountID,
                                            "nonPO",
                                            spltFileName,
                                            userID,
                                            0,
                                            num_pages,
                                            source,
                                            sender,
                                            filename,
                                            file_type,
                                            invoice_type,
                                            33,
                                            135,
                                            db,
                                            mail_row_key,
                                        )
                                        
                                        if len(str(invoId)) == 0:
                                            logger.error(f"push_frdata returned None for invoId")
                                            try:
                                                fr_trigger = db.query(model.frtrigger_tab).filter(
                                                    model.frtrigger_tab.blobpath == spltFileName
                                                )
                                                fr_trigger.update(
                                                    {
                                                        model.frtrigger_tab.status: "Error: Custom model not found in DI subscription",
                                                    }
                                                )
                                                db.commit()
                                            except Exception:
                                                logger.error(f"Failed to update error status in frtrigger_tab: {traceback.format_exc()}")
                                                        
                                        logger.info(
                                            f" Custom model failed-invoice_ID: {invoId}"
                                        )
                                        status = "success"

                                    except Exception:
                                        logger.error(f"{traceback.format_exc()}")

                                        status = traceback.format_exc()

                                    logger.info("Custom model failed")
                                    
                                    #---------------
                            except Exception:
                                invoId = ""
                                logger.error(f"{traceback.format_exc()}")

                            try:
                                if len(str(invoId)) == 0:
                                    preBltFrdata, preBltFrdata_status = getFrData_MNF(
                                        rwOcrData[grp_pages[fl][0] - 1 : grp_pages[fl][1]]
                                    )
                                    try:
                                        # Postprocessing Failed
                                        invoId = push_frdata(
                                            preBltFrdata,
                                            inv_model_id,
                                            spltFileName,
                                            entityID,
                                            entityBodyID,
                                            vendorAccountID,
                                            "nonPO",
                                            spltFileName,
                                            userID,
                                            0,
                                            num_pages,
                                            source,
                                            sender,
                                            filename,
                                            file_type,
                                            invoice_type,
                                            4,
                                            7,
                                            db,
                                            mail_row_key,
                                        )

                                        if len(str(invoId)) == 0:
                                            logger.error(f"push_frdata returned None for invoId")
                                            try:
                                                fr_trigger = db.query(model.frtrigger_tab).filter(
                                                    model.frtrigger_tab.blobpath == spltFileName
                                                )
                                                fr_trigger.update(
                                                    {
                                                        model.frtrigger_tab.status: "Error - Custom model not found in DI subscription",
                                                    }
                                                )
                                                db.commit()
                                            except Exception:
                                                logger.error(f"Failed to update error status in frtrigger_tab: {traceback.format_exc()}")

                                        logger.info(f" Onboard vendor Pending: invoice_ID: {invoId}")
                                        status = "success"

                                        try:
                                            fr_trigger = db.query(model.frtrigger_tab).filter(
                                                model.frtrigger_tab.blobpath == spltFileName
                                            )

                                            # Step 2: Perform the update operation
                                            fr_trigger.update(
                                                {
                                                    model.frtrigger_tab.status: "Processed",
                                                    model.frtrigger_tab.vendorID: vendorID,
                                                    model.frtrigger_tab.documentid: invoId,
                                                }
                                            )
                                            # Step 3: Commit the transaction
                                            db.commit()

                                        except Exception:
                                            logger.error(f"Database update error: {traceback.format_exc()}")

                                    except Exception as e:
                                        error_message = f"Error in push_frdata or postprocessing: {str(e)}"
                                        logger.error(error_message)

                                        # Update the frtrigger_tab with the error status
                                        try:
                                            fr_trigger = db.query(model.frtrigger_tab).filter(
                                                model.frtrigger_tab.blobpath == spltFileName
                                            )
                                            fr_trigger.update(
                                                {
                                                    model.frtrigger_tab.status: "Error",
                                                }
                                            )
                                            db.commit()
                                        except Exception:
                                            logger.error(f"Failed to update error status in frtrigger_tab: {traceback.format_exc()}")
                            except Exception:
                                logger.error(f"Unexpected error at line 1051: {traceback.format_exc()}")
                                status = traceback.format_exc()
                                
                            try:
                                if "Currency" in StampDataList[splt_map[fl]]:
                                    Currency = StampDataList[splt_map[fl]]["Currency"]

                                    # Call the validate_currency function
                                    # which now returns True or False
                                    isCurrencyMatch = validate_currency(
                                        invoId, Currency, db
                                    )  # noqa: E501

                                    # Check if the currency matched
                                    # (True means match, False means no match)
                                    if isCurrencyMatch:  # No need to compare to 'True'
                                        mrkCurrencyCk_isErr = 0
                                        mrkCurrencyCk_msg = "Success"

                                    else:
                                        mrkCurrencyCk_isErr = 1
                                        mrkCurrencyCk_msg = "Invalid. Please review."
                                    print(f"mrkCurrencyCk_msg: {mrkCurrencyCk_msg}")
                                    print(f"mrkCurrencyCk_isErr: {mrkCurrencyCk_isErr}")

                            except Exception:
                                logger.debug(f"{traceback.format_exc()}")

                    else:
                        try:
                            preBltFrdata, preBltFrdata_status = getFrData_MNF(
                                rwOcrData[grp_pages[fl][0] - 1 : grp_pages[fl][1]]
                            )
                            # 999999
                            invoId = push_frdata(
                                preBltFrdata,
                                999999,
                                spltFileName,
                                userID,
                                1,
                                0,
                                "nonPO",
                                spltFileName,
                                1,
                                0,
                                num_pages,
                                source,
                                sender,
                                filename,
                                file_type,
                                invoice_type,
                                26,
                                107,
                                db,
                                mail_row_key,
                            )
                            
                            if len(str(invoId)) == 0:
                                logger.error(f"push_frdata returned None for invoId")
                                try:
                                    fr_trigger = db.query(model.frtrigger_tab).filter(
                                        model.frtrigger_tab.blobpath == spltFileName
                                    )
                                    fr_trigger.update(
                                        {
                                            model.frtrigger_tab.status: "Error",
                                        }
                                    )
                                    db.commit()
                                except Exception:
                                    logger.error(f"Failed to update error status in frtrigger_tab: {traceback.format_exc()}")
                                    
                            logger.info(f" VendorUnidentified: invoice_ID: {invoId}")
                            status = "success"
                            try:
                                db.query(model.frtrigger_tab).filter(
                                    model.frtrigger_tab.blobpath == spltFileName
                                ).update(
                                    {
                                        model.frtrigger_tab.status: "VendorNotFound",
                                        model.frtrigger_tab.documentid: invoId,
                                    }
                                )

                                # Commit the transaction
                                db.commit()
                            except Exception:
                                logger.error(f"Error while updating frtrigger_tab: {traceback.format_exc()}")
                        except Exception:
                            logger.debug(traceback.format_exc())
                            status = "fail"
                            try:
                                db.query(model.frtrigger_tab).filter(
                                    model.frtrigger_tab.blobpath == spltFileName
                                ).update(
                                    {
                                        model.frtrigger_tab.status: "Error",
                                    }
                                )

                                # Commit the transaction
                                db.commit()
                            except Exception:
                                logger.error(f"Error while updating frtrigger_tab: {traceback.format_exc()}")

                        # logger.info("vendor not found!!")

                        # status = traceback.format_exc()
                    if ("StampFound" in StampDataList[splt_map[fl]]) and (
                        len(str(invoId)) > 0
                    ):
                        # stm_dt_lt = []
                        confCk_isErr = 1
                        confCk_msg = "Confirmation Number Not Found"
                        RevDateCk_isErr = 1
                        RevDateCk_msg = "Receiving Date Not Found"
                        mrkDeptCk_isErr = 1
                        mrkDeptCk_msg = "Marked Department Not Found"
                        RvrCk_isErr = 1
                        RvrCk_msg = "Receiver Not Found"
                        deptCk_isErr = 1
                        deptCk_msg = "Department Not Found"
                        strCk_isErr = 1
                        strCk_msg = "Store Number Not Found"
                        StrTyp_IsErr = 1
                        StrTyp_msg = "Store Type Not Found"
                        store_type = "NA"
                        StampFound = StampDataList[splt_map[fl]]["StampFound"]
                        stmp_created_on = datetime.now(timezone.utc).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        if StampFound == "Yes":
                            if "CreditNote" in StampDataList[splt_map[fl]]:
                                CreditNote_chk = StampDataList[splt_map[fl]]["CreditNote"]
                                if CreditNote_chk == "Yes":
                                    CreditNote = "credit note"
                                else:
                                    CreditNote = "Invoice Document"

                                CreditNoteCk_isErr = 1
                                CreditNoteCk_msg = "Response from OpenAI."

                            else:
                                CreditNote = "Invoice Document"
                                CreditNoteCk_isErr = 0
                                CreditNoteCk_msg = "No response from OpenAI."

                            # -----------------

                            stampdata: dict[str, int | str] = {}
                            stampdata["documentid"] = invoId
                            stampdata["stamptagname"] = "Credit Identifier"
                            stampdata["stampvalue"] = CreditNote
                            stampdata["is_error"] = CreditNoteCk_isErr
                            stampdata["errordesc"] = CreditNoteCk_msg
                            stampdata["created_on"] = stmp_created_on
                            stampdata["IsUpdated"] = IsUpdated
                            db.add(model.StampDataValidation(**stampdata))
                            db.commit()

                            # -----------------

                            # Check if VendorID matches 282 and proceed accordingly
                            if vendorAccountID == 282:
                                if (
                                    "GST_HST_Found" in StampDataList[splt_map[fl]]
                                    and "GST_HST_Amount" in StampDataList[splt_map[fl]]
                                ):
                                    Gst_Hst_Check = StampDataList[splt_map[fl]][
                                        "GST_HST_Found"
                                    ]
                                    if Gst_Hst_Check == "Yes":
                                        GST_HST_Amount = StampDataList[splt_map[fl]][
                                            "GST_HST_Amount"
                                        ]
                                        gst_hst_isErr = 0
                                        gst_hst_Ck_msg = ""
                                    else:
                                        GST_HST_Amount = "0.0"
                                        gst_hst_isErr = 1
                                        gst_hst_Ck_msg = "Response from OpenAI."
                                else:
                                    GST_HST_Amount = "0.0"
                                    gst_hst_isErr = 1
                                    gst_hst_Ck_msg = "No response from OpenAI."

                                stampdata["documentid"] = invoId
                                stampdata["stamptagname"] = "GST"
                                stampdata["stampvalue"] = GST_HST_Amount
                                stampdata["is_error"] = gst_hst_isErr
                                stampdata["errordesc"] = gst_hst_Ck_msg
                                stampdata["created_on"] = stmp_created_on
                                stampdata["IsUpdated"] = IsUpdated
                                db.add(model.StampDataValidation(**stampdata))
                                db.commit()

                                store_gst_hst_amount(
                                    invoId,
                                    GST_HST_Amount,
                                    gst_hst_isErr,
                                    gst_hst_Ck_msg,
                                    IsUpdated,
                                    db,
                                )

                            if "Confirmation" in StampDataList[splt_map[fl]]:
                                Confirmation_rw = StampDataList[splt_map[fl]][
                                    "Confirmation"
                                ]
                                str_nm = ""
                                Confirmation = "".join(re.findall(r"\d", Confirmation_rw))
                                if len(Confirmation) == 9:
                                    try:

                                        query = (
                                            db.query(model.PFGReceipt)
                                            .filter(
                                                model.PFGReceipt.RECEIVER_ID == Confirmation
                                            )
                                            .first()
                                        )

                                        if query:
                                            # for invRpt in query:
                                            str_nm = query.LOCATION
                                            confCk_isErr = 0
                                            confCk_msg = "Valid Confirmation Number"
                                            # str_nm = row[15]
                                        else:
                                            confCk_isErr = 1
                                            confCk_msg = "Confirmation Number Not Found"

                                    except Exception as e:
                                        logger.debug(f"{traceback.format_exc()}")
                                        confCk_isErr = 1
                                        confCk_msg = "Error:" + str(e)

                                else:
                                    confCk_isErr = 1
                                    confCk_msg = "Invalid Confirmation Number"

                            else:
                                Confirmation = "N/A"
                                confCk_isErr = 1
                                confCk_msg = "Confirmation Number NotFound"

                            stampdata["documentid"] = invoId
                            stampdata["stamptagname"] = "ConfirmationNumber"
                            stampdata["stampvalue"] = Confirmation
                            stampdata["is_error"] = confCk_isErr
                            stampdata["errordesc"] = confCk_msg
                            stampdata["created_on"] = stmp_created_on
                            stampdata["IsUpdated"] = IsUpdated
                            db.add(model.StampDataValidation(**stampdata))
                            db.commit()

                            if "MarkedDept" in StampDataList[splt_map[fl]]:
                                MarkedDept = StampDataList[splt_map[fl]]["MarkedDept"]
                                if confCk_isErr == 0:
                                    account = (
                                            db.query(model.PFGReceipt.ACCOUNT)
                                            .filter(
                                                model.PFGReceipt.RECEIVER_ID == Confirmation
                                            )
                                            .first()
                                        )
                                    inv_account = ['14100', '14150','14100', '98401',
                                                '98400', '14400', '14410', '14999',
                                                '14200', '14420']
                                    sup_account = ['71000', '71025', '71999', '71050']
                                    if account:
                                        if account[0] in inv_account:
                                            MarkedDept = "Inventory"
                                        elif account[0] in sup_account:
                                            MarkedDept = "Supplies"
                                        else:
                                            MarkedDept = "Inventory"
                                if MarkedDept == "Inventory" or MarkedDept == "Supplies":
                                    mrkDeptCk_isErr = 0
                                    mrkDeptCk_msg = ""
                                else:
                                    mrkDeptCk_isErr = 1
                                    mrkDeptCk_msg = "Invalid. Please review."

                            else:
                                mrkDeptCk_isErr = 1
                                mrkDeptCk_msg = "Not Found."
                                MarkedDept = "Inventory"
                            # ----------------------

                            stampdata: dict[str, int | str] = {}
                            stampdata["documentid"] = invoId
                            stampdata["stamptagname"] = "SelectedDept"
                            stampdata["stampvalue"] = MarkedDept
                            stampdata["is_error"] = mrkDeptCk_isErr
                            stampdata["errordesc"] = mrkDeptCk_msg
                            stampdata["created_on"] = stmp_created_on
                            stampdata["IsUpdated"] = IsUpdated
                            db.add(model.StampDataValidation(**stampdata))
                            db.commit()
                            
                            
                            if "ReceivingDate" in StampDataList[splt_map[fl]]:
                                ReceivingDate = StampDataList[splt_map[fl]]["ReceivingDate"]
                                if is_valid_date(ReceivingDate):
                                    RevDateCk_isErr = 0
                                    RevDateCk_msg = ""
                                else:
                                    RevDateCk_isErr = 0
                                    RevDateCk_msg = "Invalid Date Format"
                            else:
                                ReceivingDate = "N/A"
                                RevDateCk_isErr = 0
                                RevDateCk_msg = "ReceivingDate Not Found."

                            stampdata["documentid"] = invoId
                            stampdata["stamptagname"] = "ReceivingDate"
                            stampdata["stampvalue"] = ReceivingDate
                            stampdata["is_error"] = RevDateCk_isErr
                            stampdata["errordesc"] = RevDateCk_msg
                            stampdata["created_on"] = stmp_created_on
                            stampdata["IsUpdated"] = IsUpdated
                            db.add(model.StampDataValidation(**stampdata))
                            db.commit()

                            if "Receiver" in StampDataList[splt_map[fl]]:
                                Receiver = StampDataList[splt_map[fl]]["Receiver"]
                                RvrCk_isErr = 0
                                RvrCk_msg = ""
                            else:
                                Receiver = "N/A"
                                RvrCk_isErr = 0
                                RvrCk_msg = "Receiver Not Available"

                            stampdata["documentid"] = invoId
                            stampdata["stamptagname"] = "Receiver"
                            stampdata["stampvalue"] = Receiver
                            stampdata["is_error"] = RvrCk_isErr
                            stampdata["errordesc"] = RvrCk_msg
                            stampdata["created_on"] = stmp_created_on
                            stampdata["IsUpdated"] = IsUpdated
                            db.add(model.StampDataValidation(**stampdata))
                            db.commit()

                            if "Department" in StampDataList[splt_map[fl]]:
                                Department = StampDataList[splt_map[fl]]["Department"]
                                deptCk_isErr = 0
                                deptCk_msg = ""
                            else:
                                Department = "N/A"
                                deptCk_isErr = 1
                                deptCk_msg = "Department Not Found."

                            stampdata["documentid"] = invoId
                            stampdata["stamptagname"] = "Department"
                            stampdata["stampvalue"] = Department
                            stampdata["is_error"] = deptCk_isErr
                            stampdata["errordesc"] = deptCk_msg
                            stampdata["created_on"] = stmp_created_on
                            stampdata["IsUpdated"] = IsUpdated
                            db.add(model.StampDataValidation(**stampdata))
                            db.commit()

                            if "Store Number" in StampDataList[splt_map[fl]]:
                                storenumber = StampDataList[splt_map[fl]]["Store Number"]
                                try:
                                    try:
                                        storenumber = str(
                                            "".join(filter(str.isdigit, str(storenumber)))
                                        )
                                        # Fetch specific columns as a list
                                        # of dictionaries using .values()
                                        results = db.query(
                                            model.NonintegratedStores
                                        ).values(model.NonintegratedStores.store_number)
                                        nonIntStr = [dict(row) for row in results]
                                        nonIntStr_number = [
                                            d["store_number"] for d in nonIntStr
                                        ]
                                        if (
                                            int(
                                                "".join(
                                                    filter(
                                                        str.isdigit,
                                                        str(storenumber),
                                                    )
                                                )
                                            )
                                            in nonIntStr_number
                                        ):
                                            StrTyp_IsErr = 0
                                            StrTyp_msg = ""
                                            store_type = "Non-Integrated"

                                        else:
                                            StrTyp_IsErr = 0
                                            StrTyp_msg = ""
                                            store_type = "Integrated"
                                    except Exception:
                                        logger.debug(f"{traceback.format_exc()}")

                                    if len(str_nm) > 0:
                                        if int(storenumber) == int(str_nm):
                                            strCk_isErr = 0
                                            strCk_msg = ""
                                        else:
                                            strCk_isErr = 0
                                            strCk_msg = "Store Number Not Matching"

                                    else:
                                        strCk_isErr = 0
                                        strCk_msg = "Store Number Not Matching"

                                except Exception:
                                    logger.debug(f"{traceback.format_exc()}")
                                    strCk_isErr = 1
                                    strCk_msg = "Invalid store number"
                            else:
                                storenumber = "N/A"
                                strCk_isErr = 1
                                strCk_msg = ""

                            stampdata["documentid"] = invoId
                            stampdata["stamptagname"] = "StoreType"
                            stampdata["stampvalue"] = store_type
                            stampdata["is_error"] = StrTyp_IsErr
                            stampdata["errordesc"] = StrTyp_msg
                            stampdata["created_on"] = stmp_created_on
                            stampdata["IsUpdated"] = IsUpdated
                            db.add(model.StampDataValidation(**stampdata))
                            db.commit()

                            # stampdata = {}
                            # stampdata: dict[str, int | str] = {}
                            stampdata["documentid"] = invoId
                            stampdata["stamptagname"] = "StoreNumber"
                            stampdata["stampvalue"] = storenumber
                            stampdata["is_error"] = strCk_isErr
                            stampdata["errordesc"] = strCk_msg
                            stampdata["created_on"] = str(stmp_created_on)
                            stampdata["IsUpdated"] = IsUpdated
                            db.add(model.StampDataValidation(**stampdata))
                            db.commit()

                            try:
                                db.query(model.Document).filter(
                                    model.Document.idDocument == invoId
                                ).update(
                                    {
                                        model.Document.JournalNumber: str(
                                            Confirmation
                                        ),  # noqa: E501
                                        model.Document.dept: str(Department),
                                        model.Document.store: str(storenumber),
                                    }
                                )
                                db.commit()

                            except Exception:
                                logger.debug(f"{traceback.format_exc()}")

                            try:
                                gst_amt = 0
                                if store_type == "Integrated":
                                    payload_subtotal = ""
                                    IntegratedvoucherData(
                                        invoId, gst_amt, payload_subtotal, CreditNote, db
                                    )
                                elif store_type == "Non-Integrated":
                                    payload_subtotal = ""
                                    nonIntegratedVoucherData(
                                        invoId, gst_amt, payload_subtotal, CreditNote, db
                                    )
                            except Exception:
                                logger.debug(f"{traceback.format_exc()}")
                        else:
                            try:
                                if "CreditNote" in StampDataList[splt_map[fl]]:
                                    CreditNote_chk = StampDataList[splt_map[fl]][
                                        "CreditNote"
                                    ]
                                    if CreditNote_chk == "Yes":
                                        CreditNote = "credit note"
                                    elif CreditNote_chk == "No":
                                        CreditNote = "Invoice Document"
                                    else:
                                        CreditNote = "NA"

                                    CreditNoteCk_isErr = 1
                                    CreditNoteCk_msg = "Response from OpenAI."

                                stampdata: dict[str, int | str] = {}
                                stampdata["documentid"] = invoId
                                stampdata["stamptagname"] = "Credit Identifier"
                                stampdata["stampvalue"] = CreditNote
                                stampdata["is_error"] = CreditNoteCk_isErr
                                stampdata["errordesc"] = CreditNoteCk_msg
                                stampdata["created_on"] = stmp_created_on
                                stampdata["IsUpdated"] = IsUpdated
                                db.add(model.StampDataValidation(**stampdata))
                                db.commit()

                            except Exception:
                                logger.debug(f"No response from OpenAI.")
                                logger.debug(f"{traceback.format_exc()}")

                    try:

                        db.query(model.frtrigger_tab).filter(
                            model.frtrigger_tab.blobpath == spltFileName
                        ).update(
                            {
                                model.frtrigger_tab.status: "Processed",
                                model.frtrigger_tab.vendorID: vendorID,
                                model.frtrigger_tab.documentid: invoId,
                            },
                        )
                        db.commit()

                    except Exception:
                        # logger.info(f"ocr.py  {str(qw)}")
                        logger.debug(f"{traceback.format_exc()}")

                    status = "success"
                    fl = fl + 1

            else:
                try:
                    split_doc = db.query(model.SplitDocTab).filter
                    (model.SplitDocTab.splitdoc_id == splitdoc_id)

                    # Step 2: Perform the update operation
                    split_doc.update(
                        {
                            model.SplitDocTab.status: str("Not Sure Error"),  # noqa: E501
                        }
                    )
                    # Step 3: Commit the transaction
                    db.commit()

                except Exception:
                    logger.error(
                        f"Failed to update splitdoc_tab table {traceback.format_exc()}"
                    )
                logger.error(f"DI responed error: {fr_model_status, fr_model_msg}")
                # log to DB
            try:
                if len(str(invoId)) == 0:
                    preBltFrdata, preBltFrdata_status = getFrData_MNF(
                        rwOcrData[grp_pages[fl][0] - 1 : grp_pages[fl][1]]
                    )
                    invoId = push_frdata(
                        preBltFrdata,
                        999999,
                        spltFileName,
                        entityID,
                        1,
                        vendorAccountID,
                        "nonPO",
                        spltFileName,
                        userID,
                        0,
                        num_pages,
                        source,
                        sender,
                        filename,
                        file_type,
                        invoice_type,
                        4,
                        7,
                        db,
                        mail_row_key,
                    )
                    if len(str(invoId)) == 0:
                        logger.error(f"push_frdata returned None for invoId")
                        try:
                            fr_trigger = db.query(model.frtrigger_tab).filter(
                                model.frtrigger_tab.blobpath == spltFileName
                            )
                            fr_trigger.update(
                                {
                                    model.frtrigger_tab.status: "Error",
                                }
                            )
                            db.commit()
                        except Exception:
                            logger.error(f"Failed to update error status in frtrigger_tab: {traceback.format_exc()}")
                            
                    logger.info(
                        f" PostProcessing Error, systemcheckinvoice: invoice_ID: {invoId}"
                    )
                    status = "Error"

                    try:

                        # Update multiple fields where 'documentid' matches a certain value
                        db.query(model.frtrigger_tab).filter(
                            model.frtrigger_tab.blobpath == spltFileName
                        ).update(
                            {
                                model.frtrigger_tab.status: "Error",
                                model.frtrigger_tab.sender: sender,
                                model.frtrigger_tab.vendorID: vendorID,
                                model.frtrigger_tab.documentid: invoId,
                            }
                        )
                        db.commit()

                    except Exception:
                        # logger.info(f"ocr.py: {str(qw)}")
                        logger.error(f"{traceback.format_exc()}")
            except Exception:
                # logger.error(f"ocr.py: {err}")
                logger.error(f" ocr.py: {traceback.format_exc()}")
            # try:
            #     if len(str(invoId)) !=0:

            #         db.query(model.Document).filter(
            #             model.Document.idDocument == invoId
            #         ).update(
            #             {
            #                 model.Document.mail_row_key: str(
            #                     mail_row_key
            #                 ),  # noqa: E501

            #             }
            #         )
            #         db.commit()

            except Exception:
                logger.debug(f"{traceback.format_exc()}")
        except Exception as err:
            logger.error(f"API exception ocr.py: {traceback.format_exc()}")
            status = "error: " + str(err)
            # splitdoc_id = new_split_doc.splitdoc_id
            split_doc = (
                db.query(model.SplitDocTab)
                .filter(model.SplitDocTab.splitdoc_id == splitdoc_id)
                .first()
            )

            if split_doc:
                split_doc.status = "Error"
                split_doc.updated_on = datetime.now(tz_region)  # Update the timestamp
                db.commit()
                

        try:

            if vdrFound == 1 and modelData is not None:
                customCall = 0
                skipConf = 0

                pfg_sync(invoId, userID, db, customCall, skipConf)

        except Exception:
            logger.debug(f"{traceback.format_exc()}")

        return status
    except Exception as e:
        logger.error(f"Error in queue_process_task: {traceback.format_exc()}")
        status = "error: " + str(err)
        # splitdoc_id = new_split_doc.splitdoc_id
        split_doc = (
            db.query(model.SplitDocTab)
            .filter(model.SplitDocTab.splitdoc_id == splitdoc_id)
            .first()
        )

        if split_doc:
            split_doc.status = "Error: Unsupported File Format"
            split_doc.updated_on = datetime.now(tz_region)  # Update the timestamp
            db.commit()
            
        
        # frtrigger_insert_data = {
        #     "status": "Error",
        #     "sender": sender,
        #     "splitdoc_id": splitdoc_id,
            
        # }
        # fr_db_data = model.frtrigger_tab(**frtrigger_insert_data)
        # db.add(fr_db_data)
        # db.commit()
            
        # return f"Error: {traceback.format_exc()}"
    finally:
        try:
            # splitdoc_id = new_split_doc.splitdoc_id
            logger.info(f"Inside finally block")
            logger.info(f"splitdoc_id: {splitdoc_id}")
            # Query all rows in frtrigger_tab with the specific splitdoc_id
            # with db.begin():  # Ensures rollback on failure
            triggers = (
                db.query(model.frtrigger_tab)
                .filter(model.frtrigger_tab.splitdoc_id == splitdoc_id)
                .all()
            )

            # Check the status of all rows
            if not triggers:
                logger.info(f"No rows found in frtrigger_tab for splitdoc_id: {splitdoc_id}")
                overall_status = "Error"
            else:
                
                statuses = {trigger.status for trigger in triggers}

                # Normalize statuses, treating "Processed" and "File Processed" as the same
                normalized_statuses = {status if status not in {"Processed", "File Processed"} else "Processed" for status in statuses}

                # Determine the overall status
                if normalized_statuses == {"Processed"}:
                    overall_status = "Processed-completed"
                elif "Processed" in normalized_statuses and len(normalized_statuses) > 1:
                    overall_status = "Partially-processed"
                else:
                    overall_status = "Error"
            
            # Update the SplitDocTab status
            split_doc = (
                db.query(model.SplitDocTab)
                .filter(model.SplitDocTab.splitdoc_id == splitdoc_id)
                .first()
            )

            if split_doc:
                split_doc.status = overall_status
                split_doc.updated_on = datetime.now(tz_region)  # Update the timestamp
                # Commit the update
                db.commit()
                logger.info(f"Updated SplitDocTab {splitdoc_id} status to {overall_status}")
            else:
                logger.warning(f"SplitDocTab not found for splitdoc_id: {splitdoc_id}")

        except Exception as e:
            logger.error(f"Exception in splitDoc: {traceback.format_exc()}")
            db.rollback()  # Rollback transaction on failure
        db.close()

def queue_worker(operation_id):
    while True:
        set_operation_id(operation_id)
        try:
            db = next(get_db())
            # get the correct queue sattus for `queued` and lock it
            if settings.build_type == "debug":
                queued_status = f"{settings.local_user_name}-queued"
                processing_status = f"{settings.local_user_name}-processing"
                completed_status = f"{settings.local_user_name}-completed"
                failed_status = f"{settings.local_user_name}-failed"
            else:
                queued_status = "queued"
                processing_status = "processing"
                completed_status = "completed"
                failed_status = "failed"

            # Fetch a queue_task with status 'queued' and lock it
            queue_task = (
                db.query(QueueTask)
                .filter(QueueTask.status == queued_status)
                .with_for_update(skip_locked=True)
                .first()
            )

            if queue_task:
                # Update the queue_task status to 'processing'
                queue_task.status = processing_status
                db.add(queue_task)
                db.commit()
                # Process the queue_task
                try:
                    status = queue_process_task(queue_task)
                    logger.info(f"QueueTask {queue_task.id} => {status}")
                    if status == "success":
                        queue_task.status = completed_status
                    else:
                        queue_task.status = failed_status
                    db.add(queue_task)
                    db.commit()
                    # load the queue task from db again to check if reflected
                    queue_task_db = (
                        db.query(QueueTask)
                        .filter(QueueTask.id == queue_task.id)
                        .first()
                    )
                    logger.info(
                        f"QueueTask {queue_task.id} => {queue_task.status} <= {queue_task_db.status}"
                    )

                except Exception:
                    queue_task.status = failed_status
                    db.add(queue_task)
                    db.commit()
                    logger.error(
                        f"QueueTask {queue_task.id} failed: {traceback.format_exc()}"
                    )
        except Exception:
            logger.info(f"QueueWorker failed: {traceback.format_exc()}")
        finally:
            db.close()
        time.sleep(1)  # Polling interval
        


def nomodelfound():
    current_status = {"percentage": 0, "status": "Model not Found!"}
    return current_status
    # yield {
    #     "event": "end",
    #     "data": json.dumps(current_status)
    # }
    # print("current_status: 276: ",current_status)


def getModelData(vendorAccountID, db):
    try:
        modelDetails = []
        modelData = (
            db.query(model.DocumentModel)
            .filter(model.DocumentModel.idVendorAccount == vendorAccountID)
            .filter(model.DocumentModel.is_active == 1)
            .order_by(model.DocumentModel.UpdatedOn)
            .all()
        )
        # print("modelData line 403: ", modelData)
        reqModel = None
        for m in modelData:
            if m.modelID is not None and m.modelID != "":
                reqModel = m
                modelDetails.append(
                    {"IdDocumentModel": m.idDocumentModel, "modelName": m.modelName}
                )
        return reqModel, modelDetails
    except Exception:
        logger.error(f"{traceback.format_exc()}")
        return None


def getEntityData(vendorAccountID, db):
    entitydata = (
        db.query(model.VendorAccount)
        .options(
            Load(model.VendorAccount).load_only("entityID", "entityBodyID", "vendorID")
        )
        .filter(model.VendorAccount.idVendorAccount == vendorAccountID)
        .first()
    )
    return entitydata


def getMetaData(vendorAccountID, db):
    try:

        metadata = (
            db.query(model.FRMetaData)
            .join(
                model.DocumentModel,
                model.FRMetaData.idInvoiceModel == model.DocumentModel.idDocumentModel,
            )
            .filter(model.DocumentModel.idVendorAccount == vendorAccountID)
            .first()
        )
        return metadata
    except Exception:
        logger.error(f"{traceback.format_exc()}")
        return None


def getRuleData(idDocumentModel, db):
    ruledata = (
        db.query(model.FRMetaData)
        .filter(model.FRMetaData.idInvoiceModel == idDocumentModel)
        .first()
    )
    return ruledata


def getOcrParameters(customerID, db):
    try:
        configs = (
            db.query(model.FRConfiguration)
            .filter(model.FRConfiguration.idCustomer == customerID)
            .first()
        )
        return configs
    except Exception:
        logger.error(traceback.format_exc())
        db.rollback()
        return Response(
            status_code=500, headers={"DB Error": "Failed to get OCR parameters"}
        )


def live_model_fn_1(generatorObj):
    invoice_ID = ""
    logger.info("live_model_fn_1 started")
    # spltFileName = generatorObj['spltFileName']
    file_path = generatorObj["file_path"]
    # container = generatorObj["containername"]  # TODO: Unused variable
    API_version = generatorObj["API_version"]
    endpoint = settings.form_recognizer_endpoint
    inv_model_id = generatorObj["inv_model_id"]
    entityID = 1
    entityBodyID = generatorObj["entityBodyID"]
    # vendorAccountID = generatorObj['vendorAccountID']
    poNumber = generatorObj["poNumber"]
    modelDetails = generatorObj["modelDetails"]
    date_format = generatorObj["date_format"]

    userID = generatorObj["VendoruserID"]
    ruledata = generatorObj["ruleID"]
    file_type = generatorObj["filetype"]
    filename = generatorObj["filename"]
    sender = generatorObj["sender"]
    db = generatorObj["db"]
    source = generatorObj["source"]
    fr_data = {}
    spltFileName = generatorObj["spltFileName"]
    vendorAccountID = generatorObj["vendorAccountID"]
    UploadDocType = generatorObj["UploadDocType"]

    metaVendorAdd = generatorObj["metaVendorAdd"]
    metaVendorName = generatorObj["metaVendorName"]
    mail_row_key = generatorObj["mail_row_key"]
    # OpenAI_client = generatorObj["OpenAI_client"]

    # pre_data = generatorObj["pre_data"]
    # pre_status = generatorObj["pre_status"]
    # pre_model_msg = generatorObj["pre_model_msg"]

    accepted_file_type = "application/pdf"
    file_size_accepted = 100
    # print("in live fn")
    destination_container_name = generatorObj["destination_container_name"]
    fr_preprocessing_status, fr_preprocessing_msg, input_data, ui_status = (
        fr_preprocessing(
            vendorAccountID,
            entityID,
            file_path,
            accepted_file_type,
            file_size_accepted,
            filename,
            spltFileName,
            destination_container_name,
            db,
        )
    )

    # print("input_data: ",input_data)
    if fr_preprocessing_status == 1:
        current_status = {"percentage": 25, "status": "Pre-Processing ⏳"}
        # print("current_status: 358: ", current_status)
        logger.info(f"current_status: {current_status}")

        valid_file = False
        if (
            file_type == "image/jpg"
            or file_type == "image/jpeg"
            or file_type == "image/png"
            or file_type == "application/pdf"
        ):
            valid_file = True

        if valid_file:
            pass
            # live_model_status = 0  # TODO: Unused variable
            # live_model_msg = "Please upload jpg or pdf file"  # TODO: Unused variable
        model_type = "custom"
        # check from where this function is coming
        # (this is coming from core/azure_fr.py)
        cst_model_status, cst_model_msg, cst_data, cst_status, isComposed, template = (
            get_fr_data(
                input_data,
                API_version,
                endpoint,
                model_type,
                inv_model_id,
            )
        )

        model_type = "prebuilt"
        # check from where this function is coming
        # (this is coming from core/azure_fr.py)
        pre_model_status, pre_model_msg, pre_data, pre_status = get_fr_data(
            input_data,
            API_version,
            endpoint,
            model_type,
            inv_model_id,
        )

        if not isComposed:
            modelID = modelDetails[-1]["IdDocumentModel"]
        else:
            # modeldict = next(x for x in modelDetails
            # if x["modelName"].lower() == template.lower())
            modelID = modelDetails[-1]["IdDocumentModel"]

        no_pages_processed = len(input_data)
        if (cst_status == "succeeded") and (pre_status == "succeeded"):
            current_status = {"percentage": 50, "status": "Processing Model ⚡"}
            # print("current_status: 421: ",current_status)
            logger.info(f"current_status: {current_status}")
            # yield {
            #     "event": "update",
            #     "retry": status_stream_retry_timeout,
            #     "data": json.dumps(current_status)
            # }
            (
                fr_data,
                postprocess_msg,
                postprocess_status,
                duplicate_status,
                sts_hdr_ck,
            ) = postpro(
                cst_data,
                pre_data,
                date_format,
                modelID,
                SQLALCHEMY_DATABASE_URL,
                entityID,
                vendorAccountID,
                filename,
                db,
                sender,
                metaVendorName,
                metaVendorAdd,
            )
            if duplicate_status == 0:
                docStatus = 32
                docsubstatus = 128
            elif sts_hdr_ck == 0:
                docStatus = 4
                docsubstatus = 2
            else:
                docStatus = 4
                docsubstatus = 26

            if postprocess_status == 1:
                blobPath = file_path
                invoice_ID = push_frdata(
                    fr_data,
                    modelID,
                    file_path,
                    entityID,
                    entityBodyID,
                    vendorAccountID,
                    poNumber,
                    blobPath,
                    userID,
                    ruledata,
                    no_pages_processed,
                    source,
                    sender,
                    filename,
                    file_type,
                    UploadDocType,
                    docStatus,
                    docsubstatus,
                    db,
                    mail_row_key,
                )
                # print("invoice_ID line 504: ",invoice_ID)
                # logger.info(f"ocr.py, line 571: InvoiceDocumentID: {invoice_ID}")
                try:

                    created_on = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    data_switch = {
                        "documentID": invoice_ID,
                        "UserID": userID,
                        "CreatedON": created_on,
                        "DocPrebuiltData": {"prebuilt": fr_data["prebuilt_header"]},
                        "DocCustData": {"custom": fr_data["header"]},
                        "FilePathNew": blobPath,
                        "FilePathOld": "None",
                    }

                    data_switch_ = model.Dataswitch(**data_switch)

                    db.add(data_switch_)
                    db.commit()
                except Exception:
                    logger.error(f"{traceback.format_exc()}")
                    # logger.error(f"ocr.py line 594: exception:{str(ep)}")
                    # {"DB error": "Error while inserting data"}

                # live_model_status = 1  # TODO: Unused variable
                # live_model_msg = "Data extracted"  # TODO: Unused variable
                current_status = {"percentage": 75, "status": "Post-Processing ⌛"}
                # print("current_status: line 466: ",current_status)
                # logger.info(f"current_status: line 466: {current_status}")

                current_status = {"percentage": 100, "status": "OCR completed ✔"}

                # print("current_status: line 479: ", current_status)

                logger.info(f"current_status::{current_status}")
            else:
                # live_model_status = 0  # TODO: Unused variable
                # live_model_msg = postprocess_status  # TODO: Unused variable
                current_status = {"percentage": 75, "status": postprocess_msg}
                # print("current_status: line 521: ", current_status)
                logger.info(f"current_status: line 521:{current_status}")
        else:
            current_status = {
                "percentage": 50,
                "status": "prebuilt: " + pre_model_msg + " custom: " + cst_model_msg,
            }
            # yield {
            #     "event": "end",
            #     "data": json.dumps(current_status)
            # }

            logger.info(f"current_status: line 529: {current_status}")
            if cst_status != "succeeded":
                pass
                # live_model_status = 0  # TODO: Unused variable
                # live_model_msg = cst_model_msg  # TODO: Unused variable
            elif pre_status != "succeeded":
                pass
                # live_model_status = 0  # TODO: Unused variable
                # live_model_msg = pre_model_msg  # TODO: Unused variable
            elif pre_status == cst_status != "succeeded":
                pass
                # live_model_status = 0  # TODO: Unused variable
                # live_model_msg = (
                #     "Custom model: "
                #     + cst_model_msg
                #     + ". Prebuilt Model: "
                #     + pre_model_msg
                # )  # TODO: Unused variable
            else:
                pass
                # live_model_status = 0  # TODO: Unused variable
                # live_model_msg = "Azure FR api issue"  # TODO: Unused variable

    else:
        pass

    logger.info(f"invoice_ID line 606 ocr.py: {invoice_ID}")

    return invoice_ID


def push_frdata(
    data,
    modelID,
    filepath,
    entityID,
    entityBodyID,
    vendorAccountID,
    poNumber,
    blobPath,
    userID,
    ruledata,
    no_pages_processed,
    source,
    sender,
    filename,
    filetype,
    UploadDocType,
    docStatus,
    docsubstatus,
    db,
    mail_row_key,
):
    # credit invoice processsing:
    try:
        try:

            logger.info(f"In pushFR :, modelID: {modelID}, vendorAccountID:{vendorAccountID} docStatus:{docStatus},")
            if not vendorAccountID:
                vendorAccountID = 0
                logger.info(f" vendorAccountId updated to 0")

        except Exception as e:
            logger.error(traceback.format_exc())
        hdr_ck_list = [
            "SubTotal",
            "InvoiceTotal",
            "GST",
            "HST",
            "PST",
            "HST",
            "TotalTax",
            "LitterDeposit",
            "BottleDeposit",
            "Discount",
            "FreightCharges",
            "Fuel surcharge",
            "Credit_Card_Surcharge",
            "Deposit",
            "EcoFees",
            "EnviroFees",
            "OtherCharges",
            "Other Credit Charges",
            "ShipmentCharges",
            "TotalDiscount",
            "Usage Charges",
        ]

        tab_ck_list = ["Quantity", "UnitPrice", "Amount", "AmountExcTax"]

        credit_note = 0
        for tg in data["header"]:
            if tg["tag"] == "Credit Identifier":
                if "credit" in tg["data"]["value"].lower():
                    credit_note = 1
                    break

        if credit_note == 1:
            for crt_tg in data["header"]:
                if crt_tg["tag"] in hdr_ck_list:
                    try:
                        crt_tg["data"]["value"] = str(
                            float(crt_tg["data"]["value"]) * -1
                        )
                    except Exception:
                        logger.info(
                            f"Invalid {crt_tg['tag']} : {crt_tg['data']['value']} "
                        )

            for line_cr_tg in data["tab"]:
                for cr_rw in line_cr_tg:
                    if cr_rw["tag"] in tab_ck_list:
                        try:
                            cr_rw["data"] = str(float(cr_rw["data"]) * -1)
                        except Exception:
                            logger.info(f"invalid {cr_rw['tag']} : {cr_rw['data']}")
                            crt_tg["status"] = 0
    except Exception:
        logger.error("Credit Note Error")
        logger.error(traceback.format_exc())

    # create Invoice record

    current_ime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    if poNumber is None or poNumber == "":
        try:
            poNumber = list(
                filter(lambda d: d["tag"] == "PurchaseOrder", data["header"])
            )[0]["data"]["value"]
        except Exception:
            logger.error(traceback.format_exc())
            poNumber = ""
    # resp = requests.get(filepath)
    # file_content = BytesIO(resp.content).getvalue()
    # ref_url = getfile_as_base64(filename, filetype, file_content)
    ref_url = filepath
    logger.info(f"ref_url: {ref_url}")
    # parse tag labels data and push into ivoice data table
    # print("data: line 620",data)
    doc_header_data, doc_header, error_labels = parse_labels(
        data["header"], db, poNumber, modelID
    )
    # parse line item data and push into invoice line itemtable
    doc_line_data, error_line_items = parse_tabel(data["tab"], db, modelID)
    invoice_data = {
        "idDocumentType": 3,
        "documentModelID": modelID,
        "entityID": entityID,
        "entityBodyID": entityBodyID,
        "docheaderID": doc_header["docheaderID"] if "docheaderID" in doc_header else "",
        "totalAmount": (
            doc_header["totalAmount"] if "totalAmount" in doc_header else "0"
        ),
        "documentStatusID": docStatus,
        "documentDate": (
            doc_header["documentDate"] if "documentDate" in doc_header else ""
        ),
        "vendorAccountID": vendorAccountID,
        "documentTotalPages": no_pages_processed,
        "CreatedOn": current_ime,
        "sourcetype": source,
        "sender": sender,
        "docPath": ref_url,
        "UploadDocType": UploadDocType,
        "documentsubstatusID": docsubstatus,
        "mail_row_key": mail_row_key,
    }

    try:
        try:
            # if invoice_data.get("vendorAccountID") == 0:
            #     invoice_data.pop("vendorAccountID")

            # Convert totalAmount to a float
            invoice_data["totalAmount"] = float(invoice_data["totalAmount"]) if invoice_data["totalAmount"] else 0.0

            # Ensure documentDate is either None or a valid date
            invoice_data["documentDate"] = invoice_data["documentDate"] if invoice_data["documentDate"] else None
        except Exception as e:
            logger.debug(f"{traceback.format_exc()}")
        # if vendorAccountID==0:

        #     # invoice_data.pop('userID')
        #     invoice_data.pop('vendorAccountID')

        db_data = model.Document(**invoice_data)
        db.add(db_data)
        db.commit()
    except Exception as e:
        logger.debug(f"{traceback.format_exc()}")
        db.rollback()
        if "Incorrect datetime value" in str(e):
            invoice_data["documentDate"] = None
        try:

            db_data = model.Document(**invoice_data)
            db.add(db_data)
            db.commit()
        except Exception as e:
            logger.debug(f"{traceback.format_exc()}")
            db.rollback()
            if "for column 'docheaderID'" in str(e):
                invoice_data["docheaderID"] = ""
            try:
                db_data = model.Document(**invoice_data)
                db.add(db_data)
                db.commit()
            except Exception as e:
                logger.debug(f"{traceback.format_exc()}")
                db.rollback()
                if "for column 'PODocumentID'" in str(e):
                    invoice_data["PODocumentID"] = ""
                try:

                    db_data = model.Document(**invoice_data)
                    db.add(db_data)
                    db.commit()
                except Exception as e:
                    logger.debug(f"{traceback.format_exc()}")
                    db.rollback()
                    if "for column 'totalAmount'" in str(e):
                        invoice_data["totalAmount"] = None

                    db_data = model.Document(**invoice_data)

                    db.add(db_data)
                    db.commit()
    invoiceID = db_data.idDocument
    logger.info(f"invoiceID: {invoiceID}, invoice_data: {invoice_data}")
    for dh in doc_header_data:
        dh["documentID"] = invoiceID
        db_header = model.DocumentData(**dh)
        db.add(db_header)
        db.commit()
    for dl in doc_line_data:
        dl["documentID"] = invoiceID
        db_line = model.DocumentLineItems(**dl)
        db.add(db_line)
        db.commit()
    user_details = (
        db.query(model.User.firstName, model.User.lastName)
        .filter(model.User.idUser == userID)
        .first()
    )
    user_name = (
        user_details[0]
        if user_details[0] is not None
        else "" + " " + user_details[1] if user_details[1] is not None else ""
    )
    update_docHistory(invoiceID, userID, 0, f"Invoice Uploaded By {user_name}", db,docsubstatus)

    # update document history table
    return invoiceID


def parse_labels(label_data, db, poNumber, modelID):
    try:
        error_labels_tag_ids = []
        doc_header = {}
        data_to_add = []
        for label in label_data:
            db_data = {}
            db_data["documentTagDefID"] = get_labelId(db, label["tag"], modelID)
            db_data["CreatedOn"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            if label["tag"] == "PurchaseOrder":
                db_data["Value"] = poNumber
            else:
                db_data["Value"] = label["data"]["value"]
            try:
                if (
                    "prebuilt_confidence" in label["data"]
                    and label["data"]["prebuilt_confidence"] != ""
                ):
                    confidence = (
                        round(float(label["data"]["prebuilt_confidence"]) / 100, 2)
                        if float(label["data"]["prebuilt_confidence"]) > 1
                        else label["data"]["prebuilt_confidence"]
                    )
                    db_data["Fuzzy_scr"] = str(confidence)
                else:
                    db_data["Fuzzy_scr"] = "0.0"
                if (
                    "custom_confidence" in label["data"]
                    and label["data"]["custom_confidence"] != ""
                ):
                    confidence = (
                        round(float(label["data"]["custom_confidence"]) / 100, 2)
                        if float(label["data"]["custom_confidence"]) > 1
                        else label["data"]["custom_confidence"]
                    )
                    db_data["Fuzzy_scr"] = str(confidence)
                else:
                    db_data["Fuzzy_scr"] = "0.0"
            except Exception:
                logger.debug(traceback.format_exc())
                db_data["Fuzzy_scr"] = "0.0"
            db_data["IsUpdated"] = 0
            if label["status"] == 1:
                db_data["isError"] = 0
            else:
                error_labels_tag_ids.append(label["tag"])
                db_data["isError"] = 1
            db_data["ErrorDesc"] = label["status_message"]
            if label["bounding_regions"]:
                db_data["Xcord"] = label["bounding_regions"]["x"]
                db_data["Ycord"] = label["bounding_regions"]["y"]
                db_data["Width"] = label["bounding_regions"]["w"]
                db_data["Height"] = label["bounding_regions"]["h"]
            if label["tag"] in docLabelMap.keys():
                doc_header[docLabelMap[label["tag"]]] = label["data"]["value"]
            data_to_add.append(db_data)
        return data_to_add, doc_header, error_labels_tag_ids
    except Exception:
        logger.error(traceback.format_exc())
        return {"DB error": "Error while inserting document data"}


def parse_tabel(tabel_data, db, modelID):
    error_labels_tag_ids = []
    data_to_add = []
    for row in tabel_data:
        for col in row:
            db_data = {}
            db_data["Value"] = col["data"]
            db_data["CreatedOn"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            try:
                if (
                    "prebuilt_confidence" in col["data"]
                    and col["data"]["prebuilt_confidence"] != ""
                ):
                    confidence = (
                        round(float(col["data"]["prebuilt_confidence"]) / 100, 2)
                        if float(col["data"]["prebuilt_confidence"]) > 1
                        else col["data"]["prebuilt_confidence"]
                    )
                    db_data["Fuzzy_scr"] = str(confidence)
                else:
                    db_data["Fuzzy_scr"] = "0.0"
                if (
                    "custom_confidence" in col["data"]
                    and col["data"]["custom_confidence"] != ""
                ):
                    confidence = (
                        round(float(col["data"]["custom_confidence"]) / 100, 2)
                        if float(col["data"]["custom_confidence"]) > 1
                        else col["data"]["custom_confidence"]
                    )
                    db_data["Fuzzy_scr"] = str(confidence)
                else:
                    db_data["Fuzzy_scr"] = "0.0"
            except Exception:
                logger.debug(traceback.format_exc())
                db_data["Fuzzy_scr"] = "0"
            db_data["lineItemtagID"] = get_lineitemTagId(db, col["tag"], modelID)
            if "status" in col:
                if col["status"] == 1:
                    db_data["isError"] = 0
                else:
                    error_labels_tag_ids.append(col["tag"])
                    db_data["isError"] = 1
                db_data["ErrorDesc"] = col["status_message"]
            if col["bounding_regions"]:
                db_data["Xcord"] = col["bounding_regions"]["x"]
                db_data["Ycord"] = col["bounding_regions"]["y"]
                db_data["Width"] = col["bounding_regions"]["w"]
                db_data["Height"] = col["bounding_regions"]["h"]
            db_data["itemCode"] = col["row_count"]
            db_data["invoice_itemcode"] = col["row_count"]
            data_to_add.append(db_data)
    return data_to_add, error_labels_tag_ids


def get_lineitemTagId(db, item, modelID):
    # print("Tab :", item)
    result = (
        db.query(model.DocumentLineItemTags)
        .filter(
            model.DocumentLineItemTags.TagName == item,
            model.DocumentLineItemTags.idDocumentModel == modelID,
        )
        .first()
    )
    if result is not None:
        return result.idDocumentLineItemTags


def get_labelId(db, item, modelID):
    try:
        result = (
            db.query(model.DocumentTagDef)
            .filter(
                model.DocumentTagDef.TagLabel == item,
                model.DocumentTagDef.idDocumentModel == modelID,
            )
            .first()
        )
        if result is not None:
            return result.idDocumentTagDef
    except Exception:
        logger.error(f"{traceback.format_exc()}")
        return None





def store_gst_hst_amount(
    invoId, GST_HST_Amount, gst_hst_isErr, gst_hst_Ck_msg, IsUpdated, db
):
    # Get documentModelID for the given document
    doc_model_id = (
        db.query(model.Document.documentModelID)
        .filter(model.Document.idDocument == invoId)
        .scalar()
    )

    # Fetch or create 'GST/HST Amount' tag definition
    gst_hst_tag_def = (
        db.query(model.DocumentTagDef)
        .filter(
            model.DocumentTagDef.idDocumentModel == doc_model_id,
            model.DocumentTagDef.TagLabel == "GST",
        )
        .first()
    )

    if not gst_hst_tag_def:
        gst_hst_tag_def = model.DocumentTagDef(
            idDocumentModel=doc_model_id,
            TagLabel="GST",
            CreatedOn=func.now(),
        )
        db.add(gst_hst_tag_def)
        db.commit()  # Commit to get the ID of the newly inserted DocumentTagDef

    # Check if the corresponding entry in DocumentData exists
    document_data = (
        db.query(model.DocumentData)
        .filter(
            model.DocumentData.documentID == invoId,
            model.DocumentData.documentTagDefID == gst_hst_tag_def.idDocumentTagDef,
        )
        .first()
    )

    # Set the values based on the existing data or create a new entry
    if document_data:
        document_data.Value = GST_HST_Amount  # GST/HST Amount
        document_data.isError = gst_hst_isErr  # Error status
        document_data.ErrorDesc = gst_hst_Ck_msg  # Error description
        document_data.IsUpdated = IsUpdated  # Update flag
    else:
        # Insert new DocumentData entry if it does not exist
        document_data = model.DocumentData(
            documentID=invoId,
            documentTagDefID=gst_hst_tag_def.idDocumentTagDef,
            Value=GST_HST_Amount,
            isError=gst_hst_isErr,
            ErrorDesc=gst_hst_Ck_msg,
            IsUpdated=IsUpdated,
            CreatedOn=func.now(),
        )
        db.add(document_data)

    # Commit the changes for DocumentData
    db.commit()
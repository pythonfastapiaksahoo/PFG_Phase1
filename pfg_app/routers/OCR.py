# import logging
import io
import json
import os
import re
import traceback
from datetime import datetime, timezone

import pandas as pd

# import psycopg2
import pytz as tz
from fastapi import APIRouter, File, Form, Response, UploadFile
from PIL import Image

# from psycopg2 import extras
from pypdf import PdfReader
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import func
from sqlalchemy.orm import Load

import pfg_app.model as model
from pfg_app import settings
from pfg_app.auth import AuthHandler

# from pfg_app.azuread.auth import get_user
# from pfg_app.azuread.schemas import AzureUser
from pfg_app.core.azure_fr import get_fr_data
from pfg_app.core.stampData import is_valid_date
from pfg_app.FROps.pfg_trigger import (
    IntegratedvoucherData,
    nonIntegratedVoucherData,
    pfg_sync,
)
from pfg_app.FROps.postprocessing import getFrData_MNF, postpro
from pfg_app.FROps.preprocessing import fr_preprocessing
from pfg_app.FROps.SplitDoc import splitDoc
from pfg_app.FROps.validate_currency import validate_currency
from pfg_app.logger_module import logger

# from logModule import email_sender
from pfg_app.session.session import SQLALCHEMY_DATABASE_URL, get_db

# model.Base.metadata.create_all(bind=engine)
auth_handler = AuthHandler()
tz_region_name = os.getenv("serina_tz", "Asia/Dubai")
tz_region = tz.timezone(tz_region_name)

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
):
    try:
        try:
            # Regular expression pattern to find "DSD-" followed by digits
            match = re.search(r"/DSD-\d+/", file_path)

            # Extract mail_row_key if pattern is found, else assign None
            mail_row_key = match.group(0).strip("/") if match else None
        except Exception:
            logger.error(f"Error in file path: {str(traceback.format_exc())}")
        # email_path = ""
        # subject = ""
        vendorAccountID = 0
        db = next(get_db())
        # Create a new instance of the SplitDocTab model
        new_split_doc = model.SplitDocTab(
            invoice_path=file_path,
            status="File Received without Check",
            emailbody_path=email_path,
            email_subject=subject,
            sender=sender,
            mail_row_key=mail_row_key,
        )

        # Add the new entry to the session
        db.add(new_split_doc)

        # Commit the transaction to save it to the database
        db.commit()

        # Refresh the instance to get the new ID if needed
        db.refresh(new_split_doc)
    except Exception:
        logger.error(f"{traceback.format_exc()}")

    try:
        fl_type = filename.split(".")[-1]
        # -------------------------

        if fl_type in ["png", "jpg", "jpeg", "jpgx"]:
            image = Image.open(file.file)

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
            pdf_stream = PdfReader(file.file)
        else:
            splitdoc_id = new_split_doc.splitdoc_id
            split_doc = (
                db.query(model.SplitDocTab)
                .filter(model.SplitDocTab.splitdoc_id == splitdoc_id)
                .first()
            )

            if split_doc:
                split_doc.status = "Unsupported File Format: " + fl_type
                split_doc.updated_on = datetime.now()  # Update the timestamp

                # Commit the update
                db.commit()
            return f"Unsupported File Format: {fl_type}"

        modelData = None
        IsUpdated = 0
        invoId = ""
        customerID = 1
        userID = 1
        # logger.info(f"userID: {userID}")
        """'file_path': blob_url, 'filename': blob_name, 'file_type':
        file_type, 'source': 'Azure Blob Storage', 'invoice_type':
        invoice_type."""

        logger.info(
            f"file_path: {file_path}, filename: {filename}, file_type: {file_type},\
            source: {source}, invoice_type: {invoice_type}"
        )
        # db = next(get_db())

        containername = "invoicesplit-test"  # TODO move to settings
        subfolder_name = "DSD/splitInvo"  # TODO move to settings
        destination_container_name = "apinvoice-container"  # TODO move to settings
        fr_API_version = "2023-07-31"  # TODO move to settings

        prompt = """This is an invoice document containing an invoice ID,
        vendor name, and a stamp with handwritten or stamped information, possibly
        including a receiver's stamp. The document may include the following details:

        Store Number: Typically stamped and starting with either 'STR#' or "#".

        Marked Department: Clearly circled or marked "Inventory" or "Supplies"
        (if neither is marked, set this to "N/A") sometimes print might not be clear,
        in those case also it can return N/A.

        Department: Either a department code or department name, handwritten
        and possibly starting with "Dept".

        Receiving Date: The date when goods were received.

        Confirmation Number: A 9-digit number, usually handwritten and labeled
        as "Confirmation".

        Receiver: The name or code of the person who received the goods
        (may appear as "Receiver#" or by name).

        Invoice Number: The unique number identifying the invoice.

        Currency: Identified by currency symbols (e.g., CAD, USD)
        or determined by the country in the invoice address if
        a currency symbol is not found.

        Instructions
        Invoice Document: Yes/No
        Invoice ID: [Extracted Invoice ID].
        Stamp Present: Yes/No
        If a stamp is present, extract the following information
        and output it in JSON format:

        json
        Copy code
        {
            "StampFound": "Yes/No",
            "NumberOfPages": "Total number of pages in the document",
            "MarkedDept": "Inventory/Supplies/N/A",
            "Confirmation": "Extracted 13-digit confirmation number",
            "ReceivingDate": "Extracted receiving date",
            "Receiver": "Extracted receiver information",
            "Department": "Extracted department code or name",
            "Store Number": "Extracted store number",
            "VendorName": "Extracted vendor name",
            "InvoiceID": "Extracted invoice ID",
            "Currency": "Extracted currency"
        }
        Notes
        MarkedDept: Return "Inventory" or "Supplies" based on the clearly
        circled or marked option. If neither is marked, return "N/A".
        Confirmation: Extract a 13-digit confirmation number.
        Format: Output strictly in the JSON format with unique keys provided above,
        Store Number: Must be a 4 digit. If its less than 4 digit than add leading zeros else return N/A its not clear.
        with no additional text or explanations."""

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
        if fr_model_status == 1:

            query = db.query(
                model.Vendor.idVendor,
                model.Vendor.VendorName,
                model.Vendor.Synonyms,
                model.Vendor.Address,
            ).filter(
                func.jsonb_extract_path_text(
                    model.Vendor.miscellaneous, "VENDOR_STATUS"
                )
                == "A"
            )
            rows = query.all()
            columns = ["idVendor", "VendorName", "Synonyms", "Address"]

            vendorName_df = pd.DataFrame(rows, columns=columns)

            splitdoc_id = new_split_doc.splitdoc_id
            split_doc = (
                db.query(model.SplitDocTab)
                .filter(model.SplitDocTab.splitdoc_id == splitdoc_id)
                .first()
            )
            print("grp_pages: ", grp_pages)
            if split_doc:
                # Update the fields
                split_doc.pages_processed = grp_pages
                split_doc.status = "File Received"
                split_doc.totalpagecount = num_pages
                split_doc.num_pages = num_pages
                split_doc.updated_on = datetime.now()  # Update the timestamp

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
                try:
                    InvofileSize = fileSize[spltFileName]
                except Exception:
                    logger.error(f"{traceback.format_exc()}")
                    InvofileSize = ""
                try:

                    frtrigger_insert_data = {
                        "blobpath": spltFileName,
                        "status": "File received",
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
                    # logger.info(f"DI prbtHeaders: {prbtHeaders}")
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

                    spltFileName = splitfileNames[fl]
                    try:
                        stop = False
                        for syn, vName in zip(
                            vendorName_df["Synonyms"], vendorName_df["idVendor"]
                        ):
                            if stop:
                                break
                                # print("syn: ",syn,"   vName: ",vName)
                            if syn is not None or str(syn) != "None":
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
                                                    vendorID = vName
                                                    logger.info(
                                                        f"cos_sim:{cos_sim_di} , \
                                                            vendor:{vName}"
                                                    )
                                                    stop = True
                                                    break
                                            elif cos_sim_stmp[0][0] * 100 >= 95:
                                                vdrFound = 1
                                                vendorID = vName
                                                logger.info(
                                                    f"cos_sim:{cos_sim_stmp} , \
                                                        vendor:{vName}"
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
                                                    vendorID = vName
                                                    stop = True
                                                    break
                                                elif (
                                                    syn2.replace("\n", " ")
                                                    == di_inv_vendorName
                                                ):

                                                    vdrFound = 1
                                                    vendorID = vName
                                                    stop = True
                                                    break
                                            elif stamp_inv_vendorName != "":
                                                if syn2 == stamp_inv_vendorName:

                                                    vdrFound = 1
                                                    vendorID = vName
                                                    stop = True
                                                    break
                                                elif (
                                                    syn2.replace("\n", " ")
                                                    == stamp_inv_vendorName
                                                ):

                                                    vdrFound = 1
                                                    vendorID = vName
                                                    stop = True
                                                    break

                    except Exception:
                        logger.error(f"{traceback.format_exc()}")

                        vdrFound = 0

                except Exception:

                    logger.error(f"{traceback.format_exc()}")
                    vdrFound = 0

                # vxdrFound = 0
                if vdrFound == 1:

                    try:
                        metaVendorAdd = list(
                            vendorName_df[vendorName_df["idVendor"] == vendorID][
                                "Address"
                            ]
                        )[0]

                    except Exception:
                        logger.error(f"{traceback.format_exc()}")
                        metaVendorAdd = ""
                    try:
                        metaVendorName = list(
                            vendorName_df[vendorName_df["idVendor"] == vendorID][
                                "VendorName"
                            ]
                        )[0]
                    except Exception:
                        logger.error(f"{traceback.format_exc()}")

                        metaVendorName = ""
                    vendorAccountID = vendorID
                    poNumber = "nonPO"
                    VendoruserID = 1
                    configs = getOcrParameters(customerID, db)
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
                            )

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
                        API_version = configs.ApiVersion

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
                            # "pre_data": "",
                            # "pre_status": "",
                            # "pre_model_msg": "",
                        }

                        try:
                            invoId = live_model_fn_1(generatorObj)
                            logger.info(f"DocumentID:{invoId}")
                        except Exception:
                            invoId = ""
                            logger.error(f"{traceback.format_exc()}")

                        try:
                            if len(str(invoId)) == 0:
                                preBltFrdata, preBltFrdata_status = getFrData_MNF(
                                    rwOcrData[grp_pages[fl][0] - 1 : grp_pages[fl][1]]
                                )
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
                                )

                                logger.info(
                                    f" Onboard vendor Pending: invoice_ID: {invoId}"
                                )
                                status = "success"
                                try:

                                    fr_trigger = db.query(model.frtrigger_tab).filter
                                    (model.frtrigger_tab.blobpath == spltFileName)

                                    # Step 2: Perform the update operation
                                    fr_trigger.update(
                                        {
                                            model.frtrigger_tab.status: "PostProcessing Error",  # noqa: E501
                                            model.frtrigger_tab.vendorID: vendorID,
                                            model.frtrigger_tab.documentid: invoId,
                                        }
                                    )
                                    # Step 3: Commit the transaction
                                    db.commit()

                                except Exception:
                                    logger.error(f"{traceback.format_exc()}")

                        except Exception:
                            logger.error(f"{traceback.format_exc()}")
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
                        )

                        logger.info(f" VendorUnidentified: invoice_ID: {invoId}")
                        status = "success"

                    except Exception:

                        logger.debug(traceback.format_exc())
                        status = "fail"

                    logger.info("vendor not found!!")
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
                    except Exception as et:
                        logger.debug(traceback.format_exc())
                        try:

                            db.query(model.frtrigger_tab).filter(
                                model.frtrigger_tab.blobpath == spltFileName
                            ).update(
                                {
                                    model.frtrigger_tab.status: str(et),
                                    model.frtrigger_tab.documentid: invoId,
                                }
                            )

                            # Commit the transaction
                            db.commit()
                        except Exception:
                            logger.error(f"{traceback.format_exc()}")

                    status = traceback.format_exc()
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
                        if "MarkedDept" in StampDataList[splt_map[fl]]:
                            MarkedDept = StampDataList[splt_map[fl]]["MarkedDept"]
                            if MarkedDept == "Inventory" or MarkedDept == "Supplies":
                                mrkDeptCk_isErr = 0
                                mrkDeptCk_msg = ""
                            else:
                                mrkDeptCk_isErr = 1
                                mrkDeptCk_msg = "Invalid. Please review."

                        else:
                            mrkDeptCk_isErr = 1
                            mrkDeptCk_msg = "Not Found."
                            MarkedDept = "N/A"
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
                                    confCk_isErr = 0
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
                                IntegratedvoucherData(invoId, gst_amt, db)
                            elif store_type == "Non-Integrated":
                                nonIntegratedVoucherData(invoId, gst_amt, db)
                        except Exception:
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
                )

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
                            model.frtrigger_tab.status: "PostProcessing Error",
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

    except Exception as err:

        logger.error(f"API exception ocr.py: {traceback.format_exc()}")
        status = "error: " + str(err)

    try:

        if vdrFound == 1 and modelData is not None:
            customCall = 0
            pfg_sync(invoId, userID, db, customCall)

    except Exception:
        logger.debug(f"{traceback.format_exc()}")

    return status


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
                docStatus = 10
                docsubstatus = 12
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

                db.close()
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
):

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
        "totalAmount": doc_header["totalAmount"] if "totalAmount" in doc_header else "",
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
    }

    try:
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
    update_docHistory(invoiceID, userID, 0, f"Invoice Uploaded By {user_name}", db)

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


def update_docHistory(documentID, userID, documentstatus, documentdesc, db):
    try:
        docHistory = {}
        docHistory["documentID"] = documentID
        docHistory["userID"] = userID
        docHistory["documentStatusID"] = documentstatus
        docHistory["documentdescription"] = documentdesc
        docHistory["CreatedOn"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        db.add(model.DocumentHistoryLogs(**docHistory))
        db.commit()
    except Exception:
        logger.error(traceback.format_exc())
        db.rollback()
        return {"DB error": "Error while inserting document history"}

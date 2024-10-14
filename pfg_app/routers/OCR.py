
# import logging
import json
import os
import time
import traceback
from datetime import datetime, timezone

import pandas as pd
import psycopg2
import pytz as tz
from fastapi import APIRouter, File, Form, Response, UploadFile
from psycopg2 import extras
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
from pfg_app.FROps.pfg_trigger import (
    IntegratedvoucherData,
    nonIntegratedVoucherData,
    pfg_sync,
)
from pfg_app.FROps.postprocessing import getFrData_MNF, postpro
from pfg_app.FROps.preprocessing import fr_preprocessing
from pfg_app.FROps.SplitDoc import splitDoc
from pfg_app.FROps.stampData import is_valid_date
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
    # email_path: str = Form(...),
    # subject: str = Form(...),
    # user: AzureUser = Depends(get_user),
):
    try:
        db = next(get_db())
        # Create a new instance of the SplitDocTab model
        new_split_doc = model.SplitDocTab(
            invoice_path=file_path,
            status="File Received without Check",
            emailbody_path="email_path",
            email_subject="subject",
            sender=sender,
        )

        # Add the new entry to the session
        db.add(new_split_doc)

        # Commit the transaction to save it to the database
        db.commit()

        # Refresh the instance to get the new ID if needed
        db.refresh(new_split_doc)
    except Exception as e:
        print(e)

    try:
        invoId = ""
        customerID = 1
        userID = 1
        logger.info(f"userID: {userID}")
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

        # prompt = """This is an invoice document.Extract Invoice Number and
        # Extract the Currency from the invoice document by identifying the currency
        # symbol before the total amount. The currency can be CAD or USD.
        # If the invoice address is in Canada, set the currency to CAD,
        # otherwise set it to as per invoice address.
        # It may contain a receiver's stamp and
        # might have inventory or supplies marked or circled with a pen, circled is
        # selected. It contains store number as "STR #"
        # InvoiceDocument: Yes/No InvoiceID: [InvoiceID]. StampPresent: Yes/No.
        # If a stamp is present, identify any markings on the document related to
        # Inventory or Supplies, specifically if they are marked or circled with a pen.
        # If a stamp is present, extract the following handwritten details
        # from the stamp: ConfirmationNumber (the confirmation number labeled as
        # 'Confirmation' on the stamp),
        # ReceivingDate (the date when the goods were received),
        # Receiver (the name of the person or department who received the goods), and
        # Department (the handwritten department name or code,
        # or another specified departmentname), MarkedDept (which may be either
        # 'Inventory' or 'Supplies', based on pen marking).
        # Provide all information in the following JSON format:
        # {'StampFound': 'Yes/No', 'MarkedDept': 'Inventory/Supplies'
        # (which ever is circled more/marked only),
        # 'Confirmation': 'Extracted data', 'ReceivingDate': 'Extracted data',
        # 'Receiver': 'Extracted data', 'Department': 'Dept code',
        # 'Store Number':,'VendorName':,'InvoiceID':,'Currency':}.Output should be
        # just json"""

        prompt = """This is an invoice document. It may contain a receiver's stamp and
        might have inventory or supplies marked or circled with a pen, circled
        is selected. It contains store number as "STR #".

        InvoiceDocument: Yes/No
        InvoiceID: [InvoiceID].
        StampPresent: Yes/No.

        If a stamp is present, identify any markings on the document related to
        Inventory or Supplies, specifically if they are marked or circled with a pen.
        If a stamp is present, extract the following handwritten details from the
        stamp: ConfirmationNumber (the confirmation number labeled
        as 'Confirmation' on the stamp), ReceivingDate
        (the date when the goods were received), Receiver
        (the name of the person or department who received the goods),
        and Department (the handwritten department name or code,
        or another specified department name),
        MarkedDept (which may be either 'Inventory' or 'Supplies',
        based on pen marking).
        Extract the Invoice Number.
        Extract the Currency from the invoice document by identifying the currency
        symbol before the total amount. The currency can be CAD or USD.
        If the invoice address is in Canada, set the currency to CAD,
        otherwise set it to USD.

        Provide all information in the following JSON format:
        {
            'StampFound': 'Yes/No',
            'MarkedDept': 'Inventory/Supplies' (whichever is circled more/marked only),
            'Confirmation': 'Extracted data',
            'ReceivingDate': 'Extracted data',
            'Receiver': 'Extracted data',
            'Department': 'Dept code',
            'Store Number': 'Extracted data',
            'VendorName': 'Extracted data',
            'InvoiceID' : 'Extracted data'
            'Currency': 'Extracted data'
        }.

        Output should always be in JSON format only."""
        # TODO move to settings

        pdf_stream = PdfReader(file.file)

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
            # logger.info(f"StampDataList: {StampDataList}")
            # conn_params: Dict[str, str] = {
            #     "dbname": settings.db_name,
            #     "user": settings.db_user,
            #     "password": settings.db_password,
            #     "host": settings.db_host,
            #     "port": str(settings.db_port),
            # }

            conn = psycopg2.connect(
                dbname=settings.db_name,
                user=settings.db_user,
                password=settings.db_password,
                host=settings.db_host,
                port=str(settings.db_port),  # Ensure port is a string
            )
            cursor = conn.cursor()
            # cursor.execute(
            #     'SELECT "idVendor","VendorName","Synonyms","Address" \
            #         FROM pfg_schema.vendor;'
            # )
            # rows = cursor.fetchall()
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

            # if cursor.description is not None:
            #     colnames = [desc[0] for desc in cursor.description]
            # else:
            #     colnames = []  # Handle the case where cursor.description is None
            # vendorName_df = pd.DataFrame(rows, columns=colnames)
            time.sleep(0.5)
            cursor = conn.cursor()
            # insert_splitTab_query = """
            #     INSERT INTO pfg_schema.splitdoctab \
            #         (invoice_path, totalpagecount, pages_processed, status,\
            #             emailbody_path)
            #     VALUES (%s, %s, %s, %s, %s);
            # """

            # cursor.execute(
            #     insert_splitTab_query,
            #     (file_path, num_pages, grp_pages, "File received", sender),
            # )
            # conn.commit()
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

                except Exception as rt:
                    logger.info(f"line 180: DB insertion error:{str(rt)} ")

                if "VendorName" in prbtHeaders[splt_map[fl]]:
                    # logger.info(f"DI prbtHeaders: {prbtHeaders}")
                    inv_vendorName = prbtHeaders[splt_map[fl]]["VendorName"][0]
                    di_inv_vendorName = inv_vendorName
                    logger.info(f" DI inv_vendorName: {inv_vendorName}")
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
                                        syn_1 = syn1.split(",")

                                        for syn2 in syn_1:

                                            tfidf_matrix_di = vectorizer.fit_transform(
                                                [syn2, di_inv_vendorName]
                                            )
                                            cos_sim_di = cosine_similarity(
                                                tfidf_matrix_di[0], tfidf_matrix_di[1]
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

                    except Exception as rt:
                        logger.info(f"ocr.py line 220 {str(rt)}")
                        vdrFound = 0

                except Exception as er:
                    logger.info(f"ocr.py exception: {str(er)}")
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
                        metaVendorAdd = ""
                    try:
                        metaVendorName = list(
                            vendorName_df[vendorName_df["idVendor"] == vendorID][
                                "VendorName"
                            ]
                        )[0]
                    except Exception as err:
                        logger.error(f"metaVendorName exception:{str(err)}")
                        metaVendorName = ""
                    vendorAccountID = str(vendorID)
                    poNumber = "nonPO"
                    VendoruserID = 1
                    configs = getOcrParameters(customerID, db)
                    metadata = getMetaData(vendorAccountID, db)
                    entityID = 1
                    modelData, modelDetails = getModelData(vendorAccountID, db)

                    if modelData is None:
                        try:
                            preBltFrdata, preBltFrdata_status = getFrData_MNF(rwOcrData)

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

                        except Exception as e:
                            logger.info(
                                f"getFrData_MNF Exception line 446 orc.py: {str(e)}"
                            )
                            status = "fail"

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
                        endpoint = configs.Endpoint
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
                        }
                        try:
                            invoId = live_model_fn_1(generatorObj)
                            logger.info(f"DocumentID:{invoId}")
                        except Exception as e:
                            invoId = ""
                            logger.error(f"Exception in live_model_fn_1: {str(e)}")

                        try:

                            if len(str(invoId)) == 0:
                                preBltFrdata, preBltFrdata_status = getFrData_MNF(
                                    rwOcrData
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
                                    # cur = conn.cursor()
                                    # sql_updateFR = """UPDATE pfg_schema.frtrigger_tab \  # noqa: E501
                                    #             SET "status" = %s, "sender" = %s, \
                                    #             "vendorID" = %s \
                                    #         WHERE "blobpath" = %s; """
                                    # FRvalues = (
                                    #     "PostProcessing Error",
                                    #     sender,
                                    #     vendorID,
                                    #     spltFileName,
                                    # )
                                    # cur.execute(sql_updateFR, FRvalues)
                                    # conn.commit()
                                    fr_trigger = db.query(model.frtrigger_tab).filter
                                    (model.frtrigger_tab.blobpath == spltFileName)

                                    # Step 2: Perform the update operation
                                    fr_trigger.update(
                                        {
                                            model.frtrigger_tab.status: "PostProcessing Error",  # noqa: E501
                                            model.frtrigger_tab.vendorID: vendorID,
                                        }
                                    )
                                    # Step 3: Commit the transaction
                                    db.commit()

                                except Exception as qw:
                                    logger.info(f"ocr.py line 475: {str(qw)}")

                        except Exception as e:
                            logger.info(
                                f"Postprocessing Exception line 446 orc.py: {str(e)}"
                            )
                            status = "fail"

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

                        except Exception as e:
                            print(f"Error occurred: {e}")

                        if "StampFound" in StampDataList[splt_map[fl]]:
                            stm_dt_lt = []
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
                                    MarkedDept = StampDataList[splt_map[fl]][
                                        "MarkedDept"
                                    ]
                                    if (
                                        MarkedDept == "Inventory"
                                        or MarkedDept == "Supplies"
                                    ):
                                        mrkDeptCk_isErr = 0
                                        mrkDeptCk_msg = ""
                                    else:
                                        mrkDeptCk_isErr = 1
                                        mrkDeptCk_msg = "Invalid. Please review."

                                else:
                                    mrkDeptCk_isErr = 1
                                    mrkDeptCk_msg = "Not Found."
                                    MarkedDept = "N/A"

                                stmp_dt = [
                                    invoId,
                                    "SelectedDept",
                                    MarkedDept,
                                    mrkDeptCk_isErr,
                                    mrkDeptCk_msg,
                                    stmp_created_on,
                                ]
                                stm_dt_lt.append(stmp_dt)

                                if "Confirmation" in StampDataList[splt_map[fl]]:
                                    Confirmation = StampDataList[splt_map[fl]][
                                        "Confirmation"
                                    ]
                                    str_nm = ""
                                    if len(Confirmation) == 9:
                                        try:

                                            query = 'SELECT * \
                                                FROM pfg_schema.pfgreceipt \
                                                    WHERE "RECEIVER_ID" = %s'
                                            cursor.execute(query, (Confirmation,))
                                            row = cursor.fetchone()
                                            if row:
                                                confCk_isErr = 0
                                                confCk_msg = "Valid Confirmation Number"
                                                str_nm = row[15]
                                            else:
                                                confCk_isErr = 1
                                                confCk_msg = (
                                                    "Confirmation Number Not Found"
                                                )

                                        except Exception as e:
                                            logger.error(
                                                f"Error executing query: {str(e)}"
                                            )

                                            confCk_isErr = 0
                                            confCk_msg = "Error:" + str(e)

                                    else:
                                        confCk_isErr = 1
                                        confCk_msg = "Invalid Confirmation Number"

                                else:
                                    Confirmation = "N/A"
                                    confCk_isErr = 1
                                    confCk_msg = "Confirmation Number NotFound"

                                stmp_dt = [
                                    invoId,
                                    "ConfirmationNumber",
                                    Confirmation,
                                    confCk_isErr,
                                    confCk_msg,
                                    stmp_created_on,
                                ]
                                stm_dt_lt.append(stmp_dt)

                                if "ReceivingDate" in StampDataList[splt_map[fl]]:
                                    ReceivingDate = StampDataList[splt_map[fl]][
                                        "ReceivingDate"
                                    ]
                                    if is_valid_date(ReceivingDate):
                                        RevDateCk_isErr = 0
                                        RevDateCk_msg = ""
                                    else:
                                        RevDateCk_isErr = 1
                                        RevDateCk_msg = "Invalid Date Format"
                                else:
                                    ReceivingDate = "N/A"
                                    RevDateCk_isErr = 1
                                    RevDateCk_msg = "ReceivingDate Not Found."

                                stmp_dt = [
                                    invoId,
                                    "ReceivingDate",
                                    ReceivingDate,
                                    RevDateCk_isErr,
                                    RevDateCk_msg,
                                    stmp_created_on,
                                ]
                                stm_dt_lt.append(stmp_dt)

                                if "Receiver" in StampDataList[splt_map[fl]]:
                                    Receiver = StampDataList[splt_map[fl]]["Receiver"]
                                    RvrCk_isErr = 0
                                    RvrCk_msg = ""
                                else:
                                    Receiver = "N/A"
                                    RvrCk_isErr = 1
                                    RvrCk_msg = "Receiver Not Available"
                                stmp_dt = [
                                    invoId,
                                    "Receiver",
                                    Receiver,
                                    RvrCk_isErr,
                                    RvrCk_msg,
                                    stmp_created_on,
                                ]
                                stm_dt_lt.append(stmp_dt)

                                if "Department" in StampDataList[splt_map[fl]]:
                                    Department = StampDataList[splt_map[fl]][
                                        "Department"
                                    ]
                                    deptCk_isErr = 0
                                    deptCk_msg = ""
                                else:
                                    Department = "N/A"
                                    deptCk_isErr = 1
                                    deptCk_msg = "Department Not Found."
                                stmp_dt = [
                                    invoId,
                                    "Department",
                                    Department,
                                    deptCk_isErr,
                                    deptCk_msg,
                                    stmp_created_on,
                                ]
                                stm_dt_lt.append(stmp_dt)

                                if "Store Number" in StampDataList[splt_map[fl]]:
                                    storenumber = StampDataList[splt_map[fl]][
                                        "Store Number"
                                    ]
                                    try:
                                        try:
                                            storenumber = str(
                                                "".join(
                                                    filter(
                                                        str.isdigit, str(storenumber)
                                                    )
                                                )
                                            )
                                            # Fetch specific columns as a list
                                            # of dictionaries using .values()
                                            results = db.query(
                                                model.NonintegratedStores
                                            ).values(
                                                model.NonintegratedStores.store_number
                                            )
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
                                        except Exception as e:
                                            logger.info(
                                                f"Error fetching stores type: {str(e)}"
                                            )

                                        if int(storenumber) == int(str_nm):
                                            strCk_isErr = 0
                                            strCk_msg = ""

                                        else:
                                            strCk_isErr = 0
                                            strCk_msg = "Store Number Not Matching"

                                    except Exception as e:
                                        strCk_isErr = 1
                                        strCk_msg = "Error:" + str(e)
                                else:
                                    storenumber = "N/A"
                                    strCk_isErr = 1
                                    strCk_msg = ""

                                stmp_dt = [
                                    invoId,
                                    "StoreType",
                                    store_type,
                                    StrTyp_IsErr,
                                    StrTyp_msg,
                                    stmp_created_on,
                                ]
                                stm_dt_lt.append(stmp_dt)

                                stmp_dt = [
                                    invoId,
                                    "StoreNumber",
                                    storenumber,
                                    strCk_isErr,
                                    strCk_msg,
                                    stmp_created_on,
                                ]
                                stm_dt_lt.append(stmp_dt)
                                try:
                                    try:
                                        insert_query = """ INSERT INTO \
                                            stampdatavalidation \
                                                (documentid, stamptagname, stampvalue,\
                                                      is_error, errordesc, created_on)
                                                        VALUES %s
                                                        """
                                        extras.execute_values(
                                            cursor, insert_query, stm_dt_lt
                                        )

                                        conn.commit()

                                    except Exception as e:
                                        logger.error(
                                            f"stampdata insertion exception: {str(e)}"
                                        )
                                    cursor = conn.cursor()
                                    insert_query = """
                                    INSERT INTO \
                                        pfg_schema.stampdata \
                                            ("DOCUMENT_ID", "DEPTNAME", \
                                                "RECEIVING_DATE", \
                                                    "CONFIRMATION_NUMBER",\
                                                        "RECEIVER", "SELECTED_DEPT",\
                                                            "storenumber")
                                                VALUES (%s, %s, %s, %s, %s, %s, %s);
                                    """

                                    data_to_insert = (
                                        invoId,
                                        Department,
                                        ReceivingDate,
                                        Confirmation,
                                        Receiver,
                                        MarkedDept,
                                        storenumber,
                                    )
                                    cursor.execute(insert_query, data_to_insert)

                                    conn.commit()
                                    time.sleep(0.5)
                                    try:
                                        # print("in stampdata insertion")
                                        cur = conn.cursor()
                                        sql_updateDoc = """
                                            UPDATE pfg_schema.document
                                            SET "JournalNumber" = %s,
                                                "dept" = %s, "store" = %s
                                            WHERE "idDocument" = %s;
                                        """
                                        values = (
                                            Confirmation,
                                            Department,
                                            storenumber,
                                            invoId,
                                        )
                                        cur.execute(sql_updateDoc, values)
                                        conn.commit()
                                    except Exception as e:
                                        logger.error(
                                            f"stampdata insertion exception: {str(e)}"
                                        )
                                        # print('line 372',str(e))

                                except Exception as e:
                                    logger.error(
                                        f"stampdata insertion exception: {str(e)}"
                                    )
                                    # db.rollback()
                            try:
                                if store_type == "Integrated":
                                    IntegratedvoucherData(invoId, db)
                                elif store_type == "Non-Integrated":
                                    nonIntegratedVoucherData(invoId, db)
                            except Exception as er:
                                logger.info(f"VoucherDateException:{er}")

                        #
                        # except Exception as er:
                        #     print(str(er))
                        #     pass

                        # print("event_generator: ", event_generator)

                        try:
                            # cur = conn.cursor()
                            # sql_updateFR = """UPDATE pfg_schema.frtrigger_tab \
                            #           SET "status" = %s, "sender" = %s, \
                            #             "vendorID" = %s \
                            #         WHERE "blobpath" = %s; """
                            # FRvalues = ("Processed", sender, vendorID, spltFileName)
                            # cur.execute(sql_updateFR, FRvalues)
                            # conn.commit()
                            fr_trigger = db.query(model.frtrigger_tab).filter
                            (model.frtrigger_tab.blobpath == spltFileName)

                            # Step 2: Perform the update operation
                            fr_trigger.update(
                                {
                                    model.frtrigger_tab.status: "Processed",
                                    model.frtrigger_tab.vendorID: vendorID,
                                }
                            )

                            # Step 3: Commit the transaction
                            db.commit()

                        except Exception as qw:
                            logger.info(f"ocr.py line 475: {str(qw)}")

                        status = "success"

                else:
                    try:
                        preBltFrdata, preBltFrdata_status = getFrData_MNF(rwOcrData)
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

                    except Exception as e:
                        logger.info(
                            f"getFrData_MNF Exception line 446 orc.py: {str(e)}"
                        )
                        status = "fail"
                    logger.info("vendor not found!!")
                    try:
                        # cur = conn.cursor()
                        # sql_updateFR_1 = """
                        #     UPDATE pfg_schema.frtrigger_tab
                        #     SET "status" = %(status)s, sender = %(sender)s
                        #     WHERE "blobpath" = %(blobpath)s;
                        # """
                        # FRvalues_1 = {
                        #     "status": "VendorNotFound",
                        #     "sender": sender,
                        #     "blobpath": spltFileName,
                        # }

                        # cur.execute(sql_updateFR_1, FRvalues_1)
                        # conn.commit()
                        db.query(model.frtrigger_tab).filter(
                            model.frtrigger_tab.blobpath == spltFileName
                        ).update(
                            {
                                model.frtrigger_tab.status: "VendorNotFound",
                            }
                        )

                        # Commit the transaction
                        db.commit()
                    except Exception as et:
                        try:
                            # cur = conn.cursor()
                            # sql_updateFR_2 = """
                            #     UPDATE pfg_schema.frtrigger_tab
                            #     SET "status" = %(status)s, "sender" = %(sender)s
                            #     WHERE "blobpath" = %(blobpath)s;
                            # """
                            # FRvalues_2 = {
                            #     "status": str(et),
                            #     "sender": sender,
                            #     "blobpath": spltFileName,
                            # }

                            # cur.execute(sql_updateFR_2, FRvalues_2)
                            # conn.commit()
                            db.query(model.frtrigger_tab).filter(
                                model.frtrigger_tab.blobpath == spltFileName
                            ).update(
                                {
                                    model.frtrigger_tab.status: str(et),
                                }
                            )

                            # Commit the transaction
                            db.commit()
                        except Exception as e:
                            print("frtrigger_tab update exception: ", str(e))

                        logger.error(f"frtrigger_tab update exception: {str(et)}")

                    status = "fail"
                fl = fl + 1
                # time.sleep(0.5)

            cursor.close()
            conn.close()
        else:

            logger.error(f"DI responed error: {fr_model_status, fr_model_msg}")
            # log to DB
        try:
            if len(str(invoId)) == 0:
                preBltFrdata, preBltFrdata_status = getFrData_MNF(rwOcrData)
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
                    conn = psycopg2.connect(
                        dbname=settings.db_name,
                        user=settings.db_user,
                        password=settings.db_password,
                        host=settings.db_host,
                        port=str(settings.db_port),  # Ensure port is a string
                    )

                    cur = conn.cursor()
                    sql_updateFR = """UPDATE pfg_schema.frtrigger_tab \
                                SET "status" = %s, "sender" = %s, \
                                "vendorID" = %s \
                            WHERE "blobpath" = %s; """
                    FRvalues = ("PostProcessing Error", sender, vendorID, spltFileName)
                    cur.execute(sql_updateFR, FRvalues)
                    conn.commit()
                    cursor.close()
                    conn.close()
                except Exception as qw:
                    logger.info(f"ocr.py line 475: {str(qw)}")
        except Exception as err:
            logger.error(f"line 947 ocr.py: {err}")
            logger.error(f" ocr.py: {traceback.format_exc()}")

    except Exception as err:
        import traceback

        logger.error(f"API exception ocr.py: {traceback.format_exc()}")
        logger.error(f"API exception ocr.py: {str(err)}")
        status = "error: " + str(err)

    try:
        pfg_sync(invoId, userID, db)
        logger.info("pfg_sync Done!")
    except Exception as Er:
        logger.info(f"Ocr.py SyncError: {Er}")

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
    modelDetails = []
    modelData = (
        db.query(model.DocumentModel)
        .filter(model.DocumentModel.idVendorAccount == vendorAccountID)
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
    endpoint = generatorObj["endpoint"]
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
    # pdf_data_bytes = generatorObj["pdf_stream"]  # TODO: Unused variable
    fr_data = {}
    spltFileName = generatorObj["spltFileName"]
    vendorAccountID = generatorObj["vendorAccountID"]
    UploadDocType = generatorObj["UploadDocType"]

    metaVendorAdd = generatorObj["metaVendorAdd"]
    metaVendorName = generatorObj["metaVendorName"]
    # OpenAI_client = generatorObj["OpenAI_client"]

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
                logger.info(f"ocr.py, line 571: InvoiceDocumentID: {invoice_ID}")
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
                except Exception as ep:
                    logger.error(f"ocr.py line 594: exception:{str(ep)}")
                    # {"DB error": "Error while inserting data"}

                db.close()
                # live_model_status = 1  # TODO: Unused variable
                # live_model_msg = "Data extracted"  # TODO: Unused variable
                current_status = {"percentage": 75, "status": "Post-Processing ⌛"}
                # print("current_status: line 466: ",current_status)
                logger.info(f"current_status: line 466: {current_status}")

                current_status = {"percentage": 100, "status": "OCR completed ✔"}

                # print("current_status: line 479: ", current_status)

                logger.info(f"current_status: line 479:{current_status}")
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
        db.rollback()
        if "Incorrect datetime value" in str(e):
            invoice_data["documentDate"] = None
        try:

            db_data = model.Document(**invoice_data)
            db.add(db_data)
            db.commit()
        except Exception as e:
            db.rollback()
            if "for column 'docheaderID'" in str(e):
                invoice_data["docheaderID"] = ""
            try:
                db_data = model.Document(**invoice_data)
                db.add(db_data)
                db.commit()
            except Exception as e:
                db.rollback()
                if "for column 'PODocumentID'" in str(e):
                    invoice_data["PODocumentID"] = ""
                try:

                    db_data = model.Document(**invoice_data)
                    db.add(db_data)
                    db.commit()
                except Exception as e:
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
            if label["tag"] == "PurchaseOrder":
                db_data["Value"] = poNumber
            else:
                db_data["Value"] = label["data"]["value"]
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

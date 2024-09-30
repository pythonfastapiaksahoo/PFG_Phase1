import json
import traceback
from typing import Union

from sqlalchemy import or_
from sqlalchemy.orm import Session

import pfg_app.model as model
from pfg_app.crud.ERPIntegrationCrud import processInvoiceVoucher
from pfg_app.logger_module import logger


# db = SCHEMA
def IntegratedvoucherData(inv_id, db: Session):
    # inv_id = 418
    invoice_type = (
        db.query(model.Document.UploadDocType)
        .filter(model.Document.idDocument == inv_id)
        .scalar()
    )
    stmp_dt = (
        db.query(model.StampDataValidation)
        .filter(model.StampDataValidation.documentid == inv_id)
        .all()
    )
    stmp_dt_dict = {}
    for dtm_rw in stmp_dt:
        stmp_dt_dict[dtm_rw.stamptagname] = dtm_rw.stampvalue
    confNumber = stmp_dt_dict["ConfirmationNumber"]
    invo_recp = (
        db.query(model.PFGReceipt)
        .filter(model.PFGReceipt.RECEIVER_ID == confNumber)
        .all()
    )

    for invRpt in invo_recp:
        BUSINESS_UNIT = invRpt.BUSINESS_UNIT
        VENDOR_SETID = invRpt.VENDOR_SETID
        VENDOR_ID = invRpt.VENDOR_ID
        ACCOUNT = invRpt.ACCOUNT
        DEPTID = invRpt.DEPTID

    modelid = (
        db.query(model.Document.documentModelID)
        .filter(model.Document.idDocument == inv_id)
        .scalar()
    )
    docPath = (
        db.query(model.Document.docPath)
        .filter(model.Document.idDocument == inv_id)
        .scalar()
    )
    invo_total_Tg = (
        db.query(model.DocumentTagDef.idDocumentTagDef)
        .filter(
            model.DocumentTagDef.TagLabel == "InvoiceTotal",
            model.DocumentTagDef.idDocumentModel == modelid,
        )
        .scalar()
    )
    invo_Date_Tg = (
        db.query(model.DocumentTagDef.idDocumentTagDef)
        .filter(
            model.DocumentTagDef.TagLabel == "InvoiceDate",
            model.DocumentTagDef.idDocumentModel == modelid,
        )
        .scalar()
    )
    invo_ID_Tg = (
        db.query(model.DocumentTagDef.idDocumentTagDef)
        .filter(
            model.DocumentTagDef.TagLabel == "InvoiceId",
            model.DocumentTagDef.idDocumentModel == modelid,
        )
        .scalar()
    )
    invo_SubTotal_Tg = (
        db.query(model.DocumentTagDef.idDocumentTagDef)
        .filter(
            model.DocumentTagDef.TagLabel == "SubTotal",
            model.DocumentTagDef.idDocumentModel == modelid,
        )
        .scalar()
    )
    Invo_header = (
        db.query(model.DocumentData)
        .filter(
            model.DocumentData.documentID == inv_id,
            model.DocumentData.documentTagDefID.in_(
                [invo_total_Tg, invo_Date_Tg, invo_ID_Tg, invo_SubTotal_Tg]
            ),
        )
        .all()
    )

    invo_hrd_data = {}
    for i in Invo_header:
        invo_hrd_data[i.documentTagDefID] = i.Value
    invo_total = invo_hrd_data[invo_total_Tg]
    invo_SubTotal = invo_hrd_data[invo_SubTotal_Tg]
    invo_Date = invo_hrd_data[invo_Date_Tg]
    invo_ID = invo_hrd_data[invo_ID_Tg]

    existing_record = db.query(model.VoucherData).filter_by(documentID=inv_id).first()

    if existing_record:
        # If record exists, update the existing record with new data
        existing_record.Business_unit = BUSINESS_UNIT
        existing_record.Invoice_Id = invo_ID
        existing_record.Invoice_Dt = invo_Date
        existing_record.Vendor_Setid = VENDOR_SETID
        existing_record.Vendor_ID = VENDOR_ID
        existing_record.Deptid = DEPTID
        existing_record.Account = ACCOUNT
        existing_record.Gross_Amt = invo_SubTotal
        existing_record.Merchandise_Amt = invo_total
        existing_record.File_Name = docPath.split("/")[-1]
        existing_record.Distrib_Line_num = 1
        existing_record.Voucher_Line_num = 1
        existing_record.Image_Nbr = 1
        existing_record.Origin = invoice_type
    else:
        # If no record exists, create a new one
        VoucherData_insert_data = {
            "documentID": inv_id,
            "Business_unit": BUSINESS_UNIT,
            "Invoice_Id": invo_ID,
            "Invoice_Dt": invo_Date,
            "Vendor_Setid": VENDOR_SETID,
            "Vendor_ID": VENDOR_ID,
            "Deptid": DEPTID,
            "Account": ACCOUNT,
            "Gross_Amt": invo_SubTotal,
            "Merchandise_Amt": invo_total,
            "File_Name": docPath.split("/")[-1],
            "Distrib_Line_num": 1,
            "Voucher_Line_num": 1,
            "Image_Nbr": 1,
            "Origin": invoice_type,
        }
        VD_db_data = model.VoucherData(**VoucherData_insert_data)
        db.add(VD_db_data)

    # Commit the changes to the database
    db.commit()

    # VoucherData_insert_data = {'documentID':inv_id,'Business_unit':BUSINESS_UNIT,'Invoice_Id':invo_ID,'Invoice_Dt':invo_Date,'Vendor_Setid':VENDOR_SETID,'Vendor_ID':VENDOR_ID,'Deptid':DEPTID,'Account':ACCOUNT,'Gross_Amt':invo_SubTotal,'Merchandise_Amt':invo_total,'File_Name':docPath.split('/')[-1],'Distrib_Line_num':1,'Voucher_Line_num':1,'Image_Nbr':1,'Origin':invoice_type}
    # VD_db_data = model.VoucherData(**VoucherData_insert_data)
    # db.add(VD_db_data)
    # db.commit()


def nonIntegratedVoucherData(inv_id, db: Session):

    invoice_type = (
        db.query(model.Document.UploadDocType)
        .filter(model.Document.idDocument == inv_id)
        .scalar()
    )
    modelid = (
        db.query(model.Document.documentModelID)
        .filter(model.Document.idDocument == inv_id)
        .scalar()
    )
    docPath = (
        db.query(model.Document.docPath)
        .filter(model.Document.idDocument == inv_id)
        .scalar()
    )
    invo_total_Tg = (
        db.query(model.DocumentTagDef.idDocumentTagDef)
        .filter(
            model.DocumentTagDef.TagLabel == "InvoiceTotal",
            model.DocumentTagDef.idDocumentModel == modelid,
        )
        .scalar()
    )
    invo_Date_Tg = (
        db.query(model.DocumentTagDef.idDocumentTagDef)
        .filter(
            model.DocumentTagDef.TagLabel == "InvoiceDate",
            model.DocumentTagDef.idDocumentModel == modelid,
        )
        .scalar()
    )
    invo_ID_Tg = (
        db.query(model.DocumentTagDef.idDocumentTagDef)
        .filter(
            model.DocumentTagDef.TagLabel == "InvoiceId",
            model.DocumentTagDef.idDocumentModel == modelid,
        )
        .scalar()
    )
    invo_SubTotal_Tg = (
        db.query(model.DocumentTagDef.idDocumentTagDef)
        .filter(
            model.DocumentTagDef.TagLabel == "SubTotal",
            model.DocumentTagDef.idDocumentModel == modelid,
        )
        .scalar()
    )
    Invo_header = (
        db.query(model.DocumentData)
        .filter(
            model.DocumentData.documentID == inv_id,
            model.DocumentData.documentTagDefID.in_(
                [invo_total_Tg, invo_Date_Tg, invo_ID_Tg, invo_SubTotal_Tg]
            ),
        )
        .all()
    )

    result = (
        db.query(model.Document)
        .join(
            model.VendorAccount,
            model.Document.vendorAccountID == model.VendorAccount.idVendorAccount,
        )
        .join(model.Vendor, model.VendorAccount.vendorID == model.Vendor.idVendor)
        .filter(model.Document.idDocument == inv_id)
        .with_entities(model.Vendor.VendorCode)
        .first()
    )

    if result:
        VENDOR_ID = result[0]
    else:
        VENDOR_ID = ""

    invo_hrd_data = {}
    for i in Invo_header:
        invo_hrd_data[i.documentTagDefID] = i.Value
    invo_total = invo_hrd_data[invo_total_Tg]
    invo_SubTotal = invo_hrd_data[invo_SubTotal_Tg]
    invo_Date = invo_hrd_data[invo_Date_Tg]
    invo_ID = invo_hrd_data[invo_ID_Tg]

    InvStmDt = (
        db.query(model.StampDataValidation)
        .filter(model.StampDataValidation.documentid == inv_id)
        .all()
    )
    stmpData = {}
    for stDt in InvStmDt:
        stmpData[stDt.stamptagname] = stDt.stampvalue

    if stmpData["Department"].isdigit():
        dpt_cd = []
        if len(stmpData["Department"]) == 2:
            dpt_cd.append(stmpData["Department"] + "00")
            dpt_cd.append("00" + stmpData["Department"])
        else:
            dpt_cd.append(stmpData["Department"])

        #     dpt_cd_dt = db.query(model.PFGDepartment).filter(or_(
        #             model.PFGDepartment.DEPTID.in_(dpt_cd),  # Contains '30' anywhere
        #             model.PFGDepartment.DEPTID ==stmpData['Department']     # Exact match with '30'
        #         )
        #     ).all()

        dpt_cd_dt = (
            db.query(model.PFGDepartment)
            .filter(
                or_(
                    model.PFGDepartment.DEPTID.in_(
                        dpt_cd
                    ),  # DEPTID matches any value in dpt_cd
                    model.PFGDepartment.DEPTID == stmpData["Department"],  # Exact match
                )
            )
            .all()
        )

        # Iterate and print matching results
        for department in dpt_cd_dt:
            DEPTID = department.DEPTID
            VENDOR_SETID = department.SETID
            BUSINESS_UNIT = "OFGDS"
            ACCOUNT = "71999"

    else:

        dpt_cd_dt = (
            db.query(model.PFGDepartment)
            .filter(
                or_(
                    model.PFGDepartment.DESCR.in_([stmpData["Department"]]),
                    model.PFGDepartment.DESCRSHORT == [stmpData["Department"]],
                    model.PFGDepartment.DESCRSHORT.like(stmpData["Department"]),
                )
            )
            .all()
        )
        for department in dpt_cd_dt:
            DEPTID = department.DEPTID
            VENDOR_SETID = department.SETID
            BUSINESS_UNIT = "OFGDS"
            ACCOUNT = "71999"

    existing_record = db.query(model.VoucherData).filter_by(documentID=inv_id).first()

    if existing_record:
        # If record exists, update the existing record with new data
        existing_record.Business_unit = BUSINESS_UNIT
        existing_record.Invoice_Id = invo_ID
        existing_record.Invoice_Dt = invo_Date
        existing_record.Vendor_Setid = VENDOR_SETID
        existing_record.Vendor_ID = VENDOR_ID
        existing_record.Deptid = DEPTID
        existing_record.Account = ACCOUNT
        existing_record.Gross_Amt = invo_SubTotal
        existing_record.Merchandise_Amt = invo_total
        existing_record.File_Name = docPath.split("/")[-1]
        existing_record.Distrib_Line_num = 1
        existing_record.Voucher_Line_num = 1
        existing_record.Image_Nbr = 1
        existing_record.Origin = invoice_type
    else:
        # If no record exists, create a new one
        VoucherData_insert_data = {
            "documentID": inv_id,
            "Business_unit": BUSINESS_UNIT,
            "Invoice_Id": invo_ID,
            "Invoice_Dt": invo_Date,
            "Vendor_Setid": VENDOR_SETID,
            "Vendor_ID": VENDOR_ID,
            "Deptid": DEPTID,
            "Account": ACCOUNT,
            "Gross_Amt": invo_SubTotal,
            "Merchandise_Amt": invo_total,
            "File_Name": docPath.split("/")[-1],
            "Distrib_Line_num": 1,
            "Voucher_Line_num": 1,
            "Image_Nbr": 1,
            "Origin": invoice_type,
        }
        VD_db_data = model.VoucherData(**VoucherData_insert_data)
        db.add(VD_db_data)

    # Commit the changes to the database
    db.commit()

    # VoucherData_insert_data = {'documentID':inv_id,'Business_unit':BUSINESS_UNIT,'Invoice_Id':invo_ID,'Invoice_Dt':invo_Date,'Vendor_Setid':VENDOR_SETID,'Vendor_ID':VENDOR_ID,'Deptid':DEPTID,'Account':ACCOUNT,'Gross_Amt':invo_SubTotal,'Merchandise_Amt':invo_total,'File_Name':docPath.split('/')[-1],'Distrib_Line_num':1,'Voucher_Line_num':1,'Image_Nbr':1,'Origin':invoice_type}
    # VD_db_data = model.VoucherData(**VoucherData_insert_data)
    # db.add(VD_db_data)
    # db.commit()


# ocr check:  check1


def pfg_sync(docID, db: Session):
    logger.info("start on the pfg_sync func()")
    docModel = (
        db.query(model.Document.documentModelID)
        .filter(model.Document.idDocument == docID)
        .scalar()
    )
    invTotalMth = 0
    dateCheck = 0
    docStatusSync: dict[str, dict[str, Union[int, list[str]]]] = {}
    ocrCheck = 0
    totalCheck = 0
    ocrCheck_msg = []
    totalCheck_msg = []
    invTotalMth_msg = ""
    dateCheck_msg = ""
    overAllstatus = 0
    try:

        DocDtHdr = (
            db.query(model.DocumentData, model.DocumentTagDef)
            .join(
                model.DocumentTagDef,
                model.DocumentData.documentTagDefID
                == model.DocumentTagDef.idDocumentTagDef,
            )
            .filter(model.DocumentTagDef.idDocumentModel == docModel)
            .filter(model.DocumentData.documentID == docID)
            .all()
        )

        docHdrDt = {}
        try:
            for document_data, document_tag_def in DocDtHdr:
                docHdrDt[document_tag_def.TagLabel] = document_data.Value
            if docHdrDt["SubTotal"] == docHdrDt["InvoiceTotal"]:
                invTotalMth = 1
            else:
                invTotalMth = 0
                invTotalMth_msg = "Invoice total mismatch, please review."
        except:
            invTotalMth = 0
            invTotalMth_msg = "Invoice total mismatch, please review."

        try:
            # date_string = docHdrDt["InvoiceDate"]  # TODO: Unused variable
            try:
                dateCheck = 1
            except:
                dateCheck = 0
                dateCheck_msg = "Invoice date is invalid,Please review."
        except:
            dateCheck = 0
            dateCheck_msg = "Failed to validate the invoice date,Please review."

        if dateCheck == 1:
            ocrCheck = 1
            ocrCheck_msg.append("Success")
        else:
            ocrCheck = 0
            ocrCheck_msg.append(dateCheck_msg)

        if invTotalMth == 1:
            totalCheck = 1
            totalCheck_msg.append("Success")

        else:
            totalCheck_msg.append(invTotalMth_msg)
            totalCheck = 0

        docStatusSync["OCR Validations"] = {
            "status": ocrCheck,
            "response": ocrCheck_msg,
        }

        docStatusSync["Invoice Total Validation"] = {
            "status": totalCheck,
            "response": totalCheck_msg,
        }

        # stampdata check: check2
        # mandatory stamp fields(until integrated or non integrated)

        InvStmDt = (
            db.query(model.StampDataValidation)
            .filter(model.StampDataValidation.documentid == docID)
            .all()
        )
        stmpData = {}
        for stDt in InvStmDt:
            stmpData[stDt.stamptagname] = {stDt.stampvalue: stDt.is_error}
        strCk_msg = []
        strCk = 0
        if "StoreType" in stmpData:
            try:
                if list(stmpData["StoreType"].keys())[0] in [
                    "Integrated",
                    "Non-Integrated",
                ]:
                    strCk = 1
                    strCk_msg.append("Success")
                else:
                    strCk = 0
                    strCk_msg.append("Invalid Store Type")
            except:
                strCk = 0
                strCk_msg.append("Invalid Store Type")

        docStatusSync["StoreType Validation"] = {"status": strCk, "response": strCk_msg}
        try:
            if list(stmpData["StoreType"].keys())[0] == "Integrated":
                strCk = 1
                strCk_msg.append("Success")

                IntegratedvoucherData(docID, db)
            if list(stmpData["StoreType"].keys())[0] == "Non-Integrated":
                nonIntegratedVoucherData(docID, db)
                strCk = 1
                strCk_msg.append("Success")

        except Exception as er:
            logger.info(f"VoucherCreationException:{er} ")

        voucher_query = db.query(model.VoucherData).filter(
            model.VoucherData.documentID == docID
        )
        row_count = voucher_query.count()
        NullVal = []
        VthChk = 0
        VthChk_msg = ""
        if row_count > 1:
            VthChk = 0
            VthChk_msg = "Multiple entries found"

        elif row_count == 1:
            # Fetch the single row
            voucher_row = voucher_query.first()
            has_null_or_empty = False
            for column in model.VoucherData.__table__.columns:
                value = getattr(voucher_row, column.name)
                if value is None or value == "":
                    has_null_or_empty = True
                    NullVal.append(column.name)

            if has_null_or_empty:
                VthChk = 0
                VthChk_msg = "Missing values:" + str(NullVal)[1:-1]
            else:
                VthChk = 1
                VthChk_msg = "Success"
        else:
            VthChk = 0
            VthChk_msg = "No Voucher data Found."
        docStatusSync["VoucherCreation Data Validation"] = {
            "status": VthChk,
            "response": [VthChk_msg],
        }
        logger.info(f"docStatusSync:{docStatusSync}")

        overAllstatus_ck = 1
        if isinstance(docStatusSync, dict):
            for stCk in docStatusSync:
                if "status" in docStatusSync[stCk]:
                    overAllstatus_ck = overAllstatus_ck * int(
                        docStatusSync[stCk]["status"]  # type: ignore
                    )

        if overAllstatus_ck == 1:
            db.query(model.Document).filter(model.Document.idDocument == docID).update(
                {model.Document.documentStatusID: 2}
            )
            db.commit()
            overAllstatus = 1

            # send to ppl soft:
            try:
                resp = processInvoiceVoucher(docID, db)
                try:
                    if "data" in resp:
                        if "Http Response" in resp["data"]:
                            RespCode = resp["data"]["Http Response"]
                            if resp["data"]["Http Response"].isdigit():
                                RespCodeInt = int(RespCode)
                                if RespCodeInt == 201:
                                    docStatus = 7
                                    docSubStatus = 43

                                elif RespCodeInt == 400:
                                    # Failure: Data Error - IICS could not process the message
                                    docStatus = 21
                                    docSubStatus = 108

                                elif RespCodeInt == 406:
                                    # Failure: Data Error - Invoice could not be staged
                                    docStatus = 21
                                    docSubStatus = 109

                                elif RespCodeInt == 422:
                                    # Failure: PeopleSoft could not parse the json message
                                    docStatus = 21
                                    docSubStatus = 110

                                elif RespCodeInt == 424:
                                    # Failure: File Attachment could not loaded to File Server
                                    docStatus = 21
                                    docSubStatus = 111

                                elif RespCodeInt == 500:
                                    # Internal Server Error - Could not connect to IICS or to PeopleSoft
                                    docStatus = 21
                                    docSubStatus = 53
                                else:
                                    docStatus = 21
                                    docSubStatus = 112
                            else:
                                docStatus = 21
                                docSubStatus = 112
                        else:
                            docStatus = 21
                            docSubStatus = 112
                    else:
                        docStatus = 21
                        docSubStatus = 112
                except Exception as err:
                    logger.info(f"PopleSoftResponseError: {err}")
                    docStatus = 21
                    docSubStatus = 112

                try:
                    db.query(model.Document).filter(
                        model.Document.idDocument == docID
                    ).update(
                        {
                            model.Document.documentStatusID: docStatus,
                            model.Document.documentsubstatusID: docSubStatus,
                        }
                    )
                    db.commit()
                except Exception as err:
                    logger.info(f"ErrorUpdatingPostingData: {err}")
            except Exception as e:
                print("Error in ProcessInvoiceVoucher fun(): ", e)
                print("Error in ProcessInvoiceVoucher fun(): ", traceback.format_exc())
    except Exception as err:
        logger.info(f"SyncException:{err}")
        logger.info(f"{traceback.format_exc()}")
        docStatusSync = {}
        overAllstatus = 0
    docStatusSync["overAllstatus"] = overAllstatus  # type: ignore
    try:
        json_data = json.dumps(docStatusSync)
        db.query(model.Document).filter(model.Document.idDocument == docID).update(
            {model.Document.documentDescription: json_data}
        )

        db.commit()
    except Exception as Err:
        logger.info(f"updateDocDecError: {Err}")
    logger.info(f"overallstatus: {docStatusSync}")
    return docStatusSync

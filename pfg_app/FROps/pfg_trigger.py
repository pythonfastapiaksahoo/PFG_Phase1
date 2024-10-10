import json
import traceback
from typing import Union

from sqlalchemy import or_
from sqlalchemy.orm import Session

import pfg_app.model as model
from pfg_app.crud.ERPIntegrationCrud import processInvoiceVoucher
from pfg_app.crud.InvoiceCrud import update_docHistory
from pfg_app.logger_module import logger
from pfg_app.schemas.pfgtriggerSchema import InvoiceVoucherSchema


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


def pfg_sync(docID, userID, db: Session):
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
    overAllstatus_msg = ""
    dsdApprovalCheck = 0
    dsdApprovalCheck_msg = 5000
    duplicate_status_ck = 0
    duplicate_status_ck_msg = ""
    InvodocStatus = ""

    try:

        docTb = (
            db.query(model.Document).filter(model.Document.idDocument == docID).all()
        )

        for dtb_rw in docTb:
            InvodocStatus = dtb_rw.documentStatusID
    except Exception as e:
        logger.error(f"Exception in pfg_sync line 294: {str(e)}")

    if InvodocStatus == 10:
        duplicate_status_ck = 0
        duplicate_status_ck_msg = "Invoice already exists"
        docStatusSync["Invoice Duplicate Check"] = {
            "status": duplicate_status_ck,
            "response": [duplicate_status_ck_msg],
        }
    elif InvodocStatus == "":
        duplicate_status_ck = 0
        duplicate_status_ck_msg = "InvoiceId not found"
        docStatusSync["Invoice Duplicate Check"] = {
            "status": duplicate_status_ck,
            "response": [duplicate_status_ck_msg],
        }
    elif isinstance(InvodocStatus, int) and InvodocStatus != 10:
        duplicate_status_ck = 1
        duplicate_status_ck_msg = "Success"
        docStatusSync["Invoice Duplicate Check"] = {
            "status": duplicate_status_ck,
            "response": [duplicate_status_ck_msg],
        }

    if duplicate_status_ck == 1:
        try:
            update_docHistory(docID, userID, InvodocStatus, duplicate_status_ck_msg, db)
        except Exception as e:
            logger.error(f"pfg_sync line 401: {str(e)}")
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
                logger.info(f"docHdrDt: {docHdrDt}")

                # Invoice Total Approval Check
                try:
                    if float(docHdrDt["InvoiceTotal"]) < dsdApprovalCheck_msg:
                        dsdApprovalCheck = 1
                        dmsg = "Success"
                    elif float(docHdrDt["InvoiceTotal"]) > dsdApprovalCheck_msg:
                        try:
                            docStatus = 6
                            docSubStatus = 113
                            dmsg = f"Invoice Amount:{float(docHdrDt['InvoiceTotal'])}, \
                                Approval Needed."
                            try:
                                update_docHistory(docID, userID, docStatus, dmsg, db)
                            except Exception as e:
                                logger.error(f"pfg_sync line 534: {str(e)}")

                            try:
                                db.query(model.Document).filter(
                                    model.Document.idDocument == docID
                                ).update(
                                    {
                                        model.Document.documentStatusID: docStatus,
                                        model.Document.documentsubstatusID: docSubStatus,  # noqa: E501
                                    }
                                )
                                db.commit()
                            except Exception as err:
                                logger.info(f"ErrorUpdatingPostingData: {err}")

                        except Exception as e:
                            logger.error(f"pfg_sync amount validations: {str(e)}")
                            dmsg = "Invoice Amount Invalid" + str(e)
                    else:
                        dsdApprovalCheck = 0
                        dmsg = "Invoice Amount Invalid"

                except Exception as e:
                    logger.error(f"pfg_sync amount validations: {str(e)}")

                docStatusSync["Amount Approval Validation"] = {
                    "status": dsdApprovalCheck,
                    "response": [dmsg],
                }
                # Invoice Total check

                invTotalMth = 0
                invTotalMth_msg = "Invoice total mismatch, please review."
                if dsdApprovalCheck == 1:
                    try:
                        if docHdrDt["InvoiceTotal"] == docHdrDt["SubTotal"]:
                            invTotalMth = 1

                        elif (invTotalMth == 0) and (
                            docHdrDt["InvoiceTotal"] != docHdrDt["SubTotal"]
                        ):
                            if float(docHdrDt["InvoiceTotal"]) == float(
                                docHdrDt["SubTotal"]
                            ):
                                invTotalMth = 1
                            if (invTotalMth == 0) and ("TotalTax" in docHdrDt):
                                if (
                                    float(docHdrDt["SubTotal"])
                                    + float(docHdrDt["TotalTax"])
                                ) == float(docHdrDt["InvoiceTotal"]):
                                    invTotalMth = 1
                                if (invTotalMth == 0) and ("PST" in docHdrDt):
                                    if float(docHdrDt["SubTotal"]) + float(
                                        docHdrDt["PST"]
                                    ):
                                        invTotalMth = 1
                                if (invTotalMth == 0) and ("GST" in docHdrDt):
                                    if float(docHdrDt["SubTotal"]) + float(
                                        docHdrDt["GST"]
                                    ):
                                        invTotalMth = 1
                    except Exception as e:
                        logger.error(f"Exception in pfg_sync line 387: {str(e)}")
                        invTotalMth = 0
                        invTotalMth_msg = "Invoice total mismatch:" + str(e)
            except Exception as e:
                logger.error(traceback.format_exc())
                invTotalMth = 0
                invTotalMth_msg = "Invoice total mismatch:" + str(e)

            try:
                # date_string = docHdrDt["InvoiceDate"]  # TODO: Unused variable
                try:
                    dateCheck = 1
                except Exception:
                    logger.error(traceback.format_exc())
                    dateCheck = 0
                    dateCheck_msg = "Invoice date is invalid,Please review."
            except Exception:
                logger.error(traceback.format_exc())
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

            if (
                docStatusSync["OCR Validations"]["status"] == 1
                and docStatusSync["Invoice Total Validation"]["status"] == 1
            ):

                # -----------------------update document history table
                documentstatus = 4
                documentdesc = "OCR Validations Success"
                try:
                    update_docHistory(docID, userID, documentstatus, documentdesc, db)
                except Exception as e:
                    logger.error(f"pfg_sync line 314: {str(e)}")

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
                    except Exception as e:
                        logger.error(f"Exception in pfg_sync-Store Type: {str(e)}")
                        strCk = 0
                        strCk_msg.append("Invalid Store Type")
                else:
                    strCk = 0
                    strCk_msg.append(" Store Type Not Found")

                docStatusSync["StoreType Validation"] = {
                    "status": strCk,
                    "response": strCk_msg,
                }

                if docStatusSync["StoreType Validation"]["status"] == 1:

                    documentstatus = 4
                    documentdesc = "StoreType Validation Success"
                    try:
                        update_docHistory(
                            docID, userID, documentstatus, documentdesc, db
                        )
                    except Exception as e:
                        logger.error(f"pfg_sync line 314: {str(e)}")

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

                    if docStatusSync["VoucherCreation Data Validation"]["status"] == 1:
                        documentstatus = 4
                        documentdesc = "VoucherCreation Data Validation Success"
                        try:
                            update_docHistory(
                                docID, userID, documentstatus, documentdesc, db
                            )
                        except Exception as e:
                            logger.error(f"pfg_sync line 314: {str(e)}")

                        overAllstatus_ck = 1
                        for stCk in docStatusSync:
                            valCkStatus = docStatusSync[stCk]["status"]
                            if type(valCkStatus) is int:
                                overAllstatus_ck = overAllstatus_ck * valCkStatus

                            else:
                                overAllstatus_ck = 0

                        if overAllstatus_ck == 1:
                            overAllstatus_msg = "Success"
                            db.query(model.Document).filter(
                                model.Document.idDocument == docID
                            ).update({model.Document.documentStatusID: 2})
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
                                                    dmsg = (
                                                        InvoiceVoucherSchema.SUCCESS_STAGED  # noqa: E501
                                                    )
                                                    docStatus = 7
                                                    docSubStatus = 43

                                                elif RespCodeInt == 400:
                                                    dmsg = (
                                                        InvoiceVoucherSchema.FAILURE_IICS  # noqa: E501
                                                    )
                                                    docStatus = 21
                                                    docSubStatus = 108

                                                elif RespCodeInt == 406:
                                                    dmsg = (
                                                        InvoiceVoucherSchema.FAILURE_INVOICE  # noqa: E501
                                                    )
                                                    docStatus = 21
                                                    docSubStatus = 109

                                                elif RespCodeInt == 422:
                                                    dmsg = (
                                                        InvoiceVoucherSchema.FAILURE_PEOPLESOFT  # noqa: E501
                                                    )
                                                    docStatus = 21
                                                    docSubStatus = 110

                                                elif RespCodeInt == 424:
                                                    dmsg = (
                                                        InvoiceVoucherSchema.FAILURE_FILE_ATTACHMENT  # noqa: E501
                                                    )
                                                    docStatus = 21
                                                    docSubStatus = 111

                                                elif RespCodeInt == 500:
                                                    dmsg = (
                                                        InvoiceVoucherSchema.INTERNAL_SERVER_ERROR  # noqa: E501
                                                    )
                                                    docStatus = 21
                                                    docSubStatus = 53
                                                else:
                                                    dmsg = (
                                                        InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                                                    )
                                                    docStatus = 21
                                                    docSubStatus = 112
                                            else:
                                                dmsg = (
                                                    InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                                                )
                                                docStatus = 21
                                                docSubStatus = 112
                                        else:
                                            dmsg = (
                                                InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                                            )
                                            docStatus = 21
                                            docSubStatus = 112
                                    else:
                                        dmsg = (
                                            InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                                        )
                                        docStatus = 21
                                        docSubStatus = 112
                                except Exception as err:
                                    logger.info(f"PopleSoftResponseError: {err}")
                                    dmsg = InvoiceVoucherSchema.FAILURE_COMMON.format_message(  # noqa: E501
                                        err
                                    )
                                    docStatus = 21
                                    docSubStatus = 112

                                try:
                                    db.query(model.Document).filter(
                                        model.Document.idDocument == docID
                                    ).update(
                                        {
                                            model.Document.documentStatusID: docStatus,
                                            model.Document.documentsubstausID: docSubStatus,  # noqa: E501
                                        }
                                    )
                                    db.commit()
                                except Exception as err:
                                    logger.info(f"ErrorUpdatingPostingData: {err}")
                                try:

                                    update_docHistory(
                                        docID, userID, docStatus, dmsg, db
                                    )
                                except Exception as e:
                                    logger.error(f"pfg_sync 501: {str(e)}")
                            except Exception as e:
                                print(
                                    "Error in ProcessInvoiceVoucher fun(): ",
                                    traceback.format_exc(),
                                )
                                logger.info(f"PopleSoftResponseError: {e}")
                                dmsg = (
                                    InvoiceVoucherSchema.FAILURE_COMMON.format_message(
                                        e
                                    )
                                )
                                docStatus = 21
                                docSubStatus = 112

                                try:
                                    db.query(model.Document).filter(
                                        model.Document.idDocument == docID
                                    ).update(
                                        {
                                            model.Document.documentStatusID: docStatus,
                                            model.Document.documentsubstatusID: docSubStatus,  # noqa: E501
                                        }
                                    )
                                    db.commit()
                                except Exception as err:
                                    logger.info(f"ErrorUpdatingPostingData: {err}")
                                try:
                                    documentstatus = 21
                                    update_docHistory(
                                        docID, userID, documentstatus, dmsg, db
                                    )
                                except Exception as e:
                                    logger.error(f"pfg_sync 501: {str(e)}")
                        else:
                            overAllstatus_msg = "Validation Failed"
                    else:
                        # VoucherCreation Data Validation Failed
                        # -------------------------update document history table
                        documentSubstatus = 36
                        documentstatus = 4
                        documentdesc = "Voucher data: validation error"
                        try:
                            update_docHistory(
                                docID, userID, documentstatus, documentdesc, db
                            )
                        except Exception as e:
                            logger.error(f"pfg_sync 501: {str(e)}")
                        try:
                            db.query(model.Document).filter(
                                model.Document.idDocument == docID
                            ).update(
                                {
                                    model.Document.documentStatusID: documentstatus,
                                    model.Document.documentsubstatusID: documentSubstatus,  # noqa: E501
                                }
                            )
                            db.commit()
                        except Exception as err:
                            logger.info(f"ErrorUpdatingPostingData: {err}")
                else:

                    # ----------------------------update document history table
                    documentSubstatus = 34
                    documentstatus = 4
                    documentdesc = "Invalid Store Type"
                    try:
                        update_docHistory(
                            docID, userID, documentstatus, documentdesc, db
                        )
                    except Exception as e:
                        logger.error(f"pfg_sync line 518: {str(e)}")
                    try:
                        db.query(model.Document).filter(
                            model.Document.idDocument == docID
                        ).update(
                            {
                                model.Document.documentStatusID: documentstatus,
                                model.Document.documentsubstatusID: documentSubstatus,
                            }
                        )
                        db.commit()
                    except Exception as err:
                        logger.info(f"ErrorUpdatingPostingData: {err}")
            else:
                # -------------------------update document history table
                documentSubstatus = 33
                documentstatus = 4
                documentdesc = "OCR Validations Failed"
                try:
                    update_docHistory(docID, userID, documentstatus, documentdesc, db)
                except Exception as e:
                    logger.error(f"pfg_sync line 534: {str(e)}")
                try:
                    db.query(model.Document).filter(
                        model.Document.idDocument == docID
                    ).update(
                        {
                            model.Document.documentStatusID: documentstatus,
                            model.Document.documentsubstatusID: documentSubstatus,
                        }
                    )
                    db.commit()
                except Exception as err:
                    logger.info(f"ErrorUpdatingPostingData: {err}")

        except Exception as err:
            logger.info(f"SyncException:{err}")
            logger.info(f"{traceback.format_exc()}")
            docStatusSync = {}
            overAllstatus = 0
            overAllstatus_msg = f"SyncException:{err}"
    else:
        try:
            update_docHistory(docID, userID, InvodocStatus, duplicate_status_ck_msg, db)
        except Exception as e:
            logger.error(f"pfg_sync line 886: {str(e)}")
        overAllstatus_msg = "Failed"

    docStatusSync["Status Overview"] = {
        "status": overAllstatus,
        "response": [overAllstatus_msg],
    }
    try:
        json_data = json.dumps(docStatusSync)
        db.query(model.Document).filter(model.Document.idDocument == docID).update(
            {model.Document.documentDescription: json_data}
        )

        db.commit()
    except Exception as Err:
        logger.info(f"updateDocDecError: {Err}")
    logger.info(f"Status Overview: {docStatusSync}")
    return docStatusSync

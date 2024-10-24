import json
import re
import traceback
from datetime import datetime
from typing import Union

from sqlalchemy import or_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

import pfg_app.model as model
from pfg_app.crud.ERPIntegrationCrud import processInvoiceVoucher
from pfg_app.crud.InvoiceCrud import update_docHistory
from pfg_app.FROps.customCall import customModelCall
from pfg_app.FROps.validate_currency import validate_currency
from pfg_app.logger_module import logger
from pfg_app.schemas.pfgtriggerSchema import InvoiceVoucherSchema


def clean_amount(amount_str):
    if isinstance(amount_str, float):
        amount_str = str(amount_str)
    try:
        cleaned_amount = re.findall(r"[\d.]+", amount_str)
        if cleaned_amount:
            return round(float("".join(cleaned_amount)), 2)
    except Exception:
        return None
    return 0.0


# db = SCHEMA
def IntegratedvoucherData(inv_id, db: Session):

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
    if len(stmp_dt_dict) > 0:
        confNumber = stmp_dt_dict["ConfirmationNumber"]
    else:
        confNumber = ""
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
    if invo_SubTotal_Tg in invo_hrd_data:
        invo_SubTotal = invo_hrd_data[invo_SubTotal_Tg]
    else:
        invo_SubTotal = invo_total
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
    if invo_SubTotal_Tg in invo_hrd_data:
        invo_SubTotal = invo_hrd_data[invo_SubTotal_Tg]
    else:
        invo_SubTotal = invo_total
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
    stmpDept = stmpData["Department"]
    itmSelected = stmpData["SelectedDept"]
    if itmSelected == "Inventory":
        ACCOUNT = "14100"
    elif itmSelected == "Supplies":
        ACCOUNT = "71999"
    else:
        ACCOUNT = ""

    if stmpDept.isdigit():
        dpt_cd = []
        if len(stmpDept) == 2:
            dpt_cd.append(stmpDept + "00")
            dpt_cd.append("00" + stmpDept)
        else:
            dpt_cd.append(stmpDept)

        dpt_cd_dt = (
            db.query(model.PFGDepartment)
            .filter(
                or_(
                    model.PFGDepartment.DEPTID.in_(
                        dpt_cd
                    ),  # DEPTID matches any value in dpt_cd
                    model.PFGDepartment.DEPTID == stmpDept,  # Exact match
                )
            )
            .all()
        )

        # Iterate and print matching results
        for department in dpt_cd_dt:
            DEPTID = department.DEPTID
            VENDOR_SETID = department.SETID
            BUSINESS_UNIT = "OFGDS"
            # ACCOUNT = "71999"

    else:
        try:

            dpt_cd_dt = (
                db.query(model.PFGDepartment)
                .filter(
                    or_(
                        model.PFGDepartment.DESCR.in_([stmpDept]),
                        model.PFGDepartment.DESCRSHORT == stmpDept,
                        model.PFGDepartment.DESCRSHORT.like("%" + stmpDept + "%"),
                    )
                )
                .all()
            )
            for department in dpt_cd_dt:
                DEPTID = department.DEPTID
                VENDOR_SETID = department.SETID
                BUSINESS_UNIT = "OFGDS"
                # ACCOUNT = "71999"
        except SQLAlchemyError:
            db.rollback()

        # dpt_cd_dt = (
        #     db.query(model.PFGDepartment)
        #     .filter(
        #         or_(
        #             model.PFGDepartment.DESCR.in_([stmpData["Department"]]),
        #             model.PFGDepartment.DESCRSHORT == [stmpData["Department"]],
        #             model.PFGDepartment.DESCRSHORT.like(stmpData["Department"]),
        #         )
        #     )
        #     .all()
        # )
        # for department in dpt_cd_dt:
        #     DEPTID = department.DEPTID
        #     VENDOR_SETID = department.SETID
        #     BUSINESS_UNIT = "OFGDS"
        #     ACCOUNT = "71999"

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


def format_and_validate_date(date_str):
    dateValCk = 0
    try:
        date_str = date_str.replace("-", " ").replace("/", " ").replace(".", " ")

        date_obj = datetime.strptime(date_str, "%Y %m %d")
        formatted_date = date_obj.strftime("%Y-%m-%d")
        dateValCk = 1
    except ValueError:
        formatted_date = date_str
        dateValCk = 0
    return formatted_date, dateValCk


def pfg_sync(docID, userID, db: Session):
    logger.info(f"start on the pfg_sync,DocID{docID}")

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
    # dsdApprovalCheck = 0
    # dsdApprovalCheck_msg = 5000
    duplicate_status_ck = 0
    duplicate_status_ck_msg = ""
    InvodocStatus = ""
    fileSizeThreshold = 10
    confirmation_ck = 0
    confirmation_ck_msg = ""
    row_count = 0

    try:

        docTb = (
            db.query(model.Document).filter(model.Document.idDocument == docID).all()
        )

        for dtb_rw in docTb:
            InvodocStatus = dtb_rw.documentStatusID
            filePath = dtb_rw.docPath
    except Exception as e:
        logger.error(f"Exception in pfg_sync line 294: {str(e)}")

    sentToPPlSft = {
        7: "Sent to PeopleSoft",
        29: "Voucher Created",
        30: "Voucher Not Found",
        27: "Quick Invoice",
        14: "Posted In PeopleSoft",
        28: "Recycled Invoice",
    }

    if InvodocStatus in sentToPPlSft.keys():
        if isinstance(InvodocStatus, int):
            docStatusSync[sentToPPlSft[InvodocStatus]] = {
                "status": 1,
                "response": ["Invoice Already Sent to PeopleSoft"],
            }
    else:

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
        try:
            if isinstance(InvodocStatus, int) and InvodocStatus == 26:
                customModelCall(docID)
        except Exception:
            logger.error(f"{traceback.format_exc()}")
        # ----------
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
        tagNames = {}

        for document_data, document_tag_def in DocDtHdr:
            docHdrDt[document_tag_def.TagLabel] = document_data.Value
            tagNames[document_tag_def.TagLabel] = document_tag_def.idDocumentTagDef
        logger.info(f"docHdrDt: {docHdrDt}")

        # ----------------
        if len(docHdrDt) > 0:
            if duplicate_status_ck == 1:
                try:
                    update_docHistory(
                        docID, userID, InvodocStatus, duplicate_status_ck_msg, db
                    )
                except Exception:
                    logger.debug(traceback.format_exc())
                try:

                    try:

                        logger.info(f"docHdrDt: {docHdrDt}")

                        try:
                            if "Currency" in docHdrDt:
                                Currency = docHdrDt["Currency"]
                                # Call the validate_currency function
                                # which now returns True or False
                                isCurrencyMatch = validate_currency(
                                    docID, Currency, db
                                )  # noqa: E501

                                # Check if the currency matched
                                # (True means match, False means no match)
                                if isCurrencyMatch:  # No need to compare to 'True'
                                    dmsg = "Success"

                                else:
                                    dmsg = "Invoice Currency Invalid"

                            else:
                                dmsg = "No currency found in the OpenAI result"
                            logger.info(f"dmsg: {dmsg}")
                        except Exception:
                            logger.debug(f"Error occurred: {traceback.format_exc()}")
                        # Invoice Total Approval Check
                        # try:
                        #     if float(docHdrDt["InvoiceTotal"]) < dsdApprovalCheck_msg:
                        #         dsdApprovalCheck = 1
                        #         dmsg = "Success"
                        #  elif float(docHdrDt["InvoiceTotal"]) > dsdApprovalCheck_msg:
                        #         try:
                        #             docStatus = 6
                        #             docSubStatus = 113
                        #             dmsg = f"Invoice Amount:{
                        #                 float(docHdrDt['InvoiceTotal'])
                        #                 }, Approval Needed."  # noqa: E501

                        #             try:
                        #                 update_docHistory(
                        #                     docID, userID, docStatus, dmsg, db
                        #                 )
                        #             except Exception as e:
                        #                 logger.error(f"pfg_sync line 534: {str(e)}")
                        #                 dmsg = str(e)

                        #             try:
                        #                 db.query(model.Document).filter(
                        #                     model.Document.idDocument == docID
                        #                 ).update(
                        #                     {
                        #                         model.Document.documentStatusID: docStatus,  # noqa: E501
                        #                         model.Document.documentsubstatusID: docSubStatus,  # noqa: E501
                        #                     }
                        #                 )
                        #                 db.commit()
                        #             except Exception as err:
                        #               logger.info(f"ErrorUpdatingPostingData: {err}")
                        #                 dmsg = str(err)

                        #         except Exception as e:
                        #             logger.error(
                        #                 f"pfg_sync amount validations: {str(e)}"
                        #             )
                        #             dmsg = "Invoice Amount Invalid" + str(e)
                        #     else:
                        #         dsdApprovalCheck = 0
                        #         dmsg = "Invoice Amount Invalid"

                        # except Exception as e:
                        #     logger.error(f"pfg_sync amount validations: {str(e)}")
                        #     dmsg = str(e)

                        # docStatusSync["Amount Approval Validation"] = {
                        #     "status": dsdApprovalCheck,
                        #     "response": [dmsg],
                        # }

                        # Invoice Total check

                        invTotalMth = 0
                        invTotalMth_msg = "Invoice total mismatch, please review."
                        # if dsdApprovalCheck == 1:

                        try:
                            if "SubTotal" in docHdrDt:
                                subTotal = clean_amount(docHdrDt["SubTotal"])

                                if subTotal is not None:
                                    invoTotal = clean_amount(docHdrDt["InvoiceTotal"])
                                    if invoTotal is not None:
                                        if invoTotal == subTotal:
                                            invTotalMth = 1
                                        elif round(abs(invoTotal - subTotal), 2) < 0.09:
                                            invTotalMth = 1
                                        if (invTotalMth == 0) and (
                                            "TotalTax" in docHdrDt
                                        ):
                                            totlTax = clean_amount(docHdrDt["TotalTax"])
                                            if totlTax is not None:
                                                sm_tx = clean_amount(subTotal + totlTax)
                                                if sm_tx is not None:
                                                    if sm_tx == invoTotal:
                                                        invTotalMth = 1
                                                    elif (
                                                        round(abs(sm_tx - invoTotal), 2)
                                                        < 0.09
                                                    ):  # noqa: E501
                                                        invTotalMth = 1
                                        if (invTotalMth == 0) and ("PST" in docHdrDt):
                                            pst = clean_amount(docHdrDt["PST"])
                                            if pst is not None:
                                                pst_sm = clean_amount(subTotal + pst)
                                                if pst_sm is not None:
                                                    if pst_sm == invoTotal:
                                                        invTotalMth = 1
                                                    elif (
                                                        round(
                                                            abs(pst_sm - invoTotal), 2
                                                        )
                                                        < 0.09
                                                    ):  # noqa: E501
                                                        invTotalMth = 1

                                                if (invTotalMth == 0) and (
                                                    "TotalTax" in docHdrDt
                                                ):
                                                    pstTTax_sm = clean_amount(
                                                        subTotal + pst + totlTax
                                                    )
                                                    if pstTTax_sm is not None:
                                                        if pstTTax_sm == invoTotal:
                                                            invTotalMth = 1
                                                        elif (
                                                            round(
                                                                abs(
                                                                    pstTTax_sm
                                                                    - invoTotal
                                                                ),
                                                                2,
                                                            )
                                                            < 0.09
                                                        ):  # noqa: E501
                                                            invTotalMth = 1

                                        if (invTotalMth == 0) and ("GST" in docHdrDt):
                                            gst = clean_amount(docHdrDt["GST"])
                                            if gst is not None:
                                                gst_sm = clean_amount(subTotal + gst)
                                                if gst_sm is not None:
                                                    if gst_sm == invoTotal:
                                                        invTotalMth = 1
                                                    elif (
                                                        round(
                                                            abs(gst_sm - invoTotal), 2
                                                        )
                                                        < 0.09
                                                    ):  # noqa: E501
                                                        invTotalMth = 1

                                                if (invTotalMth == 0) and (
                                                    "PST" in docHdrDt
                                                ):
                                                    pst_gst_sm = clean_amount(
                                                        subTotal + gst + pst
                                                    )
                                                    if pst_gst_sm is not None:
                                                        if pst_gst_sm == invoTotal:
                                                            invTotalMth = 1
                                                        elif (
                                                            round(
                                                                abs(
                                                                    pst_gst_sm
                                                                    - invoTotal
                                                                ),
                                                                2,
                                                            )
                                                            < 0.09
                                                        ):  # noqa: E501
                                                            invTotalMth = 1

                                        if (invTotalMth == 0) and ("HST" in docHdrDt):
                                            hst = clean_amount(docHdrDt["HST"])
                                            if hst is not None:
                                                hst_sm = clean_amount(subTotal + hst)
                                                if hst_sm is not None:
                                                    if hst_sm == invoTotal:
                                                        invTotalMth = 1
                                                    elif (
                                                        round(
                                                            abs(hst_sm - invoTotal), 2
                                                        )
                                                        < 0.09
                                                    ):  # noqa: E501
                                                        invTotalMth = 1

                                                if (invTotalMth == 0) and (
                                                    "GST" in docHdrDt
                                                ):
                                                    hst_gst_sm = clean_amount(
                                                        subTotal + hst + gst
                                                    )
                                                    if hst_gst_sm is not None:
                                                        if hst_gst_sm == invoTotal:
                                                            invTotalMth = 1
                                                        elif (
                                                            round(
                                                                abs(
                                                                    hst_gst_sm
                                                                    - invoTotal
                                                                ),
                                                                2,
                                                            )
                                                            < 0.09
                                                        ):  # noqa: E501
                                                            invTotalMth = 1

                                        if (invTotalMth == 0) and (
                                            "LitterDeposit" in docHdrDt
                                        ):
                                            litterDeposit = clean_amount(
                                                docHdrDt["LitterDeposit"]
                                            )
                                            if litterDeposit is not None:
                                                litterDeposit_sm = clean_amount(
                                                    subTotal + litterDeposit
                                                )
                                                if litterDeposit_sm is not None:
                                                    if litterDeposit_sm == invoTotal:
                                                        invTotalMth = 1
                                                    elif (
                                                        round(
                                                            abs(
                                                                litterDeposit_sm
                                                                - invoTotal
                                                            ),
                                                            2,
                                                        )
                                                        < 0.09
                                                    ):  # noqa: E501
                                                        invTotalMth = 1
                                        if (invTotalMth == 0) and (
                                            "Fuel surcharge" in docHdrDt
                                        ):  # noqa: E501
                                            surcharge = clean_amount(
                                                docHdrDt["Fuel surcharge"]
                                            )  # noqa: E501
                                            if surcharge is not None:
                                                surchargr_sm = clean_amount(
                                                    surcharge + subTotal
                                                )  # noqa: E501
                                                if (
                                                    round(
                                                        abs(surchargr_sm - invoTotal),
                                                        2,
                                                    )
                                                    < 0.09
                                                ):  # noqa: E501
                                                    invTotalMth = 1

                                        if (invTotalMth == 0) and (
                                            "ShipmentCharges" in docHdrDt
                                        ):  # noqa: E501
                                            ShipmentCharges = clean_amount(
                                                docHdrDt["ShipmentCharges"]
                                            )  # noqa: E501
                                            if ShipmentCharges is not None:
                                                ShipmentCharges_sm = clean_amount(
                                                    ShipmentCharges + subTotal
                                                )  # noqa: E501
                                                if (
                                                    round(
                                                        abs(
                                                            ShipmentCharges_sm
                                                            - invoTotal
                                                        ),
                                                        2,
                                                    )
                                                    < 0.09
                                                ):
                                                    invTotalMth = 1
                            else:
                                invTotalMth = 1
                                invTotalMth_msg = "Skip total check: Subtotal Missing"
                        except Exception as e:
                            logger.debug(traceback.format_exc())
                            invTotalMth = 0
                            invTotalMth_msg = "Invoice total mismatch:" + str(e)
                    except Exception as e:
                        logger.debug(traceback.format_exc())
                        invTotalMth = 0
                        invTotalMth_msg = "Invoice total mismatch:" + str(e)

                    try:
                        date_string = docHdrDt["InvoiceDate"]  # TODO: Unused variable
                        try:
                            formatted_date, dateValCk = format_and_validate_date(
                                date_string
                            )
                            if dateValCk == 1:
                                dateCheck = 1
                            else:
                                dateCheck = 0
                                dateCheck_msg = (
                                    "Invoice date is invalid, Please review."
                                )
                            if formatted_date != date_string:
                                # updating formatted date string:
                                try:
                                    print(formatted_date)
                                    print(date_string)

                                except Exception:
                                    logger.debug(traceback.format_exc())

                        except Exception as er:
                            logger.error(traceback.format_exc())
                            dateCheck = 0
                            dateCheck_msg = str(er)

                    except Exception:
                        logger.error(traceback.format_exc())
                        dateCheck = 0
                        dateCheck_msg = (
                            "Failed to validate the invoice date,Please review."
                        )

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

                    if (
                        docStatusSync["OCR Validations"]["status"] == 1
                        and docStatusSync["Invoice Total Validation"]["status"] == 1
                    ):

                        # -----------------------update document history table
                        documentstatus = 4
                        documentdesc = "OCR Validations Success"
                        try:
                            update_docHistory(
                                docID, userID, documentstatus, documentdesc, db
                            )
                        except Exception:
                            logger.error(traceback.format_exc())

                        InvStmDt = (
                            db.query(model.StampDataValidation)
                            .filter(model.StampDataValidation.documentid == docID)
                            .all()
                        )
                        stmpData = {}
                        for stDt in InvStmDt:
                            stmpData[stDt.stamptagname] = {
                                stDt.stampvalue: stDt.is_error
                            }
                        if InvStmDt and len(stmpData) > 0:

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
                                except Exception:
                                    logger.debug(traceback.format_exc)
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
                                except Exception:
                                    logger.debug(traceback.format_exc)

                                try:
                                    if (
                                        list(stmpData["StoreType"].keys())[0]
                                        == "Integrated"
                                    ):

                                        # -----------------------------------------
                                        if "ConfirmationNumber" in stmpData:
                                            Confirmation_rw = list(
                                                stmpData["ConfirmationNumber"].keys()
                                            )[0]

                                            Confirmation = "".join(
                                                re.findall(r"\d", Confirmation_rw)
                                            )
                                            if len(Confirmation) == 9:
                                                try:

                                                    RepTb = (
                                                        db.query(model.PFGReceipt)
                                                        .filter(
                                                            model.PFGReceipt.RECEIVER_ID
                                                            == Confirmation
                                                        )
                                                        .all()
                                                    )  # noqa: E501

                                                    if RepTb:
                                                        confirmation_ck = 1
                                                        confirmation_ck_msg = (
                                                            "Valid Confirmation Number"
                                                        )

                                                    else:
                                                        confirmation_ck = 0
                                                        confirmation_ck_msg = "Confirmation number not found"  # noqa: E501s

                                                except Exception as e:
                                                    logger.debug(
                                                        f"{traceback.format_exc()}"
                                                    )

                                                    confirmation_ck = 0
                                                    confirmation_ck_msg = (
                                                        "Error:" + str(e)
                                                    )  # noqa: E501

                                            else:
                                                confirmation_ck = 0
                                                confirmation_ck_msg = (
                                                    "Invalid Confirmation Number"
                                                )

                                        else:

                                            confirmation_ck = 0
                                            confirmation_ck_msg = (
                                                "Confirmation Number NotFound"
                                            )

                                        # -----------------------------------

                                        strCk = 1
                                        strCk_msg.append("Success")
                                        if confirmation_ck == 1:
                                            IntegratedvoucherData(docID, db)

                                    if (
                                        list(stmpData["StoreType"].keys())[0]
                                        == "Non-Integrated"
                                    ):
                                        nonIntegratedVoucherData(docID, db)
                                        strCk = 1
                                        strCk_msg.append("Success")

                                except Exception:

                                    logger.debug(f"{traceback.format_exc()}")

                                if confirmation_ck == 1 or (
                                    list(stmpData["StoreType"].keys())[0]
                                    == "Non-Integrated"
                                ):
                                    voucher_query = db.query(model.VoucherData).filter(
                                        model.VoucherData.documentID == docID
                                    )
                                    row_count = voucher_query.count()
                                    NullVal = []
                                    VthChk = 0
                                    VthChk_msg = ""
                                if row_count == 1:

                                    voucher_row = voucher_query.first()
                                    has_null_or_empty = False
                                    for column in model.VoucherData.__table__.columns:
                                        value = getattr(voucher_row, column.name)
                                        if value is None or value == "":
                                            has_null_or_empty = True
                                            NullVal.append(column.name)

                                    if has_null_or_empty:
                                        VthChk = 0
                                        VthChk_msg = (
                                            "Missing values:" + str(NullVal)[1:-1]
                                        )  # noqa: E501
                                    else:
                                        VthChk = 1
                                        VthChk_msg = "Success"

                                elif confirmation_ck == 0:
                                    VthChk = confirmation_ck
                                    VthChk_msg = confirmation_ck_msg
                                elif row_count > 1:
                                    VthChk = 0
                                    VthChk_msg = "Multiple entries found"

                                else:
                                    VthChk = 0
                                    VthChk_msg = "No Voucher data Found."
                                docStatusSync["Voucher creation data validation"] = {
                                    "status": VthChk,
                                    "response": [VthChk_msg],
                                }
                                logger.info(f"docStatusSync:{docStatusSync}")

                                # file size check:
                                try:
                                    frTriggerTab = (
                                        db.query(model.frtrigger_tab)
                                        .filter(
                                            model.frtrigger_tab.blobpath == filePath
                                        )  # noqa: E501
                                        .all()
                                    )

                                    for fr_rw in frTriggerTab:
                                        fileSize = fr_rw.filesize
                                    if len(fileSize) > 0:
                                        if float(fileSize) <= fileSizeThreshold:
                                            docStatusSync["File size check"] = {
                                                "status": 1,
                                                "response": ["Success"],
                                            }
                                        else:
                                            docStatusSync["File size check"] = {
                                                "status": 1,
                                                "response": [
                                                    "FileSize:" + str(fileSize) + "MB."
                                                ],
                                            }
                                    else:
                                        docStatusSync["File size check"] = {
                                            "status": 0,
                                            "response": ["File Size not found."],
                                        }
                                except Exception:

                                    logger.debug(f"{traceback.format_exc()}")

                                if (
                                    docStatusSync["Voucher creation data validation"][
                                        "status"
                                    ]
                                    == 1
                                ):
                                    documentstatus = 4
                                    documentdesc = "VoucherCreation Data \
                                        Validation Success"
                                    try:
                                        update_docHistory(
                                            docID,
                                            userID,
                                            documentstatus,
                                            documentdesc,
                                            db,  # noqa: E501
                                        )
                                    except Exception:
                                        logger.debug(traceback.format_exc())

                                    overAllstatus_ck = 1
                                    for stCk in docStatusSync:
                                        if stCk != "File Size Check":
                                            valCkStatus = docStatusSync[stCk]["status"]
                                            if type(valCkStatus) is int:
                                                overAllstatus_ck = (
                                                    overAllstatus_ck * valCkStatus
                                                )

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
                                        SentToPeopleSoft = 0
                                        dmsg = ""
                                        try:
                                            resp = processInvoiceVoucher(docID, db)
                                            try:
                                                if "data" in resp:
                                                    if "Http Response" in resp["data"]:
                                                        RespCode = resp["data"][
                                                            "Http Response"
                                                        ]
                                                        if resp["data"][
                                                            "Http Response"
                                                        ].isdigit():
                                                            RespCodeInt = int(RespCode)
                                                            if RespCodeInt == 201:
                                                                SentToPeopleSoft = 1
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
                                                logger.debug(
                                                    f"PopleSoftResponseError: {traceback.format_exc()}"  # noqa: E501
                                                )
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
                                                        model.Document.documentStatusID: docStatus,  # noqa: E501
                                                        model.Document.documentsubstausID: docSubStatus,  # noqa: E501
                                                    }
                                                )
                                                db.commit()
                                            except Exception:
                                                logger.error(traceback.format_exc())
                                            try:

                                                update_docHistory(
                                                    docID, userID, docStatus, dmsg, db
                                                )

                                            except Exception:
                                                logger.error(traceback.format_exc())
                                        except Exception as e:

                                            logger.debug(traceback.format_exc())
                                            dmsg = InvoiceVoucherSchema.FAILURE_COMMON.format_message(  # noqa: E501
                                                e
                                            )
                                            docStatus = 21
                                            docSubStatus = 112

                                        docStatusSync["Sent to PeopleSoft"] = {
                                            "status": SentToPeopleSoft,
                                            "response": [dmsg],
                                        }

                                        try:
                                            db.query(model.Document).filter(
                                                model.Document.idDocument == docID
                                            ).update(
                                                {
                                                    model.Document.documentStatusID: docStatus,  # noqa: E501
                                                    model.Document.documentsubstatusID: docSubStatus,  # noqa: E501
                                                }
                                            )
                                            db.commit()
                                        except Exception:
                                            logger.debug(traceback.format_exc())

                                        try:
                                            documentstatus = 21
                                            update_docHistory(
                                                docID,
                                                userID,
                                                documentstatus,
                                                dmsg,
                                                db,  # noqa: E501
                                            )
                                        except Exception:
                                            logger.debug(f"{traceback.format_exc()}")
                                    else:
                                        overAllstatus_msg = "Validation Failed"
                                else:
                                    # VoucherCreation Data Validation Failed
                                    # ------update document history table
                                    documentSubstatus = 36
                                    documentstatus = 4
                                    documentdesc = "Voucher data: validation error"
                                    try:
                                        update_docHistory(
                                            docID,
                                            userID,
                                            documentstatus,
                                            documentdesc,
                                            db,  # noqa: E501
                                        )
                                    except Exception:
                                        logger.debug(f"{traceback.format_exc()}")
                                    try:
                                        db.query(model.Document).filter(
                                            model.Document.idDocument == docID
                                        ).update(
                                            {
                                                model.Document.documentStatusID: documentstatus,  # noqa: E501
                                                model.Document.documentsubstatusID: documentSubstatus,  # noqa: E501
                                            }
                                        )
                                        db.commit()
                                    except Exception:
                                        logger.debug(f"{traceback.format_exc()}")
                            else:

                                # -------------update document history table
                                documentSubstatus = 34
                                documentstatus = 4
                                documentdesc = "Invalid Store Type"
                                try:
                                    update_docHistory(
                                        docID, userID, documentstatus, documentdesc, db
                                    )
                                except Exception:
                                    logger.debug(f"{traceback.format_exc()}")
                                try:
                                    db.query(model.Document).filter(
                                        model.Document.idDocument == docID
                                    ).update(
                                        {
                                            model.Document.documentStatusID: documentstatus,  # noqa: E501
                                            model.Document.documentsubstatusID: documentSubstatus,  # noqa: E501
                                        }
                                    )
                                    db.commit()
                                except Exception:
                                    logger.debug(f"{traceback.format_exc()}")
                        else:
                            docStatusSync["Stamp Data Validations"] = {
                                "status": 0,
                                "response": ["No Stamp Data Found"],
                            }
                            documentSubstatus = 118
                            documentstatus = 4
                            documentdesc = "No Stamp Data Found"
                            try:
                                update_docHistory(
                                    docID, userID, documentstatus, documentdesc, db
                                )
                            except Exception:
                                logger.debug(f"{traceback.format_exc()}")
                                overAllstatus_msg = "Failed"

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
                            except Exception:
                                logger.debug(f"{traceback.format_exc()}")

                    else:
                        # -------------------------update document history table
                        documentSubstatus = 33
                        documentstatus = 4
                        documentdesc = "OCR Validations Failed"
                        try:
                            update_docHistory(
                                docID, userID, documentstatus, documentdesc, db
                            )
                        except Exception:

                            logger.error(f"{traceback.format_exc()}")
                            overAllstatus_msg = "Failed"
                    # try:
                    #     db.query(model.Document).filter(
                    #         model.Document.idDocument == docID
                    #     ).update(
                    #         {
                    #             model.Document.documentStatusID: documentstatus,
                    #             model.Document.documentsubstatusID: documentSubstatus,
                    #         }
                    #     )
                    #     db.commit()
                    # except Exception as err:
                    #     logger.info(f"ErrorUpdatingPostingData: {err}")

                except Exception as err:
                    logger.error(f"{traceback.format_exc()}")
                    docStatusSync = {}
                    overAllstatus = 0
                    overAllstatus_msg = f"SyncException:{err}"
            else:
                try:
                    update_docHistory(
                        docID, userID, InvodocStatus, duplicate_status_ck_msg, db
                    )
                except Exception as e:
                    logger.error(f"pfg_sync line 886: {str(e)}")
                overAllstatus_msg = "Failed"
        else:

            try:
                docHrd_msg = "No Header Data found"
                docHrd_status = 0
                update_docHistory(docID, userID, docHrd_status, docHrd_msg, db)
            except Exception:
                logger.debug(traceback)
            overAllstatus_msg = "Failed"

        if (overAllstatus == 1) and ("Sent to PeopleSoft" in docStatusSync):
            if docStatusSync["Sent to PeopleSoft"]["status"] == 0:
                overAllstatus = 0
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
            logger.debug(f"updateDocDecError: {Err}")
        # logger.info(f"Status Overview: {docStatusSync}")
    return docStatusSync

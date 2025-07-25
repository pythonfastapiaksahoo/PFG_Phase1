#pfg_sync
import json
import re
import traceback
from datetime import datetime
from typing import Union
from sqlalchemy import func
from sqlalchemy import and_, or_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.sql import case

import pfg_app.model as model
from pfg_app.crud.ERPIntegrationCrud import processInvoiceVoucher
from pfg_app.crud.InvoiceCrud import update_docHistory
# from pfg_app.routers.OCR import update_docHistory
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


def crd_clean_amount(amount_str):
    if isinstance(amount_str, float):
        amount_str = str(amount_str)
    elif isinstance(amount_str, int):
        amount_str = str(amount_str)
    try:
        cleaned_amount = re.findall(r"[\d.]+", amount_str)
        if cleaned_amount:
            return round(float("".join(cleaned_amount)), 2) * -1
    except Exception:
        logger.info(traceback.format_exc())
        return None
    return 0.0


# db = SCHEMA
def IntegratedvoucherData(userID, inv_id, gst_amt, payload_subtotal, CreditNote, db: Session):
    voucher_data_status = 1
    intStatus = 0
    recvLineNum = 0
    intStatusMsg = ""
    vdrMatchStatus = 0
    vdrStatusMsg = ""
    if "credit" in CreditNote.lower():
        crt_ck_status = 1
        gst_amt = crd_clean_amount(gst_amt)
    else:
        crt_ck_status = 0
    stmp_dt = (
        db.query(model.StampDataValidation)
        .filter(model.StampDataValidation.documentid == inv_id)
        .all()
    )

    stmp_dt_dict = {}
    for dtm_rw in stmp_dt:
        stmp_dt_dict[dtm_rw.stamptagname] = dtm_rw.stampvalue
    storeNumber, storeType, confNumber = "", "", ""
    if len(stmp_dt_dict) > 0:
        if "ConfirmationNumber" in stmp_dt_dict:
            confNumber = stmp_dt_dict["ConfirmationNumber"]
        else:
            voucher_data_status = 0
        if "StoreNumber" in stmp_dt_dict:
            storeNumber_rw = stmp_dt_dict["StoreNumber"]
        else:
            voucher_data_status = 0
        if "StoreType" in stmp_dt_dict:
            storeType = stmp_dt_dict["StoreType"]
        else:
            voucher_data_status = 0
    else:
        voucher_data_status = 0

    invo_recp = (
        db.query(model.PFGReceipt)
        .filter(model.PFGReceipt.RECEIVER_ID == confNumber)
        .all()
    )
    BUSINESS_UNIT = ""  # type: ignore
    VENDOR_SETID = ""  # type: ignore
    vendor_id = ""  # type: ignore
    ACCOUNT = ""  # type: ignore
    DEPTID = ""  # type: ignore
    location = ""  # type: ignore
    recvLineNum = ""  # type: ignore
    location_rw = ""

    for invRpt in invo_recp:
        BUSINESS_UNIT = invRpt.BUSINESS_UNIT
        VENDOR_SETID = invRpt.VENDOR_SETID
        Supplier_id = invRpt.VENDOR_ID
        ACCOUNT = invRpt.ACCOUNT
        DEPTID = invRpt.DEPTID
        location_rw = invRpt.LOCATION
        recvLineNum = invRpt.RECV_LN_NBR

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
        vendor_id = result[0]

    if Supplier_id == vendor_id:
        vdrMatchStatus = 1
        vdrStatusMsg = "Supplier ID Match success"
    else:
        # if skip_supplierCk==1:
        #     vdrMatchStatus = 1
        #     vdrStatusMsg = "Supplier ID Match skipped"
        # else:
        vdrMatchStatus = 0
        vdrStatusMsg = (
            "Supplier ID Mismatch.\nReceiptMaster's Supplier ID: "
            + str(Supplier_id)
            + "\nMapped Supplier ID: "
            + str(vendor_id)
        )  # noqa: E501

    # check data type of recvLineNum and if its not int make it to 0
    if type(recvLineNum) is not int:
        recvLineNum = 0

    if type(location_rw) is not int and location_rw.isdigit():
        location = int(location_rw)
    else:
        location = 0

    if type(storeNumber_rw) is not int and storeNumber_rw.isdigit():
        storeNumber = int(storeNumber_rw)
    else:
        storeNumber = 0
        storeNumber_rw = "0"

    if location == storeNumber and location != "" and location != 0:
        intStatus = 1
        intStatusMsg = "Store match Success"
    else:
        intStatus = 0
        intStatusMsg = "Incorrect store number"

    if intStatus == 1:

        # ---------------------------------------------
        docTabData = (
            db.query(model.Document).filter(model.Document.idDocument == inv_id).first()
        )

        if docTabData:
            docPath = docTabData.docPath
            docModel = docTabData.documentModelID
            invoice_type = docTabData.UploadDocType
        else:
            voucher_data_status = 0

        DocDtHdr = (
            db.query(model.DocumentData, model.DocumentTagDef)
            .join(
                model.DocumentTagDef,
                model.DocumentData.documentTagDefID
                == model.DocumentTagDef.idDocumentTagDef,
            )
            .filter(model.DocumentTagDef.idDocumentModel == docModel)
            .filter(model.DocumentData.documentID == inv_id)
            .all()
        )

        docHdrDt = {}
        tagNames = {}

        for document_data, document_tag_def in DocDtHdr:
            docHdrDt[document_tag_def.TagLabel] = document_data.Value
            tagNames[document_tag_def.TagLabel] = document_tag_def.idDocumentTagDef
        currency_code = ""
        freight_charges = 0
        misc_amt = 0
        #
        if "InvoiceTotal" in docHdrDt:
            if crt_ck_status == 1:
                invo_total = crd_clean_amount(docHdrDt["InvoiceTotal"])
            else:
                invo_total = clean_amount(docHdrDt["InvoiceTotal"])
            if payload_subtotal == "":
                if "SubTotal" in docHdrDt:
                    if crt_ck_status == 1:
                        invo_SubTotal = crd_clean_amount(docHdrDt["SubTotal"])
                    else:
                        invo_SubTotal = clean_amount(docHdrDt["SubTotal"])
                else:
                    if "GST" in docHdrDt:
                        if crt_ck_status == 1:
                            invo_SubTotal = crd_clean_amount(
                                docHdrDt["InvoiceTotal"]
                            ) - crd_clean_amount(docHdrDt["GST"])
                        else:
                            invo_SubTotal = clean_amount(
                                docHdrDt["InvoiceTotal"]
                            ) - clean_amount(docHdrDt["GST"])

                    elif "TotalTax" in docHdrDt:
                        if crt_ck_status == 1:
                            invo_SubTotal = crd_clean_amount(
                                docHdrDt["InvoiceTotal"]
                            ) - crd_clean_amount(docHdrDt["TotalTax"])
                        else:
                            invo_SubTotal = clean_amount(
                                docHdrDt["InvoiceTotal"]
                            ) - clean_amount(docHdrDt["TotalTax"])
                    else:
                        invo_SubTotal = invo_total
            else:
                if crt_ck_status == 1:
                    invo_SubTotal = crd_clean_amount(payload_subtotal)
                else:
                    invo_SubTotal = clean_amount(payload_subtotal)

        else:
            voucher_data_status = 0
        if "InvoiceDate" in docHdrDt:
            invo_Date = docHdrDt["InvoiceDate"]
        else:
            voucher_data_status = 0
        if "InvoiceId" in docHdrDt:
            invo_ID = docHdrDt["InvoiceId"]
        else:
            voucher_data_status = 0

        if "FreightCharges" in docHdrDt:
            if crt_ck_status == 1:
                freight_charges = crd_clean_amount(docHdrDt["FreightCharges"])
            else:
                freight_charges = clean_amount(docHdrDt["FreightCharges"])

        # try:
        if "Currency" in docHdrDt:
            currency_code_rw = docHdrDt["Currency"]

            isCurrencyMatch = validate_currency(inv_id, currency_code_rw, db)

            if isCurrencyMatch:
                currency_code = currency_code_rw
            else:
                currency_code = "CAD"
        else:
            currency_code = "CAD"
        try:
            if gst_amt > 0:
                vat_applicability = 'T'
            else:
                vat_applicability = 'O'
        except Exception:
            logger.debug(traceback.format_exc())   
            vat_applicability = 'O' 
        
        try:
            get_tmid_qry = (
                db.query(model.User)
                .filter(model.User.idUser == userID, model.User.employee_id.isnot(None))
                .first()
            )
           
            if get_tmid_qry:
                employee_id = get_tmid_qry.employee_id
            else:
                employee_id = None  # Explicitly set to None when no matching record

        except Exception as e:
            logger.error(f"Error in getting employee_id: {e}")  # Use error level for logging
            employee_id = None
            
        if voucher_data_status == 1:

            existing_record = (
                db.query(model.VoucherData).filter_by(documentID=inv_id).first()
            )

            if existing_record:
                # If record exists, update the existing record with new data
                existing_record.Business_unit = BUSINESS_UNIT
                existing_record.Invoice_Id = invo_ID
                existing_record.Invoice_Dt = invo_Date
                existing_record.Vendor_Setid = VENDOR_SETID
                existing_record.Vendor_ID = vendor_id
                existing_record.Deptid = DEPTID
                existing_record.Account = ACCOUNT
                existing_record.Gross_Amt = invo_total
                existing_record.Merchandise_Amt = invo_SubTotal
                existing_record.File_Name = docPath.split("/")[-1]
                existing_record.Distrib_Line_num = 1
                existing_record.Voucher_Line_num = 1
                existing_record.Image_Nbr = 1
                existing_record.Origin = invoice_type
                existing_record.storenumber = location_rw
                existing_record.storetype = storeType
                existing_record.receiver_id = str(confNumber)
                existing_record.status = voucher_data_status
                existing_record.recv_ln_nbr = recvLineNum
                existing_record.gst_amt = gst_amt
                existing_record.currency = currency_code
                existing_record.freight_amt = freight_charges
                existing_record.misc_amt = misc_amt
                existing_record.vat_applicability = vat_applicability
                existing_record.opr_id = employee_id
            else:
                # If no record exists, create a new one
                VoucherData_insert_data = {
                    "documentID": inv_id,
                    "Business_unit": BUSINESS_UNIT,
                    "Invoice_Id": invo_ID,
                    "Invoice_Dt": invo_Date,
                    "Vendor_Setid": VENDOR_SETID,
                    "Vendor_ID": vendor_id,
                    "Deptid": DEPTID,
                    "Account": ACCOUNT,
                    "Gross_Amt": invo_total,
                    "Merchandise_Amt": invo_SubTotal,
                    "File_Name": docPath.split("/")[-1],
                    "Distrib_Line_num": 1,
                    "Voucher_Line_num": 1,
                    "Image_Nbr": 1,
                    "Origin": invoice_type,
                    "storenumber": str(location_rw),
                    "storetype": storeType,
                    "receiver_id": str(confNumber),
                    "status": voucher_data_status,
                    "recv_ln_nbr": recvLineNum,
                    "gst_amt": gst_amt,
                    "currency_code": currency_code,
                    "freight_amt": freight_charges,
                    "misc_amt": misc_amt,
                    "vat_applicability": vat_applicability,
                    "opr_id": employee_id
                }
                VD_db_data = model.VoucherData(**VoucherData_insert_data)
                db.add(VD_db_data)

            # Commit the changes to the database
            db.commit()
    return intStatus, intStatusMsg, vdrMatchStatus, vdrStatusMsg


def nonIntegratedVoucherData(
    userID, inv_id, gst_amt, payload_subtotal, CreditNote, db: Session
):
    nonIntStatus = 1
    nonIntStatusMsg = ""
    voucher_data_status = 1
    recvLineNum = 0
    docTabData = (
        db.query(model.Document).filter(model.Document.idDocument == inv_id).first()
    )

    if "credit" in CreditNote.lower():
        crt_ck_status = 1
        gst_amt = crd_clean_amount(gst_amt)
    else:
        crt_ck_status = 0

    if docTabData:
        docPath = docTabData.docPath
        docModel = docTabData.documentModelID
        invoice_type = docTabData.UploadDocType
    else:
        voucher_data_status = 0

    DocDtHdr = (
        db.query(model.DocumentData, model.DocumentTagDef)
        .join(
            model.DocumentTagDef,
            model.DocumentData.documentTagDefID
            == model.DocumentTagDef.idDocumentTagDef,
        )
        .filter(model.DocumentTagDef.idDocumentModel == docModel)
        .filter(model.DocumentData.documentID == inv_id)
        .all()
    )

    docHdrDt = {}
    tagNames = {}

    for document_data, document_tag_def in DocDtHdr:
        docHdrDt[document_tag_def.TagLabel] = document_data.Value
        tagNames[document_tag_def.TagLabel] = document_tag_def.idDocumentTagDef
    currency_code = ""
    freight_charges = 0
    misc_amt = 0
    if "InvoiceTotal" in docHdrDt:
        if crt_ck_status == 1:
            invo_total = crd_clean_amount(docHdrDt["InvoiceTotal"])
        else:
            invo_total = clean_amount(docHdrDt["InvoiceTotal"])
        if payload_subtotal == "":
            if "SubTotal" in docHdrDt:
                if crt_ck_status == 1:
                    invo_SubTotal = crd_clean_amount(docHdrDt["SubTotal"])
                else:
                    invo_SubTotal = clean_amount(docHdrDt["SubTotal"])
            else:
                if "GST" in docHdrDt:
                    if crt_ck_status == 1:
                        invo_SubTotal = crd_clean_amount(
                            docHdrDt["InvoiceTotal"]
                        ) - crd_clean_amount(docHdrDt["GST"])
                    else:
                        invo_SubTotal = clean_amount(
                            docHdrDt["InvoiceTotal"]
                        ) - clean_amount(docHdrDt["GST"])
                elif "TotalTax" in docHdrDt:
                    if crt_ck_status == 1:
                        invo_SubTotal = crd_clean_amount(
                            docHdrDt["InvoiceTotal"]
                        ) - crd_clean_amount(docHdrDt["TotalTax"])
                    else:
                        invo_SubTotal = clean_amount(
                            docHdrDt["InvoiceTotal"]
                        ) - clean_amount(docHdrDt["TotalTax"])
                else:
                    invo_SubTotal = invo_total
        else:
            if crt_ck_status == 1:
                invo_SubTotal = crd_clean_amount(payload_subtotal)
            else:
                invo_SubTotal = clean_amount(payload_subtotal)

    else:
        voucher_data_status = 0
    if "InvoiceDate" in docHdrDt:
        invo_Date = docHdrDt["InvoiceDate"]
    else:
        voucher_data_status = 0
    if "InvoiceId" in docHdrDt:
        invo_ID = docHdrDt["InvoiceId"]
    else:
        voucher_data_status = 0

    if "FreightCharges" in docHdrDt:
        freight_charges = clean_amount(docHdrDt["FreightCharges"])
    # try:
    if "Currency" in docHdrDt:
        currency_code_rw = docHdrDt["Currency"]

        isCurrencyMatch = validate_currency(inv_id, currency_code_rw, db)

        if isCurrencyMatch:
            currency_code = currency_code_rw
        else:
            currency_code = "CAD"
    else:
        currency_code = "CAD"
    #         isCurrencyMatch = validate_currency(
    #             inv_id, currency_code_rw, db
    #         )

    #         if isCurrencyMatch:
    #             currency_code = currency_code_rw

    #         else:
    #             currency_code = ""
    #     else:
    #         currency_code = ""
    # except Exception:
    #     currency_code = ""

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
        account_number = (
            db.query(model.Vendor.account)
            .filter(model.Vendor.VendorCode == VENDOR_ID)
            .first()
        )
        account_no = account_number[0]
        # Check if there are multiple accounts
        if account_no and "," in account_no:
            # Extract the first account
            account = account_no.split(",")[0]
        elif account_no:
            # If only one account exists, assign it
            account = account_no
        else:
            # If account is None or empty, set it as empty
            account = ""

    else:
        VENDOR_ID = ""
        account = ""

    InvStmDt = (
        db.query(model.StampDataValidation)
        .filter(model.StampDataValidation.documentid == inv_id)
        .all()
    )
    stmpData = {}
    for stDt in InvStmDt:
        stmpData[stDt.stamptagname] = stDt.stampvalue
    stmpDept = stmpData["Department"]
    storeType = stmpData["StoreType"]
    if "StoreNumber" in stmpData:
        storeNumber = stmpData["StoreNumber"]
        if len(str(storeNumber)) == 3:
            storeNumber = "0" + str(storeNumber)
        elif len(str(storeNumber)) == 2:
            storeNumber = "00" + str(storeNumber)
    elif "storenumber" in stmpData:
        storeNumber = stmpData["StoreNumber"]
        if len(str(storeNumber)) == 3:
            storeNumber = "0" + str(storeNumber)
        elif len(str(storeNumber)) == 2:
            storeNumber = "00" + str(storeNumber)
    else:
        voucher_data_status = 0

    # Determine ACCOUNT based on account existence or emptiness and selected department
    itmSelected = stmpData["SelectedDept"]
    if itmSelected == "Inventory":
        ACCOUNT = "14100"
    elif itmSelected == "Supplies":
        # Determine ACCOUNT based on account existence or emptiness
        if " " in str(account):
            account = account.replace(" ","")
        if account and len(str(account))>3:
            ACCOUNT = account
        else:
            ACCOUNT = "71999"
    else:
        ACCOUNT = ""

    if voucher_data_status == 1:
        try:
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
                if dpt_cd_dt:
                    for department in dpt_cd_dt:
                        DEPTID = department.DEPTID
                        VENDOR_SETID = department.SETID
                        BUSINESS_UNIT = ""
                        # ACCOUNT = "71999"
                else:
                    nonIntStatusMsg = "Department not found"
                    nonIntStatus = 0

            else:
                try:

                    dpt_cd_dt = (
                        db.query(model.PFGDepartment)
                        .filter(
                            or_(
                                model.PFGDepartment.DESCR.in_([stmpDept]),
                                model.PFGDepartment.DESCRSHORT == stmpDept,
                                model.PFGDepartment.DESCRSHORT.ilike(f"%{stmpDept}%"),
                            )
                        )
                        .all()
                    )
                    if dpt_cd_dt:
                        for department in dpt_cd_dt:
                            DEPTID = department.DEPTID
                            VENDOR_SETID = department.SETID
                            BUSINESS_UNIT = ""
                            # ACCOUNT = "71999"
                    else:
                        nonIntStatusMsg = "Department not found"
                        nonIntStatus = 0

                        # ACCOUNT = "71999"
                except SQLAlchemyError:
                    db.rollback()
        except Exception:
            logger.debug(f" {traceback.format_exc()}")
            nonIntStatusMsg = "Error in Department Matching"
            nonIntStatus = 0

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
        try:
            if gst_amt > 0:
                vat_applicability = 'T'
            else:
                vat_applicability = 'O'
        except Exception:
            logger.debug(traceback.format_exc())   
            vat_applicability = 'O' 
        
        try:
            get_tmid_qry = (
                db.query(model.User)
                .filter(model.User.idUser == userID, model.User.employee_id.isnot(None))
                .first()
            )

            if get_tmid_qry:
                employee_id = get_tmid_qry.employee_id
            else:
                employee_id = None  # Explicitly set to None when no matching record

        except Exception as e:
            logger.error(f"Error in getting employee_id: {e}")  # Use error level for logging
            employee_id = None
            
        if nonIntStatus == 1:

            existing_record = (
                db.query(model.VoucherData).filter_by(documentID=inv_id).first()
            )

            if existing_record:
                # If record exists, update the existing record with new data
                existing_record.Business_unit = BUSINESS_UNIT
                existing_record.Invoice_Id = invo_ID
                existing_record.Invoice_Dt = invo_Date
                existing_record.Vendor_Setid = VENDOR_SETID
                existing_record.Vendor_ID = VENDOR_ID
                existing_record.Deptid = DEPTID
                existing_record.Account = ACCOUNT
                existing_record.Gross_Amt = invo_total
                existing_record.Merchandise_Amt = invo_SubTotal
                existing_record.File_Name = docPath.split("/")[-1]
                existing_record.Distrib_Line_num = 1
                existing_record.Voucher_Line_num = 1
                existing_record.Image_Nbr = 1
                existing_record.Origin = invoice_type
                existing_record.storenumber = storeNumber
                existing_record.storetype = storeType
                existing_record.receiver_id = "NA"
                existing_record.status = voucher_data_status
                existing_record.gst_amt = gst_amt
                existing_record.currency_code = currency_code
                existing_record.freight_amt = freight_charges
                existing_record.misc_amt = misc_amt
                existing_record.vat_applicability = vat_applicability
                existing_record.opr_id = employee_id
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
                    "Gross_Amt": invo_total,
                    "Merchandise_Amt": invo_SubTotal,
                    "File_Name": docPath.split("/")[-1],
                    "Distrib_Line_num": 1,
                    "Voucher_Line_num": 1,
                    "Image_Nbr": 1,
                    "Origin": invoice_type,
                    "storenumber": str(storeNumber),
                    "storetype": storeType,
                    "receiver_id": "NA",
                    "status": voucher_data_status,
                    "recv_ln_nbr": recvLineNum,
                    "gst_amt": gst_amt,
                    "currency_code": currency_code,
                    "freight_amt": freight_charges,
                    "misc_amt": misc_amt,
                    "vat_applicability": vat_applicability,
                    "opr_id": employee_id
                }
                VD_db_data = model.VoucherData(**VoucherData_insert_data)
                db.add(VD_db_data)

            # Commit the changes to the database
            db.commit()

    return nonIntStatus, nonIntStatusMsg


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


def pfg_sync(docID, userID, db: Session, customCall=0, skipCk=0):
    logger.info(f"pfg_sync start: docID: {docID}, userID: {userID}, customCall: {customCall}, skipCk: {skipCk}")
    
    if '2' in str(skipCk):
        zero_dollar = 1
    else:
        zero_dollar = 0
    if '3' in str(skipCk):
        skip_supplierCk = 1
    else:
        skip_supplierCk = 0
    if '1' in str(skipCk):
        skipConf = 1
    else:
        skipConf = 0
    if '4' in str(skipCk):
        approvalCk = 1
    else:
        approvalCk = 0
    invo_StatusCode = 0

    logger.info(f"start on the pfg_sync,DocID{docID}, skipVal: {skipConf}")

    docModel = (
        db.query(model.Document.documentModelID)
        .filter(model.Document.idDocument == docID)
        .scalar()
    )

    logger.info(f"docModel:{docModel}")

    tagDef = (
        db.query(model.DocumentTagDef)
        .filter(model.DocumentTagDef.idDocumentModel == docModel)
        .all()
    )

    tagNames = {}

    for document_tag_def in tagDef:
        tagNames[document_tag_def.TagLabel] = document_tag_def.idDocumentTagDef

    logger.info(f"tagNames: {tagNames}")
    CreditNote = "Invoice Document"
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
    InvodocStatus = 0
    InvodocStatus_bu = 0
    fileSizeThreshold = 10
    confirmation_ck = 0
    confirmation_ck_msg = ""
    row_count = 0
    # StrTyp_Err = 0
    gst_amt = 0
    tax_isErr = 0
    documentModelID = ""
    otrChgsCk = 0
    credit_note = 0
    blank_id = 0
    vdrAccId = ""
    vrdNm = ""
    crdVal_ck = 0
    strbucks = 0
    credit_found = 0
    gst_found = 0
    amt_threshold = 10000
    otrCrg_ck_zdr = 0
    otrTax_total = 0
    invoSubStatus = 0
    ocr_msg = "Please review invoice details."
    try:
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
        OtherChargesList = [
            "PST",
            "HST",
            "LitterDeposit",
            "BottleDeposit",
            "Discount",  # noqa: E501
            "FreightCharges",
            "Fuel surcharge",
            "EnviroFees",  # noqa: E501
            "Credit_Card_Surcharge",
            "Deposit",
            "EcoFees",  # noqa: E501
            "OtherCharges",
            "Other Credit Charges",
            "ShipmentCharges",  # noqa: E501
            "TotalDiscount",
            "Usage Charges",
            "Miscellaneous",
        ]  # noqa: E501

        tab_ck_list = ["Quantity", "UnitPrice", "Amount", "AmountExcTax"]

        docTb = (
            db.query(model.Document).filter(model.Document.idDocument == docID).all()
        )

        for dtb_rw in docTb:
            InvodocStatus = dtb_rw.documentStatusID
            InvodocStatus_bu = dtb_rw.documentStatusID
            invoSubStatus = dtb_rw.documentsubstatusID
            filePath = dtb_rw.docPath
            invID_docTab = dtb_rw.docheaderID
            vdrAccID = dtb_rw.vendorAccountID
            documentModelID = dtb_rw.documentModelID
            vdrAccId = dtb_rw.vendorAccountID
        logger.info(
            f"docID: {docID}, InvodocStatus:{InvodocStatus},"
            + "filePath:{filePath},"
            + "invID_docTab:{invID_docTab},"
            + "vdrAccID:{vdrAccID}"
        )

    except Exception as e:
        logger.error(f"docID: {docID},{str(e)}")

    try:
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
                    "response": ["Invoice sent to peopleSoft"],
                }
                return docStatusSync
        elif InvodocStatus == 10 and invoSubStatus == 13:
            # if invoSubStatus == 13:
            docStatusSync["Rejected"] = {
                "status": 1,
                "StatusCode":0,
                "response": ["Invoice rejected by user"],
            }
            return docStatusSync
        elif InvodocStatus == 33:
            docStatusSync["Custom model mapping required"] = {
                "status": 0,
                "StatusCode":0,
                "response": [],
            }
            if invoSubStatus==145:
                docStatusSync["Custom model mapping required"]["response"] = ["No active model found"]
            elif invoSubStatus ==144:
                docStatusSync["Custom model mapping required"]["response"] = ["Model mapping failed"]
            elif invoSubStatus ==135:
                docStatusSync["Custom model mapping required"]["response"] = ["Model not found in DI subscription"]
            else:
                docStatusSync["Custom model mapping required"]["response"] = ["Custom model mapping required"]
                
            return docStatusSync
        else:
            if skipConf==1:
                documentdesc = "Confirmation validations were bypassed by the user."
                update_docHistory(
                    docID, userID, InvodocStatus, documentdesc, db,invoSubStatus
                )
            if zero_dollar==1:
                documentdesc = "Zero-dollar invoice approved by the user."
                update_docHistory(
                    docID, userID, InvodocStatus, documentdesc, db,invoSubStatus
                )
            if skip_supplierCk==1:
                documentdesc = "Supplier ID mismatch approved by the user."
                update_docHistory(
                    docID, userID, InvodocStatus, documentdesc, db, invoSubStatus
                )
            if approvalCk==1:
                documentdesc = f"Amount Approved by the user."
                update_docHistory(
                    docID, userID, InvodocStatus, documentdesc, db, invoSubStatus
                )

    except Exception:
        logger.error(f"docID: {docID},{traceback.format_exc()}")
    
   

    docTb = (
        db.query(model.Document).filter(model.Document.idDocument == docID).all()
    )

    for dtb_rw in docTb:
        InvodocStatus = dtb_rw.documentStatusID
        InvodocStatus_bu = dtb_rw.documentStatusID
        invoSubStatus = dtb_rw.documentsubstatusID
        filePath = dtb_rw.docPath
        invID_docTab = dtb_rw.docheaderID
        vdrAccID = dtb_rw.vendorAccountID
        documentModelID = dtb_rw.documentModelID
        vdrAccId = dtb_rw.vendorAccountID

    try: 
        hd_tags_qry = (
                db.query(model.DocumentTagDef).filter(model.DocumentTagDef.idDocumentModel == documentModelID).all()
            )
        tag_id_mod = {}
        for idDM in hd_tags_qry:
            tag_id_mod[idDM.idDocumentTagDef] = idDM.TagLabel
            
        DocDtHdr = (
                db.query(model.DocumentData, model.DocumentTagDef)
                .join(
                    model.DocumentTagDef,
                    model.DocumentData.documentTagDefID
                    == model.DocumentTagDef.idDocumentTagDef,
                )
                # .filter(model.DocumentTagDef.idDocumentModel == docModel)
                .filter(model.DocumentData.documentID == docID)
                .all()
            )


        docHdrDt = {}
        tagNames = {}
        dup_ck_sm = {}
        del_otherKeys = []
        tag_dup_ck = []
        for document_data, document_tag_def in DocDtHdr:
            
            if document_data.documentTagDefID in tag_id_mod.keys():
                dup_ck_sm[document_data.idDocumentData] = document_tag_def.TagLabel
                tag_dup_ck.append(document_tag_def.TagLabel)
                docHdrDt[document_tag_def.TagLabel] = document_data.Value
                tagNames[document_tag_def.TagLabel] = document_tag_def.idDocumentTagDef
            else:
                del_otherKeys.append(document_data.idDocumentData)
        seen = {}
        filtered_data = {}

        # Sort in ascending order to keep the smallest key and remove higher ones
        for key in sorted(dup_ck_sm):
            value = dup_ck_sm[key]
            if value not in seen:
                seen[value] = key  # Store the first occurrence (smallest key)
                filtered_data[key] = value
            else:
                del_otherKeys.append(key)  # Track deleted keys

        logger.info("docID: {docID} - Filtered Data: {filtered_data}")
        logger.info("docID: {docID} - Deleted Keys: {del_otherKeys}")

        # Delete related records in DocumentUpdates
        db.query(model.DocumentUpdates).filter(
            model.DocumentUpdates.documentDataID.in_(del_otherKeys)
        ).delete(synchronize_session=False)

        # Now delete the records in DocumentData
        db.query(model.DocumentData).filter(
            model.DocumentData.idDocumentData.in_(del_otherKeys)
        ).delete(synchronize_session=False)

        # Commit the transaction
        db.commit()

        DocDtHdr = (
                db.query(model.DocumentData, model.DocumentTagDef)
                .join(
                    model.DocumentTagDef,
                    model.DocumentData.documentTagDefID
                    == model.DocumentTagDef.idDocumentTagDef,
                )
                # .filter(model.DocumentTagDef.idDocumentModel == docModel)
                .filter(model.DocumentData.documentID == docID)
                .all()
            )

        docHdrDt = {}
        tagNames = {}

        for document_data, document_tag_def in DocDtHdr:

            docHdrDt[document_tag_def.TagLabel] = document_data.Value
            tagNames[document_tag_def.TagLabel] = document_tag_def.idDocumentTagDef
        

    except Exception:
        logger.error(f"docID: {docID} - {traceback.format_exc()}")
    
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
    logger.info(f"docID: {docID} -docHdrDt: {docHdrDt}")
    logger.info(f"docID: {docID} - tagNames: {tagNames}")

    try:
        if "InvoiceId" in docHdrDt:
            if invID_docTab !=docHdrDt["InvoiceId"]:

                db.query(model.Document).filter(
                                model.Document.idDocument == docID,
                ).update({model.Document.docheaderID: docHdrDt["InvoiceId"]})
                db.commit()
                invID_docTab = docHdrDt["InvoiceId"]
                logger.info(f"docID: {docID} - updated docHeader: docID: {docID} invID_docTab: {invID_docTab}")
    except Exception:
        logger.info(f"docID: {docID} - exception: {invID_docTab}")
    

    #-------------
    try:
        # missing_val = []

        # if "GST" in docHdrDt:
        #     gst_found = 1
        # else:
        #     missing_val.append("GST")
        #     gst_found = 0
        # if "Credit Identifier" in docHdrDt:
        #     credit_found = 1
        # else:
        #     missing_val.append("Credit Identifier")
        #     credit_found = 0
        # if missing_val:
        #     existing_tags = (
        #         db.query(model.DocumentTagDef.TagLabel)
        #         .filter(
        #             model.DocumentTagDef.idDocumentModel == documentModelID,
        #             model.DocumentTagDef.TagLabel.in_(
        #                 ["Credit Identifier"]
        #             ),
        #         )
        #         .all()
        #     )

        #     # Extract existing tag labels from the result
        #     existing_tag_labels = {tag.TagLabel for tag in existing_tags}

        #     # Prepare missing tags
        #     missing_tags = []
        #     if "Credit Identifier" not in existing_tag_labels:
        #         missing_tags.append(
        #             model.DocumentTagDef(
        #                 idDocumentModel=documentModelID,
        #                 TagLabel="Credit Identifier",
        #                 CreatedOn=func.now(),
        #             )
        #         )

        #     if "GST" not in existing_tag_labels:
        #         missing_tags.append(
        #             model.DocumentTagDef(
        #                 idDocumentModel=documentModelID,
        #                 TagLabel="GST",
        #                 CreatedOn=func.now(),
        #             )
        #         )

        #     if missing_tags:
        #         # db.add_all(missing_tags)
        #         # db.commit()
        #         logger.info("Missing Tags Inserted")
        custHdrDt_insert_missing=[]
        # documenttagdef = (
        #     db.query(model.DocumentTagDef)
        #     .filter(model.DocumentTagDef.idDocumentModel == documentModelID)
        #     .all()
        # )
        
        try:
            missing_val = []

            # Check if "GST" and "Credit Identifier" exist in docHdrDt
            gst_found = int("GST" in docHdrDt)
            credit_found = int("Credit Identifier" in docHdrDt)

            if not gst_found:
                missing_val.append("GST")
            if not credit_found:
                missing_val.append("Credit Identifier")

            if missing_val:
                # Fetch existing tag labels correctly
                existing_tags = (
                    db.query(model.DocumentTagDef.TagLabel)
                    .filter(
                        model.DocumentTagDef.idDocumentModel == documentModelID,
                        model.DocumentTagDef.TagLabel.in_(missing_val),  # Check only missing ones
                    )
                    .all()
                )

                # Convert existing tags to a set for quick lookup
                existing_tag_labels = {tag[0] for tag in existing_tags}  # Fetch as tuples, take first value

                # Prepare only truly missing tags
                missing_tags = [
                    model.DocumentTagDef(
                        idDocumentModel=documentModelID,
                        TagLabel=tag,
                        CreatedOn=func.now(),
                    )
                    for tag in missing_val if tag not in existing_tag_labels  # Avoid inserting duplicates
                ]

                # Insert missing tags only if required
                if missing_tags:
                    db.add_all(missing_tags)
                    db.commit()

            # Fetch all tags for the document
            documenttagdef = (
                db.query(model.DocumentTagDef)
                .filter(model.DocumentTagDef.idDocumentModel == documentModelID)
                .all()
            )
        except Exception:
            logger.error(f"docID: {docID} - {traceback.format_exc()}")


        hdr_tags = {}
        for hdrTags in documenttagdef:
            hdr_tags[hdrTags.TagLabel] = hdrTags.idDocumentTagDef
        if "Credit Identifier" not in docHdrDt:
            try:
                custHdrDt_insert_missing.append(
                                        {
                                            "documentID": docID,
                                            "documentTagDefID": hdr_tags["Credit Identifier"],
                                            "Value": "Invoice Document",
                                            "IsUpdated": 0,
                                            "isError": 0,
                                            "ErrorDesc": "Defaulting to Invoice Document",
                                        }
                                    )
            except Exception:
                logger.error(f"docID: {docID} - {traceback.format_exc()}")

        if "GST" not in docHdrDt:
            try:
                custHdrDt_insert_missing.append(
                                        {
                                            "documentID": docID,
                                            "documentTagDefID": hdr_tags["GST"],
                                            "Value": 0,
                                            "IsUpdated": 0,
                                            "isError": 0,
                                            "ErrorDesc": "Defaulting to 0",
                                        }
                                    )
            except Exception:
                logger.error(f"docID: {docID} - {traceback.format_exc()}")

        # add missing values to the invoice data:
        for entry in custHdrDt_insert_missing:
            new_record = model.DocumentData(
                documentID=entry["documentID"],
                documentTagDefID=entry["documentTagDefID"],
                Value=entry["Value"],
                IsUpdated=entry["IsUpdated"],
                isError=entry["isError"],
                ErrorDesc=entry["ErrorDesc"],
            )

            db.add(new_record)

        try:
            db.commit()
        except Exception as err:
            logger.debug(f"docID: {docID} - ErrorUpdatingPostingData: {err}")

    except Exception:
        logger.error(f"docID: {docID} - {traceback.format_exc()}")
    #-------------

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
                "response": ["Invoice sent to peopleSoft"],
            }
    elif InvodocStatus == 10 and invoSubStatus == 13:
        # if invoSubStatus == 13:
        docStatusSync["Rejected"] = {
            "status": 1,
            "StatusCode":0,
            "response": ["Invoice rejected by user"],
        }

    else:

        try:

            try:
                if invID_docTab == None:
                    blank_id = 1
                elif invID_docTab.lower() == "na":
                    blank_id = 1
                else:
                    cln_invID = re.sub(r"[^a-zA-Z0-9\s]", "", invID_docTab)
                    if len(cln_invID) == 0:
                        blank_id = 1
                if blank_id == 1:
                    InvodocStatus = 4
                    invoSubstatus = 142
                    try:
                        db.query(model.Document).filter(
                            model.Document.idDocument == docID
                        ).update(
                            {
                                model.Document.documentStatusID: InvodocStatus,  # noqa: E501
                                model.Document.documentsubstatusID: invoSubstatus,  # noqa: E501
                            }
                        )
                        db.commit()
                    except Exception as err:
                        logger.debug(f"ErrorUpdatingPostingData: {err}")

                    docStatusSync["Invoice ID"] = {
                        "status": 0,
                        "StatusCode":0,
                        "response": ["Invoice ID not found"],
                    }
                    return docStatusSync

                if cln_invID != invID_docTab:
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

                    # docHdrDt = {}
                    # tagNames = {}

                    for document_data, document_tag_def in DocDtHdr:
                        docHdrDt[document_tag_def.TagLabel] = document_data.Value
                        tagNames[document_tag_def.TagLabel] = (
                            document_tag_def.idDocumentTagDef
                        )
                        docInvoIdTag = tagNames["InvoiceId"]
                        db.query(model.DocumentData).filter(
                            model.DocumentData.documentID == docID,
                            model.DocumentData.documentTagDefID == docInvoIdTag,
                        ).update({model.DocumentData.Value: cln_invID})
                        db.commit()

                        db.query(model.Document).filter(
                            model.Document.idDocument == docID,
                        ).update({model.Document.docheaderID: cln_invID})
                        db.commit()
                        invID_docTab = cln_invID
            except Exception:
                logger.error(f"docID: {docID} - {traceback.format_exc()}")

            #

            # docTb_docHdr_count = (
            #     db.query(model.Document)
            #     .filter(
            #         model.Document.docheaderID == invID_docTab,
            #         model.Document.vendorAccountID == vdrAccID,
            #         or_(
            #             model.Document.documentStatusID not in (10),  # First condition
            #             and_(
            #                 model.Document.documentStatusID ==32 ,  # Second condition
            #                 model.Document.idDocument == docID,
            #             ),
            #         ),
            #     )
            #     .count()
            # )
            docTb_docHdr_count = (
                db.query(model.Document)
                .filter(
                    model.Document.docheaderID == invID_docTab,
                    model.Document.vendorAccountID == vdrAccID,
                    model.Document.documentStatusID.notin_(
                        (10, 0)
                    ),  # Filter for statuses not in (10, 0)
                )
                .count()
            )

            if docTb_docHdr_count > 1:
                InvodocStatus = 32
                invoSubstatus = 128
                try:
                    db.query(model.Document).filter(
                        model.Document.idDocument == docID
                    ).update(
                        {
                            model.Document.documentStatusID: InvodocStatus,  # noqa: E501
                            model.Document.documentsubstatusID: invoSubstatus,  # noqa: E501
                        }
                    )
                    db.commit()
                except Exception as err:
                    logger.debug(f"docID: {docID} - ErrorUpdatingPostingData: {err}")

                # logger.error(f"Duplicate Document Header ID: {invID_docTab}")
            else:
                InvodocStatus = 4
                if InvodocStatus_bu != 4:
                    InvodocStatus = 4
                    invoSubstatus = 27
                    try:
                        db.query(model.Document).filter(
                            model.Document.idDocument == docID
                        ).update(
                            {
                                model.Document.documentStatusID: InvodocStatus,  # noqa: E501
                                model.Document.documentsubstatusID: invoSubstatus,  # noqa: E501
                            }
                        )
                        db.commit()
                    except Exception as err:
                        logger.debug(f"docID: {docID} - ErrorUpdatingPostingData: {err}")

            print(f"Count of rows: {docTb_docHdr_count}")
        except Exception as e:
            logger.debug(f"docID: {docID} - {str(e)}")

        if InvodocStatus == 32:
            duplicate_status_ck = 0
            duplicate_status_ck_msg = "Invoice already exists"
            docStatusSync["Invoice duplicate check"] = {
                "status": duplicate_status_ck,
                "StatusCode":0,
                "response": [duplicate_status_ck_msg],
            }
            return docStatusSync
        elif InvodocStatus == 10:
            duplicate_status_ck = 0
            duplicate_status_ck_msg = "Invoice rejected by user"
            docStatusSync["Invoice rejected"] = {
                "status": duplicate_status_ck,
                "StatusCode":0,
                "response": [duplicate_status_ck_msg],
            }
            return docStatusSync
        elif InvodocStatus == 0:
            duplicate_status_ck = 0
            duplicate_status_ck_msg = "InvoiceId not found"
            docStatusSync["Invoice duplicate check"] = {
                "status": duplicate_status_ck,
                "StatusCode":0,
                "response": [duplicate_status_ck_msg],
            }
        elif isinstance(InvodocStatus, int) and InvodocStatus != 10:
            duplicate_status_ck = 1
            duplicate_status_ck_msg = "Duplicate check success"
            docStatusSync["Invoice duplicate check"] = {
                "status": duplicate_status_ck,
                "StatusCode":0,
                "response": [duplicate_status_ck_msg],
            }
        try:
            if (documentModelID == 999999 and vdrAccID != 0):
                
                InvodocStatus = 33
                invoSubstatus = 144
                try:
                    db.query(model.Document).filter(
                        model.Document.idDocument == docID
                    ).update(
                        {
                            model.Document.documentStatusID: InvodocStatus,  # noqa: E501
                            model.Document.documentsubstatusID: invoSubstatus,  # noqa: E501
                        }
                    )
                    db.commit()
                except Exception as err:
                    logger.debug(f"docID: {docID} - ErrorUpdatingPostingData: {err}")
                model_count = (
                    db.query(model.DocumentModel)
                    .filter(model.DocumentModel.idVendorAccount == vdrAccID)
                    .filter(model.DocumentModel.is_active == 1)
                    .order_by(
                        model.DocumentModel.UpdatedOn
                    )  # This line is optional; ordering isn't needed for a count
                    .count()
                )
                if model_count > 1:
                    # multiple active models found
                    docStatusSync["Status overview"] = {
                        "status": 0,
                        "StatusCode":0,
                        "response": [
                            "Multiple active models detected. Please combine the models and try again."  # noqa: E501
                        ],
                    }
                    return docStatusSync

                elif model_count == 0:
                    # no active models found
                    docStatusSync["Status overview"] = {
                        "status": 0,
                        "StatusCode":0,
                        "response": [
                            "No active models found. Please train the model to onboard the vendor"  # noqa: E501
                        ],
                    }
                    return docStatusSync

                else:
                    docStatusSync["Status overview"] = {
                        "status": 0,
                        "StatusCode":0,
                        "response": [
                            "Model unmapped, please remap the vendor."  # noqa: E501
                        ],
                    }
                    return docStatusSync

                    # customModelCall(docID)
                    # docTb = (
                    #     db.query(model.Document)
                    #     .filter(model.Document.idDocument == docID)
                    #     .all()
                    # )
                     # update dochistory table
                    # try:
                    #     custModelCall_msg =  "Custom Model Call done"
                    #     update_docHistory(
                    #         docID, userID, InvodocStatus,custModelCall_msg , db, invoSubStatus
                    #     )
                    # except Exception:
                    #     logger.debug(traceback.format_exc())

                    # for dtb_rw in docTb:
                    #     InvodocStatus = dtb_rw.documentStatusID
                    #     filePath = dtb_rw.docPath
                    #     invID_docTab = dtb_rw.docheaderID
                    #     vdrAccID = dtb_rw.vendorAccountID
                    #     documentModelID = dtb_rw.documentModelID
                    #     invoSubStatus = dtb_rw.documentsubstatusID
        except Exception:
            logger.error(f"docID: {docID} - {traceback.format_exc()}")

        if vdrAccID == 0:
            docStatusSync["Status overview"] = {
                "status": 0,
                "StatusCode":0,
                "response": ["Vendor mapping unsuccessful"],
            }
            return docStatusSync
        # ----------

        # ----------------
        if len(docHdrDt) > 0:
            VdrCk_tb = db.query(model.Vendor).filter_by(idVendor=vdrAccID).first()
            if VdrCk_tb:
                vrdNm = VdrCk_tb.VendorName
                if vrdNm == "STARBUCKS COFFEE CANADA INC":
                    strbucks = 1
            if duplicate_status_ck == 1:
                try:
                    update_docHistory(
                        docID, userID, InvodocStatus, duplicate_status_ck_msg, db,invoSubStatus
                    )
                except Exception:
                    logger.debug(traceback.format_exc())
                try:
                    InvStmDt = (
                        db.query(model.StampDataValidation)
                        .filter(model.StampDataValidation.documentid == docID)
                        .all()
                    )
                    stmpData = {}
                    for stDt in InvStmDt:
                        stmpData[stDt.stamptagname] = {stDt.stampvalue: stDt.is_error}

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
                                    dmsg = "currency match success"

                                else:
                                    dmsg = "Invoice currency invalid"

                            else:
                                dmsg = "No currency found in the OpenAI result"
                            logger.info(f"dmsg: {dmsg}")
                        except Exception:
                            logger.debug(f"Error occurred: {traceback.format_exc()}")
                        try:
                            if "Credit Identifier" in docHdrDt:

                                logger.info(f"stampdata: {stmpData}")  # noqa: E501
                                if "credit" in docHdrDt["Credit Identifier"].lower():
                                    if InvStmDt and len(stmpData) > 0:

                                        strCk_msg = []
                                        strCk = 0
                                        if "Credit Identifier" in stmpData:
                                            opnAi_crd_info = list(
                                                stmpData["Credit Identifier"].keys()
                                            )[
                                                0
                                            ]  # noqa: E501
                                            if "credit" in opnAi_crd_info.lower():

                                                credit_note = 1
                                                CreditNote = "Credit Note"
                                                update_crdVal = {}
                                                for crt_tg in docHdrDt:
                                                    if crt_tg in hdr_ck_list:
                                                        cngTgId = tagNames[crt_tg]
                                                        if len(str(cngTgId)) > 0:
                                                            update_crdVal[crt_tg] = (
                                                                docHdrDt[crt_tg]
                                                            )  # noqa: E501
                                                if len(update_crdVal) > 0:
                                                    for upd_tg in update_crdVal:
                                                        if (
                                                            str(update_crdVal[upd_tg])[
                                                                0
                                                            ]
                                                            != "-"
                                                        ):  # noqa: E501
                                                            if (
                                                                str(
                                                                    update_crdVal[
                                                                        upd_tg
                                                                    ]
                                                                )
                                                                == "0"
                                                            ):
                                                                update_crdVal[
                                                                    upd_tg
                                                                ] = str(
                                                                    update_crdVal[
                                                                        upd_tg
                                                                    ]
                                                                )
                                                            else:
                                                                update_crdVal[
                                                                    upd_tg
                                                                ] = "-" + str(
                                                                    update_crdVal[
                                                                        upd_tg
                                                                    ]
                                                                )  # noqa: E501
                                            else:

                                                InvodocStatus = 4
                                                invoSubstatus = 129
                                                documentdesc = (
                                                    "Please review document type."
                                                )
                                                try:
                                                    update_docHistory(
                                                        docID,
                                                        userID,
                                                        InvodocStatus,
                                                        documentdesc,
                                                        db,invoSubStatus,  # noqa: E501
                                                    )
                                                except Exception:
                                                    logger.debug(
                                                        f"{traceback.format_exc()}"
                                                    )

                                                try:
                                                    db.query(model.Document).filter(
                                                        model.Document.idDocument
                                                        == docID
                                                    ).update(
                                                        {
                                                            model.Document.documentStatusID: InvodocStatus,  # noqa: E501
                                                            model.Document.documentsubstatusID: invoSubstatus,  # noqa: E501
                                                        }
                                                    )
                                                    db.commit()
                                                except Exception as err:
                                                    logger.debug(
                                                        f"ErrorUpdatingPostingData: {err}"
                                                    )
                                                docStatusSync["Status overview"] = {
                                                    "status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                        "Please review document type."
                                                    ],
                                                }
                                                return docStatusSync

                                    else:
                                        InvodocStatus = 4
                                        invoSubstatus = 129
                                        documentdesc = "Please review document type."
                                        try:
                                            update_docHistory(
                                                docID,
                                                userID,
                                                InvodocStatus,
                                                documentdesc,
                                                db,invoSubStatus  # noqa: E501
                                            )
                                        except Exception:
                                            logger.debug(f"{traceback.format_exc()}")
                                        try:
                                            db.query(model.Document).filter(
                                                model.Document.idDocument == docID
                                            ).update(
                                                {
                                                    model.Document.documentStatusID: InvodocStatus,  # noqa: E501
                                                    model.Document.documentsubstatusID: invoSubstatus,  # noqa: E501
                                                }
                                            )
                                            db.commit()
                                        except Exception as err:
                                            logger.debug(
                                                f"ErrorUpdatingPostingData: {err}"
                                            )
                                        docStatusSync["Status overview"] = {
                                            "status": 0,
                                            "StatusCode":0,
                                            "response": [
                                                "Please review document type."
                                            ],
                                        }
                                        return docStatusSync
                                else:
                                    if "Credit Identifier" in stmpData:
                                        opnAi_crd_info = list(
                                            stmpData["Credit Identifier"].keys()
                                        )[0]
                                        if ("credit" in opnAi_crd_info.lower()) or (
                                            "NA" in opnAi_crd_info
                                        ):
                                            InvodocStatus = 4
                                            invoSubstatus = 129
                                            documentdesc = (
                                                "Please review document type."
                                            )
                                            try:
                                                update_docHistory(
                                                    docID,
                                                    userID,
                                                    InvodocStatus,
                                                    documentdesc,
                                                    db,invoSubStatus  # noqa: E501
                                                )
                                            except Exception:
                                                logger.debug(
                                                    f"{traceback.format_exc()}"
                                                )

                                            try:
                                                # Updating the document's status and substatus
                                                db.query(model.Document).filter(
                                                    model.Document.idDocument == docID
                                                ).update(
                                                    {
                                                        model.Document.documentStatusID: InvodocStatus,
                                                        model.Document.documentsubstatusID: invoSubstatus,
                                                    }
                                                )
                                                db.commit()

                                            except Exception as err:
                                                db.rollback()
                                                logger.debug(
                                                    f"ErrorUpdatingPostingData: {err}"
                                                )
                                            docStatusSync["Status overview"] = {
                                                "status": 0,
                                                "StatusCode":0,
                                                "response": [
                                                    "Please review document type."
                                                ],
                                            }
                                            return docStatusSync
                                    else:
                                        InvodocStatus = 4
                                        invoSubstatus = 129
                                        documentdesc = "Please review document type."
                                        try:
                                            update_docHistory(
                                                docID,
                                                userID,
                                                InvodocStatus,
                                                documentdesc,
                                                db,invoSubStatus  # noqa: E501
                                            )
                                        except Exception:
                                            logger.debug(f"{traceback.format_exc()}")

                                        try:
                                            # Updating the document's status and substatus
                                            db.query(model.Document).filter(
                                                model.Document.idDocument == docID
                                            ).update(
                                                {
                                                    model.Document.documentStatusID: InvodocStatus,
                                                    model.Document.documentsubstatusID: invoSubstatus,
                                                }
                                            )
                                            db.commit()

                                        except Exception as err:
                                            db.rollback()
                                            logger.debug(
                                                f"ErrorUpdatingPostingData: {err}"
                                            )
                                if credit_note == 0:
                                    # credit_note = 0
                                    update_crdVal = {}
                                    for crt_tg in docHdrDt:
                                        if crt_tg in hdr_ck_list:
                                            cngTgId = tagNames[crt_tg]
                                            if len(str(cngTgId)) > 0:
                                                update_crdVal[crt_tg] = docHdrDt[crt_tg]
                                    if len(update_crdVal) > 0:
                                        for upd_tg in update_crdVal:
                                            if update_crdVal[upd_tg]=="":
                                                update_crdVal[upd_tg] = "0.00"
                                                

                                            elif str(update_crdVal[upd_tg])[0] == "-":
                                                update_crdVal[upd_tg] = str(
                                                    update_crdVal[upd_tg]
                                                )[1:]

                                if len(update_crdVal) > 0:
                                    case_statement = case(
                                        [
                                            (
                                                model.DocumentData.documentTagDefID
                                                == tagNames[tag],
                                                value,
                                            )
                                            for tag, value in update_crdVal.items()
                                        ],
                                        else_=model.DocumentData.Value,  # Optional: Retain the current value if no match
                                    )

                                    # Perform the update query
                                    db.query(model.DocumentData).filter(
                                        model.DocumentData.documentID == docID,
                                        model.DocumentData.documentTagDefID.in_(
                                            tagNames[tag] for tag in update_crdVal
                                        ),
                                    ).update(
                                        {model.DocumentData.Value: case_statement},
                                        synchronize_session=False,
                                    )

                                    # Commit the transaction
                                    db.commit()
                        except Exception:
                            logger.info(f" Error occurred: {traceback.format_exc()}")

                            invTotalMth = 0
                            invTotalMth_msg = "Invoice total mismatch, please review."                                                                                                      

                        if credit_note == 1:
                            if "PST" in docHdrDt:
                                pst = crd_clean_amount(docHdrDt["PST"])
                                if (pst is not None) and pst < 0:
                                    otrCrg_ck_zdr = crd_clean_amount(otrCrg_ck_zdr + pst)
                            if "HST" in docHdrDt:
                                hst = crd_clean_amount(docHdrDt["HST"])
                                if (hst is not None) and hst > 0:
                                    otrCrg_ck_zdr = crd_clean_amount(otrCrg_ck_zdr + hst)
                            if "GST" in docHdrDt:
                                gst_amt = crd_clean_amount(docHdrDt["GST"])
                                if gst_amt is None:
                                    gst_amt = 0
                                else:
                                    otrCrg_ck_zdr = crd_clean_amount(otrCrg_ck_zdr + gst_amt)
                            elif "TotalTax" in docHdrDt:
                                gst_amt = crd_clean_amount(docHdrDt["TotalTax"])
                                if gst_amt is None:
                                    gst_amt = 0
                                else:
                                    otrCrg_ck_zdr = crd_clean_amount(otrCrg_ck_zdr + gst_amt)
                            else:
                                gst_amt = 0
                            if "InvoiceTotal" in docHdrDt:
                                invoTotal = crd_clean_amount(docHdrDt["InvoiceTotal"])
                                if (invoTotal is not None):
                                   
                                    if "SubTotal" in docHdrDt:
                                        subTotal = crd_clean_amount(
                                            docHdrDt["SubTotal"]
                                        )  # noqa: E501
                                        
                                        if subTotal is not None:
                                            if strbucks == 1:

                                                if "TotalTax" in docHdrDt:
                                                    total_tx = crd_clean_amount(
                                                        docHdrDt["TotalTax"]
                                                    )  # noqa: E501
                                                else:
                                                    total_tx = 0
                                                if gst_amt > total_tx:
                                                    docStatusSync[
                                                        "Status overview"
                                                    ] = {
                                                        "status": 0,
                                                        "StatusCode":0,
                                                        "response": [
                                                            "Total tax mismatch"
                                                        ],
                                                    }
                                                    return docStatusSync
                                                else:
                                                    if gst_amt <= total_tx:
                                                        subTotal = (
                                                            subTotal
                                                            + (
                                                                total_tx
                                                                - gst_amt
                                                            )
                                                        )
                                            subTl_gst_sm = crd_clean_amount(subTotal+gst_amt)
                                            # if (round(abs(subTl_gst_sm-invoTotal)),2) < 0.09:
                                            try:
                                                # Ensure values are extracted from tuples if needed
                                                subTl_gst_sm = subTl_gst_sm[0] if isinstance(subTl_gst_sm, tuple) else subTl_gst_sm
                                                invoTotal = invoTotal[0] if isinstance(invoTotal, tuple) else invoTotal

                                                # Ensure both values are floats
                                                subTl_gst_sm = float(subTl_gst_sm)
                                                invoTotal = float(invoTotal)

                                                # Now perform the calculation safely
                                                if round(abs(subTl_gst_sm - invoTotal), 2) < 0.09:

                                                    invTotalMth = 1
                                            except Exception:
                                                logger.info(f"docID: {docID} - Error occurred: {traceback.format_exc()}")


                                            # # if (gst_amt is not None) and abs(
                                            # #     gst_amt
                                            # # ) > 0:  # noqa: E501
                                            # #     gst_sm = crd_clean_amount(
                                            # #         subTotal + gst_amt
                                            # #     )
                                            # #     if gst_sm is not None:
                                            # #         if gst_sm == invoTotal:
                                            # #             invTotalMth = 1

                                            # #         elif (
                                            # #             round(
                                            # #                 abs(
                                            # #                     gst_sm - invoTotal
                                            # #                 ),  # noqa: E501
                                            # #                 2,
                                            # #             )
                                            # #             < 0.09
                                            # #         ):  # noqa: E501
                                            # #             invTotalMth = 1

                                            # #         else:
                                            # #             invTotalMth = 0
                                            # #             invTotalMth_msg = (
                                            # #                 "Invoice total mismatch"
                                            #             )
                                            # if tax_isErr == 0 and invTotalMth!=1:
                                            #     if invoTotal == subTotal:
                                            #         invTotalMth = 1
                                            #         gst_amt = 0
                                            #     elif (
                                            #         round(
                                            #             abs(invoTotal - subTotal), 2
                                            #         )  # noqa: E501
                                            #         < 0.09
                                            #     ):
                                            #         invTotalMth = 1
                                            if invTotalMth == 0:
                                                for othCrgs in OtherChargesList:
                                                    if othCrgs in docHdrDt:
                                                        othCrgs_amt = crd_clean_amount(
                                                            docHdrDt[othCrgs]
                                                        )
                                                        if othCrgs_amt is not None:
                                                            othCrgs_sm = (
                                                                crd_clean_amount(
                                                                    othCrgs_amt
                                                                    + subTotal
                                                                )
                                                            )
                                                            if othCrgs_sm is not None:
                                                                if (
                                                                    round(
                                                                        abs(
                                                                            othCrgs_sm
                                                                            - invoTotal
                                                                        ),
                                                                        2,
                                                                    )
                                                                    < 0.09
                                                                ):  # noqa: E501
                                                                    invTotalMth = 1
                                                                    otrChgsCk = 1
                                                                    break
                                                                elif (
                                                                    round(
                                                                        abs(
                                                                            (
                                                                                othCrgs_sm
                                                                                + gst_amt
                                                                            )
                                                                            - invoTotal
                                                                        ),
                                                                        2,
                                                                    )
                                                                    < 0.09
                                                                ):  # noqa: E501
                                                                    invTotalMth = 1
                                                                    otrChgsCk = 1
                                                                    break
                                        else:
                                            logger.info("subTotal: {}")
                                            invTotalMth = 0
                                            invTotalMth_msg = "Invalid invoice subtotal,Please review."
                                    else:
                                        subTotal = invoTotal - gst_amt
                                        logger.info(" crd subTotal: {subTotal}")
                                        invTotalMth = 1
                                        invTotalMth_msg = "Default invoice subtotal."
                                
                                else:
                                    invTotalMth = 0
                                    invo_StatusCode = 0
                                    invTotalMth_msg = "Invalid invoice total,Please review."
                            else:
                                invTotalMth = 0
                                invTotalMth_msg = "Invalid invoice total,Please review."
                            if invTotalMth==1:
                                if invoTotal==0:
                                    if zero_dollar == 1:
                                        invTotalMth = 1
                                        invo_StatusCode = 2
                                        invTotalMth_msg = "User approved Zero $ invoice."
                                        logger.info("Zero $ invoice approved")
                                    else:
                                        invTotalMth = 0
                                        invo_StatusCode = 2
                                        invTotalMth_msg = "Approval required for Zero $ invoice."
                                elif invoTotal >= amt_threshold:
                                    if approvalCk==1:
                                        invo_StatusCode = 4
                                        invTotalMth = 1
                                        invTotalMth_msg =  f"User approved invoice total"
                                        logger.info("Ammount approved")
                                    else:
                                        invTotalMth = 0
                                        invo_StatusCode = 4
                                        invTotalMth_msg =  f"Needs user approval,(Invoice total >= ${amt_threshold})"
                                else:
                                    invTotalMth = 1
                                    invTotalMth_msg = "Success"

                        else:

                            # TAX validations:
                            if "PST" in docHdrDt:
                                pst = clean_amount(docHdrDt["PST"])
                                if (pst is not None) and pst > 0:
                                    otrCrg_ck_zdr = clean_amount(otrCrg_ck_zdr + pst)
                            if "HST" in docHdrDt:
                                hst = clean_amount(docHdrDt["HST"])
                                if (hst is not None) and hst > 0:
                                    otrCrg_ck_zdr = clean_amount(otrCrg_ck_zdr + hst)
                            if "GST" in docHdrDt:
                                gst_amt = clean_amount(docHdrDt["GST"])
                                if gst_amt is None:
                                    gst_amt = 0
                                else:
                                    otrCrg_ck_zdr = clean_amount(otrCrg_ck_zdr + gst_amt)
                            elif "TotalTax" in docHdrDt:
                                gst_amt = clean_amount(docHdrDt["TotalTax"])
                                if gst_amt is None:
                                    gst_amt = 0
                                else:
                                    otrCrg_ck_zdr = clean_amount(otrCrg_ck_zdr + gst_amt)
                            else:
                                gst_amt = 0
                            
                            if "InvoiceTotal" in docHdrDt:
                                invoTotal = clean_amount(docHdrDt["InvoiceTotal"])
                                if (invoTotal is not None):
                                    try:
                                        if "SubTotal" in docHdrDt:
                                            subTotal = clean_amount(
                                                docHdrDt["SubTotal"]
                                            )  # noqa: E501

                                            if subTotal is not None:
                                                if strbucks == 1:

                                                    if "TotalTax" in docHdrDt:
                                                        total_tx = clean_amount(
                                                            docHdrDt["TotalTax"]
                                                        )  # noqa: E501
                                                        if gst_amt > total_tx:
                                                            docStatusSync[
                                                                "Status overview"
                                                            ] = {
                                                                "status": 0,
                                                                "StatusCode":0,
                                                                "response": [
                                                                    "Total tax mismatch"
                                                                ],
                                                            }
                                                            return docStatusSync
                                                        else:
                                                            if gst_amt < total_tx:
                                                                subTotal = (
                                                                    subTotal
                                                                    + (
                                                                        total_tx
                                                                        - gst_amt
                                                                    )
                                                                )

                                                gst_sm = clean_amount(
                                                    subTotal + gst_amt
                                                )
                                                
                                                if (
                                                    round(
                                                        abs(
                                                            gst_sm
                                                            - invoTotal
                                                        ),  # noqa: E501
                                                        2,
                                                    )
                                                    < 0.09
                                                ):  # noqa: E501
                                                    invTotalMth = 1

                                                else:
                                                    # tax_isErr = 1
                                                    invTotalMth = 0
                                                    invTotalMth_msg = "Invoicetotal mismatch"

                                                prv_othChg_sm = clean_amount(subTotal + gst_amt)
                                                for othCrgs in OtherChargesList:
                                                    
                                                    if othCrgs in docHdrDt:
                                                        othCrgs_cln = clean_amount(
                                                            docHdrDt[othCrgs]
                                                        )
                                                        
                                                        if othCrgs_cln is not None:
                                                            prv_othChg_sm = clean_amount(prv_othChg_sm + othCrgs_cln)
                                                            othCrgs_sm = (
                                                                clean_amount(
                                                                    othCrgs_cln
                                                                    + subTotal+ gst_amt
                                                                )
                                                            )
                                                            if (
                                                                othCrgs_sm
                                                                is not None
                                                            ):  
                                                                if (
                                                                    round(
                                                                        abs(
                                                                            othCrgs_sm
                                                                            - invoTotal
                                                                        ),
                                                                        2,
                                                                    )
                                                                    < 0.09
                                                                ):  # noqa: E501
                                                                    invTotalMth = (
                                                                        1
                                                                    )
                                                                    otrChgsCk = (
                                                                        1
                                                                    )
                                                                    break
                                                                elif (
                                                                    round(
                                                                        abs(
                                                                            (
                                                                                othCrgs_sm
                                                                                + gst_amt
                                                                            )
                                                                            - invoTotal
                                                                        ),
                                                                        2,
                                                                    )
                                                                    < 0.09
                                                                ):  # noqa: E501
                                                                    invTotalMth = (
                                                                        1
                                                                    )
                                                                    otrChgsCk = (
                                                                        1
                                                                    )
                                                                    break
                                                                elif (round(abs(prv_othChg_sm - invoTotal), 2) < 0.09):
                                                                    invTotalMth = (
                                                                        1
                                                                    )
                                                                    otrChgsCk = (
                                                                        1
                                                                    ) 
                                                                    break
                                                                else:
                                                                    if invTotalMth==0:
                                                                        invTotalMth_msg = "Invoice total mismatch"

                                            else:
                                                invTotalMth = 0
                                                invTotalMth_msg = (
                                                    "Invalid invoice subtotal,Please review."
                                                )
                                     
                                        else:
                                            subTotal = invoTotal - gst_amt
                                            invTotalMth = 1
                                            invTotalMth_msg = (
                                                "Skip total check: Subtotal Missing"
                                                )
                                    except Exception as e:
                                        logger.debug(traceback.format_exc())
                                        invTotalMth = 0
                                        invTotalMth_msg = "Please review invoice amount details."
                                else:
                                    invTotalMth = 0
                                    invTotalMth_msg = "Invalid invoice total."
                            else:
                                invTotalMth = 0
                                invTotalMth_msg = "Invalid invoice total."
                            if invTotalMth==1:
                                if invoTotal==0:
                                    if zero_dollar == 1:
                                        invTotalMth = 1
                                        invo_StatusCode = 2
                                        invTotalMth_msg = "User approved Zero $ invoice."
                                        logger.info("Zero $ invoice approved")
                                    else:
                                        invTotalMth = 0
                                        invo_StatusCode = 2
                                        invTotalMth_msg = "Approval required for Zero $ invoice."
                                if invoTotal >= amt_threshold:
                                    if approvalCk==1:
                                        invo_StatusCode = 4
                                        invTotalMth = 1
                                        invTotalMth_msg =  f"User approved invoice total"
                                        logger.info("Ammount approved")
                                    else:
                                        invTotalMth = 0
                                        invo_StatusCode = 4
                                        invTotalMth_msg =  f"Needs user approval,(Invoice total >= ${amt_threshold})"                  
                    except Exception as e:
                        logger.debug(traceback.format_exc())
                        invTotalMth = 0
                        invTotalMth_msg = "Invoice total mismatch"
                    
                    try:
                        date_string = docHdrDt.get(
                            "InvoiceDate", ""
                        )  # TODO: Unused variable
                        try:
                            if date_string is not None:

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
                            else:
                                formatted_date = date_string
                                dateCheck = 0
                                dateValCk = 0
                                dateCheck_msg = "Date missing, kindly review."
                            if (dateValCk == 1) and (formatted_date != date_string):
                                # updating formatted date string:
                                docDateTag = tagNames["InvoiceDate"]
                                try:
                                    db.query(model.DocumentData).filter(
                                        model.DocumentData.documentID == docID,
                                        model.DocumentData.documentTagDefID
                                        == docDateTag,
                                    ).update({model.DocumentData.Value: formatted_date})
                                    db.commit()

                                    db.query(model.Document).filter(
                                        model.Document.idDocument == docID,
                                    ).update(
                                        {model.Document.documentDate: formatted_date}
                                    )
                                    db.commit()
                                    # print(formatted_date)
                                    # print(date_string)

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

                    if len(invID_docTab) == 0:
                        ocr_msg = "No invoice number found"
                        ocrCheck = 0
                        ocrCheck_msg.append("No invoice number found")

                    elif dateCheck == 1:
                        ocrCheck = 1
                        ocr_msg = "Date validation success"
                        ocrCheck_msg.append("Date validation success")
                    else:
                        ocr_msg = "Failed to validate the invoice date."
                        ocrCheck = 0
                        ocrCheck_msg.append(dateCheck_msg)

                    if invTotalMth == 1:
                        totalCheck = 1
                        totalCheck_msg.append("Invoice total validation success")

                    else:
                        totalCheck_msg.append(invTotalMth_msg)
                        totalCheck = 0

                        try:
                            docInvoTotalTag = tagNames["InvoiceTotal"]
                            db.query(model.DocumentData).filter(
                                model.DocumentData.documentID == docID,
                                model.DocumentData.documentTagDefID == docInvoTotalTag,
                            ).update(
                                {
                                    model.DocumentData.isError: 1,
                                    model.DocumentData.ErrorDesc: invTotalMth_msg,
                                }
                            )
                            db.commit()

                        except Exception:
                            logger.debug(traceback.format_exc())
                    if totalCheck_msg==[""]:
                        totalCheck_msg.append("Invoice total mismatch.")
                    docStatusSync["OCR validations"] = {
                        "status": ocrCheck,
                        "StatusCode":0,
                        "response": ocrCheck_msg,
                    }

                    docStatusSync["Invoice total validation"] = {
                        "status": totalCheck,
                        "StatusCode":invo_StatusCode,
                        "response": totalCheck_msg,
                    }

                    if (
                        docStatusSync["OCR validations"]["status"] == 1
                        and docStatusSync["Invoice total validation"]["status"] == 1
                    ):

                        # -----------------------update document history table
                        documentstatus = 4
                        documentdesc = "OCR validations success"
                        try:
                            update_docHistory(
                                docID, userID, documentstatus, documentdesc, db,invoSubStatus
                            )
                        except Exception:
                            logger.error(traceback.format_exc())

                        # InvStmDt = (
                        #     db.query(model.StampDataValidation)
                        #     .filter(model.StampDataValidation.documentid == docID)
                        #     .all()
                        # )
                        # stmpData = {}
                        # for stDt in InvStmDt:
                        #     stmpData[stDt.stamptagname] = {
                        #         stDt.stampvalue: stDt.is_error
                        #     }
                        if InvStmDt and len(stmpData) > 0:
                            try:
                                if  "SelectedDept" in stmpData:
                                    itm_cat = list(stmpData["SelectedDept"].keys())[0]
                                    if itm_cat not in ("Inventory", "Supplies"):
                                        docStatusSync[
                                                                        "Status overview"
                                                                    ] = {
                                                                        "status": 0,
                                                                        "StatusCode":0,
                                                                        "response": [
                                                                            "Please select item category."
                                                                        ],
                                                                    }
                                        return docStatusSync
                                else:
                                    docStatusSync["Item category validation"] = {
                                    "status": 0,
                                    "StatusCode":0,
                                    "response": ["Please select item category."],
                                }
                            except Exception:
                                logger.debug(f"{traceback.format_exc()}")
                                docStatusSync["Item category validation"] = {
                                    "status": 0,
                                    "StatusCode":0,
                                    "response": ["Please select item category."],
                                }
                                    

                            # #---------------------
                            # itmSelected = stmpData["SelectedDept"]
                            # if itmSelected == "Inventory":
                            #     ACCOUNT = "14100"
                            # elif itmSelected == "Supplies":

                            # #-------------------

                            strCk_msg = []
                            strCk = 0
                            if "StoreNumber" in stmpData:
                                storeNum = list(stmpData["StoreNumber"].keys())[0]
                            elif "StoreNumber" in stmpData:
                                storeNum = list(stmpData["StoreNumber"].keys())[0]
                            else:
                                storeNum = ""
                            if "StoreType" in stmpData and len(storeNum) > 0:

                                # ---------------------
                                try:
                                    storenumber = str(
                                        "".join(filter(str.isdigit, str(storeNum)))
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
                                    if int(storenumber) in nonIntStr_number:
                                        store_type = "Non-Integrated"

                                    else:

                                        store_type = "Integrated"
                                except Exception:
                                    store_type = ""
                                    logger.debug(f"{traceback.format_exc()}")

                                if store_type == list(stmpData["StoreType"].keys())[0]:

                                    strCk = 1
                                    strCk_msg.append("Store type validatoin success")

                                elif store_type in [
                                    "Integrated",
                                    "Non-Integrated",
                                ]:
                                    # stmpData["storenumber"] = store_type
                                    stmpData["StoreType"] = {store_type: 0}
                                    db.query(model.StampDataValidation).filter(
                                        model.StampDataValidation.documentid == docID,
                                        model.StampDataValidation.stamptagname
                                        == "StoreType",
                                    ).update(
                                        {
                                            model.StampDataValidation.stampvalue: store_type  # noqa: E501
                                        }
                                    )
                                    db.commit()
                                    strCk = 1
                                    strCk_msg = ["Store type validatoin success"]
                                else:
                                    strCk = 0
                                    strCk_msg = ["Invalid Store Type"]

                                # #---------------------
                                # try:
                                #     if list(stmpData["StoreType"].keys())[0] in [
                                #         "Integrated",
                                #         "Non-Integrated",
                                #     ]:
                                #         strCk = 1
                                #         strCk_msg.append("Success")
                                #     else:
                                #         strCk = 0
                                #         strCk_msg.append("Invalid Store Type")
                                # except Exception:
                                #     logger.debug(traceback.format_exc)
                                #     strCk = 0
                                #     strCk_msg.append("Invalid Store Type")
                            else:
                                strCk = 0
                                strCk_msg.append(" Store Type Not Found")

                            docStatusSync["Storetype validation"] = {
                                "status": strCk,
                                "StatusCode":0,
                                "response": strCk_msg,
                            }

                            if docStatusSync["Storetype validation"]["status"] == 1:

                                documentstatus = 4
                                documentdesc = "Storetype validation Success"
                                try:
                                    update_docHistory(
                                        docID, userID, documentstatus, documentdesc, db,invoSubStatus
                                    )
                                except Exception:
                                    logger.debug(traceback.format_exc)
                                # subtotal for payload:
                                # if otrChgsCk == 1:
                                #     # if gst_amt !=0:
                                #     #     if (invoTotal - gst_amt) == othCrgs_sm:
                                #     payload_subtotal = othCrgs_sm
                                #         # else:


                                # elif "SubTotal" in docHdrDt:

                                #     if vrdNm == "STARBUCKS COFFEE CANADA INC":
                                #         payload_subtotal = subTotal
                                #     else:
                                #         payload_subtotal = docHdrDt["SubTotal"]
                                # else:
                                #     payload_subtotal = subTotal
                                try:
                                    payload_subtotal = invoTotal-gst_amt
                                    logger.info(f"invoTotal: {invoTotal}, gst_amt: {gst_amt}")
                                except Exception:
                                    logger.info(f"Exception- invoTotal: {invoTotal}, gst_amt: {gst_amt}")
                                                
                                    payload_subtotal = subTotal
                                    logger.debug(traceback.format_exc)

                                try:
                                    if (
                                        list(stmpData["StoreType"].keys())[0]
                                        == "Integrated"
                                        and skipConf == 1
                                    ):
                                        try:
                                            db.query(model.StampDataValidation).filter(
                                                model.StampDataValidation.documentid
                                                == docID,
                                                model.StampDataValidation.stamptagname
                                                == "ConfirmationNumber",
                                            ).update(
                                                {
                                                    model.StampDataValidation.skipconfig_ck: 1,  # noqa: E501
                                                }
                                            )
                                            db.commit()
                                        except Exception:
                                            logger.debug(traceback.format_exc())
                                        skipValidationCK, skipValidationStatusMsg = (
                                            nonIntegratedVoucherData(
                                                userID,
                                                docID,
                                                gst_amt,
                                                payload_subtotal,
                                                CreditNote,
                                                db,
                                            )
                                        )
                                        if skipValidationCK == 1:
                                            DeptCk = 1
                                            DeptCk_msg = [
                                                "User bypassed confirmation number validation"  # noqa: E501
                                            ]
                                        else:
                                            DeptCk = 0
                                            DeptCk_msg = [skipValidationStatusMsg]

                                            docStatusSync["Storetype validation"] = {
                                                "status": DeptCk,
                                                "StatusCode":0,
                                                "response": DeptCk_msg,
                                            }
                                        voucher_query = db.query(
                                            model.VoucherData
                                        ).filter(model.VoucherData.documentID == docID)
                                        row_count = voucher_query.count()
                                        NullVal = []
                                        VthChk = 0
                                        VthChk_msg = ""

                                    elif (
                                        list(stmpData["StoreType"].keys())[0]
                                        == "Integrated"
                                    ):

                                        # -------------------------------
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
                                                            "Valid confirmation number"
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
                                                    "Invalid confirmation number"
                                                )

                                        else:

                                            confirmation_ck = 0
                                            confirmation_ck_msg = (
                                                "Confirmation number not found"
                                            )

                                        # -----------------------------------

                                        strCk = 1
                                        strCk_msg = ["Store type validatoin success"]
                                        if confirmation_ck == 1:
                                            (
                                                intStatus,
                                                intStatusMsg,
                                                vdrMatchStatus,
                                                vdrStatusMsg,
                                            ) = IntegratedvoucherData(
                                                userID,
                                                docID,
                                                gst_amt,
                                                payload_subtotal,
                                                CreditNote,
                                                db,
                                            )

                                            if vdrMatchStatus == 0:
                                                if skip_supplierCk==1:

                                                    docStatusSync[
                                                        "Supplier ID validation"
                                                    ] = {
                                                        "status": 1,
                                                        "StatusCode":3,
                                                        "response": [vdrStatusMsg],
                                                    }
                                                else:
                                                    docStatusSync[
                                                        "Supplier ID validation"
                                                    ] = {
                                                        "status": 0,
                                                        "StatusCode":3,
                                                        "response": [vdrStatusMsg],
                                                    }
                                            if intStatus == 0:
                                                docStatusSync[
                                                    "Storetype validation"
                                                ] = {
                                                    "status": 0,
                                                    "response": [intStatusMsg],
                                                }

                                    if (
                                        list(stmpData["StoreType"].keys())[0]
                                        == "Non-Integrated"
                                    ):

                                        nonIntStatus, nonIntStatusMsg = (
                                            nonIntegratedVoucherData(
                                                userID,
                                                docID,
                                                gst_amt,
                                                payload_subtotal,
                                                CreditNote,
                                                db,
                                            )
                                        )
                                        if nonIntStatus == 1:
                                            DeptCk = 1
                                            DeptCk_msg = ["Department validation success"]
                                        else:
                                            DeptCk = 0
                                            DeptCk_msg = [nonIntStatusMsg]

                                            docStatusSync["Storetype validation"] = {
                                                "status": DeptCk,
                                                "StatusCode":0,
                                                "response": DeptCk_msg,
                                            }
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
                                if docStatusSync["Storetype validation"]["status"] == 0:
                                    VthChk = 0
                                    if isinstance(
                                        docStatusSync["Storetype validation"][
                                            "response"
                                        ],
                                        list,
                                    ):
                                        respMsg = docStatusSync["Storetype validation"][
                                            "response"
                                        ][
                                            0
                                        ]  # Safely access the first element
                                    elif isinstance(
                                        docStatusSync["Storetype validation"][
                                            "response"
                                        ],
                                        int,
                                    ):
                                        # Handle the case where response is an int
                                        respMsg = str(
                                            docStatusSync["Storetype validation"][
                                                "response"
                                            ]
                                        )
                                    else:
                                        respMsg = docStatusSync["Storetype validation"][
                                            "response"
                                        ]

                                    VthChk_msg = respMsg

                                elif row_count == 1:

                                    voucher_row = voucher_query.first()
                                    has_null_or_empty = False

                                    for column in model.VoucherData.__table__.columns:

                                        if (
                                            (store_type == "Non-Integrated")
                                            or skipConf == 1
                                        ) and (column.name == "Business_unit"):
                                            continue
                                        if column.name == "currency_code":
                                            continue

                                        value = getattr(voucher_row, column.name)
                                        if value is None or value == "":
                                            has_null_or_empty = True
                                            NullVal.append(column.name)
                                    # for column in model.VoucherData.__table__.columns:

                                    #     value = getattr(voucher_row, column.name)
                                    #     if value is None or value == "":
                                    #         has_null_or_empty = True
                                    #         NullVal.append(column.name)

                                    if has_null_or_empty:
                                        VthChk = 0
                                        VthChk_msg = (
                                            "Missing values:" + str(NullVal)[1:-1]
                                        )  # noqa: E501
                                    else:
                                        VthChk = 1
                                        VthChk_msg = "Stamp Data validation success"

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
                                    "StatusCode":0,
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
                                                "StatusCode":0,
                                                "response": ["File size validation Success"],
                                            }
                                        else:
                                            docStatusSync["File size check"] = {
                                                "status": 1,
                                                "StatusCode":0,
                                                "response": [],
                                            }
                                    else:
                                        docStatusSync["File size check"] = {
                                            "status": 0,
                                            "StatusCode":0,
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
                                            db,invoSubStatus,  # noqa: E501
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
                                        overAllstatus_msg = "Invoice Validation success"    
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
                                                            logger.info(f"RespCodeInt {docID}: {RespCodeInt}")
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
                                                                docStatus = 35
                                                                docSubStatus = 149

                                                            elif RespCodeInt == 406:
                                                                dmsg = (
                                                                    InvoiceVoucherSchema.FAILURE_INVOICE  # noqa: E501
                                                                )
                                                                docStatus = 35
                                                                docSubStatus = 148

                                                            elif RespCodeInt == 408:
                                                                dmsg = (
                                                                    InvoiceVoucherSchema.PAYLOAD_DATA_ERROR  # noqa: E501
                                                                )
                                                                docStatus = 4
                                                                docSubStatus = 146
                                                                
                                                            elif RespCodeInt == 409:
                                                                dmsg = (
                                                                    InvoiceVoucherSchema.BLOB_STORAGE_ERROR  # noqa: E501
                                                                )
                                                                docStatus = 4
                                                                docSubStatus = 147
                                                                
                                                            elif RespCodeInt == 422:
                                                                dmsg = (
                                                                    InvoiceVoucherSchema.FAILURE_PEOPLESOFT  # noqa: E501
                                                                )
                                                                docStatus = 35
                                                                docSubStatus = 150

                                                            elif RespCodeInt == 424:
                                                                dmsg = (
                                                                    InvoiceVoucherSchema.FAILURE_FILE_ATTACHMENT  # noqa: E501
                                                                )
                                                                docStatus = 35
                                                                docSubStatus = 151

                                                            elif RespCodeInt == 500:
                                                                dmsg = (
                                                                    InvoiceVoucherSchema.INTERNAL_SERVER_ERROR  # noqa: E501
                                                                )
                                                                docStatus = 21
                                                                docSubStatus = 152
                                                            
                                                            elif RespCodeInt == 104:
                                                                dmsg = (
                                                                    InvoiceVoucherSchema.FAILURE_CONNECTION_ERROR  # noqa: E501
                                                                )
                                                                docStatus = 21
                                                                docSubStatus = 143
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
                                                        logger.info(f"error docID: {docID} - No Http Response found")
                                                        dmsg = (
                                                            InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                                                        )
                                                        docStatus = 21
                                                        docSubStatus = 112
                                                        
                                                else:
                                                    logger.info(f"error docID: {docID} - No data found ppl dft response")
                                                    dmsg = (
                                                        InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                                                    )
                                                    docStatus = 21
                                                    docSubStatus = 112
                                            except Exception as err:
                                                logger.info(f"error docID: {docID} - No response")
                                                logger.debug(
                                                    f"PopleSoftResponseError: {traceback.format_exc()}"  # noqa: E501
                                                )
                                                dmsg = InvoiceVoucherSchema.FAILURE_COMMON.format_message(  # noqa: E501
                                                    err
                                                )
                                                docStatus = 21
                                                docSubStatus = 112

                                            try:
                                                logger.info(f"Updating the document status for doc_id:{docID}")
                                                db.query(model.Document).filter(
                                                    model.Document.idDocument == docID
                                                ).update(
                                                    {
                                                        model.Document.documentStatusID: docStatus,
                                                        model.Document.documentsubstatusID: docSubStatus,
                                                        model.Document.retry_count: case(
                                                            (model.Document.retry_count.is_(None), 1),  # If NULL, set to 1
                                                            else_=model.Document.retry_count + 1        # Otherwise, increment
                                                        ) if docStatus == 21 else model.Document.retry_count
                                                    }
                                                )
                                                db.commit()
                                                logger.info(f"Updated docStatus {docID}: {docStatus}")
                                            except Exception:
                                                logger.error(traceback.format_exc())
                                            try:

                                                update_docHistory(
                                                    docID, userID, docStatus, dmsg, db, docSubStatus
                                                )

                                            except Exception:
                                                logger.error(traceback.format_exc())
                                        except Exception as e:
                                            logger.info(f"error docID: {docID} - No response - failed")
                                            logger.debug(traceback.format_exc())
                                            dmsg = InvoiceVoucherSchema.FAILURE_COMMON.format_message(  # noqa: E501
                                                e
                                            )
                                            docStatus = 21
                                            docSubStatus = 112

                                        docStatusSync["Sent to PeopleSoft"] = {
                                            "status": SentToPeopleSoft,
                                            "StatusCode":0,
                                            "response": [dmsg],
                                        }

                                        try:
                                            db.query(model.Document).filter(
                                                model.Document.idDocument == docID
                                            ).update(
                                                {
                                                    model.Document.documentStatusID: docStatus,
                                                    model.Document.documentsubstatusID: docSubStatus,
                                                    model.Document.retry_count: case(
                                                        (model.Document.retry_count.is_(None), 1),  # If NULL, set to 1
                                                        else_=model.Document.retry_count + 1        # Otherwise, increment
                                                    ) if docStatus == 21 else model.Document.retry_count
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
                                                db,docSubStatus,  # noqa: E501
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
                                            db,documentSubstatus  # noqa: E501
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
                                        docID, userID, documentstatus, documentdesc, db,documentSubstatus,
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
                                "StatusCode":0,
                                "response": ["No Stamp Data Found"],
                            }
                            documentSubstatus = 118
                            documentstatus = 4
                            documentdesc = "No Stamp Data Found"
                            try:
                                update_docHistory(
                                    docID, userID, documentstatus, documentdesc, db,documentSubstatus
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
                        
                        if docStatusSync["OCR validations"]["status"] == 0:
                            documentdesc = ocr_msg
                        elif docStatusSync["Invoice total validation"]["status"] == 0:
                            documentdesc = "Invoice total validation failed"
                        else:
                            documentdesc = "OCR / invoice total validation failed"
                        try:
                            update_docHistory(
                                docID, userID, documentstatus, documentdesc, db,documentSubstatus
                            )
                        except Exception:

                            logger.error(f"{traceback.format_exc()}")
                            overAllstatus_msg = "Failed"

                except Exception as err:
                    logger.error(f"{traceback.format_exc()}")
                    docStatusSync = {}
                    overAllstatus = 0
                    overAllstatus_msg = f"SyncException:{err}"
            else:
                try:
                    update_docHistory(
                        docID, userID, InvodocStatus, duplicate_status_ck_msg, db,invoSubStatus
                    )
                except Exception as e:
                    logger.error(f"pfg_sync line 886: {str(e)}")
                overAllstatus_msg = "Failed"
        else:

            try:
                docHrd_msg = "No Header Data found"
                docHrd_status = 0
                update_docHistory(docID, userID, docHrd_status, docHrd_msg, db,invoSubStatus)
            except Exception:
                logger.debug(traceback)
            overAllstatus_msg = "Failed"

        if (overAllstatus == 1) and ("Sent to PeopleSoft" in docStatusSync):
            if docStatusSync["Sent to PeopleSoft"]["status"] == 0:
                overAllstatus = 0
                overAllstatus_msg = "Failed"

        docStatusSync["Status overview"] = {
            "status": overAllstatus,
            "StatusCode":0,
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
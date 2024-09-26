import json
import math
import os
import re
import sys
import traceback
from datetime import datetime
from itertools import groupby

import model
import requests
from fuzzywuzzy import fuzz, process
from routers import ERPapis
from session.session import get_db
from sqlalchemy import func, or_
from sqlalchemy.orm import Load, Session, load_only

sys.path.append("..")
po_tag_map = {
    "PurchQty": "Quantity",
    "Name": "Description",
    "PurchId": "PO_HEADER_ID",
    "Qty": "Quantity",
    "POLineNumber": "PO_LINE_ID",
}
complete_batch_cycle = []  # type: ignore


def complete_batch_update(sub_status, data):
    check = list(
        filter(lambda d: d.get("sub_status") == sub_status, complete_batch_cycle)
    )
    if len(check) == 0 or sub_status in [0, 1]:
        complete_batch_cycle.append(data)
    return "success"


# update status and sub status function


def update_document_status(db, id_doc, docStatus, doc_substatus, docDescription):
    try:
        db.query(model.Document).filter(model.Document.idDocument == id_doc).update(
            {
                "documentStatusID": str(docStatus),
                "documentsubstatusID": str(doc_substatus),
                "documentDescription": docDescription,
                "UpdatedOn": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        db.commit()
    except Exception as e:
        print(str(e))
        db.rollback()
        db.query(model.Document).filter(model.Document.idDocument == id_doc).update(
            {
                "documentStatusID": str(docStatus),
                "documentsubstatusID": str(doc_substatus),
                "documentDescription": "",
                "UpdatedOn": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        db.commit()


# update log history rules


def log_history_rules(db, id_doc, doc_substatus, type_s):
    try:
        db.add(
            model.DocumentRuleupdates(
                **{
                    "documentID": id_doc,
                    "documentSubStatusID": str(doc_substatus),
                    "IsActive": str(1),
                    "type": type_s,
                }
            )
        )
        db.commit()
    except BaseException:
        db.rollback()


# update log history


def log_history(db, id_doc, doc_substatus, docStatus, docDesc, created_date, userID):
    try:
        db.add(
            model.DocumentHistoryLogs(
                **{
                    "documentID": id_doc,
                    "documentSubStatusID": str(doc_substatus),
                    "documentdescription": docDesc,
                    "documentStatusID": str(docStatus),
                    "CreatedOn": created_date,
                    "userID": userID,
                }
            )
        )
        db.commit()
    except Exception as e:

        db.rollback()
        db.add(
            model.DocumentHistoryLogs(
                **{
                    "documentID": id_doc,
                    "documentSubStatusID": str(doc_substatus),
                    "documentdescription": "",
                    "documentStatusID": str(docStatus),
                    "CreatedOn": created_date,
                    "userID": userID,
                }
            )
        )
        db.commit()


# update invoice line items


def update_LineItems(db, value, error, desc):
    try:
        db.query(model.DocumentLineItems).filter(
            model.DocumentLineItems.idDocumentLineItems == value["idDocumentLineItems"]
        ).update({"isError": error, "CK_status": value["ck_status"], "ErrorDesc": desc})
        db.commit()
    except BaseException:
        db.rollback()


# get notification user and template details


def get_notification_details(db, id_doc, triggercode, subject):
    entityID = db.query(model.Document.entityID).filter_by(idDocument=id_doc).scalar()
    recepients = (
        db.query(model.UserAccess.UserID)
        .filter_by(EntityID=entityID, isActive=1)
        .distinct()
    )
    recepients = (
        db.query(
            model.User.idUser,
            model.User.email,
            model.User.firstName,
            model.User.lastName,
        )
        .filter(model.User.idUser.in_(recepients), model.User.isActive == 1)
        .all()
    )
    user_ids, *email = zip(*list(recepients))
    email_ids = list(zip(email[0], email[1], email[2]))
    cust_id = db.query(model.Entity.customerID).filter_by(idEntity=entityID).scalar()
    details = {
        "user_id": user_ids,
        "trigger_code": triggercode,
        "cust_id": cust_id,
        "inv_id": id_doc,
        "additional_details": {"subject": subject, "recipients": email_ids},
    }
    return details


# update overall status with checks


def update_overall_status(
    db,
    id_doc,
    enablegrncreation,
    GrnCreationType,
    grn_found,
    isbatchmap,
    grnNotApproved,
    u_id,
):
    docStatus = 1
    doc_substatus = 1
    docDesc = ""
    check_lines = (
        db.query(model.DocumentLineItems)
        .filter(model.DocumentLineItems.documentID == id_doc)
        .all()
    )
    if len(check_lines) == 0:
        update_document_status(db, id_doc, 4, 29, "OCR Error, Missing Line Items")
        return 4, 29, "OCR Error, Missing Line Items"
    db.query(model.DocumentData).filter(model.DocumentData.documentID == id_doc).update(
        {"isError": 0}
    )
    db.query(model.DocumentLineItems).filter(
        model.DocumentLineItems.documentID == id_doc
    ).update({"isError": 0})
    db.commit()
    complete_batch_update(
        23, {"status": 1, "msg": "Matching Success!", "sub_status": 23}
    )
    if enablegrncreation == 1:
        if GrnCreationType == 1 and grn_found == 0:
            docStatus = 4
            doc_substatus = 35
            docDesc = "Waiting for GRN"
            log_history(
                db,
                id_doc,
                35,
                4,
                "Waiting for GRN Creation",
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                u_id,
            )
        elif grn_found == 1 and grnNotApproved == False:
            docStatus = 2
            doc_substatus = 37
            docDesc = "success"
            log_history(
                db,
                id_doc,
                37,
                2,
                "GRN Created Successfully in ERP",
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                u_id,
            )
        elif grn_found == 1 and grnNotApproved:
            docStatus = 4
            doc_substatus = 78
            docDesc = "success"
            log_history(
                db,
                id_doc,
                78,
                4,
                "GRN Approval Pending",
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                u_id,
            )
        else:
            if grn_found == 1 and grnNotApproved == False:
                docStatus = 2
                doc_substatus = 23 if isbatchmap == 1 else 31
                docDesc = "success"
                log_history(
                    db,
                    id_doc,
                    37,
                    2,
                    "GRN Created Successfully in ERP",
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    u_id,
                )
            elif grn_found == 1 and grnNotApproved:
                docStatus = 4
                doc_substatus = 78
                docDesc = "success"
                log_history(
                    db,
                    id_doc,
                    78,
                    4,
                    "GRN Approval Pending",
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    u_id,
                )
            else:
                docStatus = 4
                doc_substatus = 35
                docDesc = "Waiting for GRN"
                log_history(
                    db,
                    id_doc,
                    35,
                    4,
                    "Waiting for GRN Creation",
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    u_id,
                )

    else:
        docStatus = 2
        doc_substatus = 23 if isbatchmap == 1 else 31
        docDesc = "success"
        log_history(
            db,
            id_doc,
            23,
            2,
            "Processing Document",
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            u_id,
        )
    update_document_status(db, id_doc, docStatus, doc_substatus, docDesc)
    return docStatus, doc_substatus, docDesc


# update overall status with checks (grn creation)


def update_overall_status_with_grn(
    db, GrnCreationType, id_doc, grn_found, enablegrncreation, grnNotApproved, u_id
):
    if grn_found == 1:
        docStatus, doc_substatus, docDesc = update_overall_status(
            db,
            id_doc,
            enablegrncreation,
            GrnCreationType,
            grn_found,
            1,
            grnNotApproved,
            u_id,
        )
    else:
        docStatus = 4
        docDesc = "GRN not found"
        doc_substatus = 38
        update_document_status(db, id_doc, docStatus, doc_substatus, docDesc)
        log_history(
            db,
            id_doc,
            doc_substatus,
            4,
            docDesc,
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            None,
        )
    return docStatus, doc_substatus, docDesc


def check_if_ocr_updated(inv_id, db):
    try:
        modelid = (
            db.query(model.Document.documentModelID)
            .filter(model.Document.idDocument == inv_id)
            .scalar()
        )
        SubTotalTag = (
            db.query(model.DocumentTagDef.idDocumentTagDef)
            .filter(
                model.DocumentTagDef.TagLabel == "SubTotal",
                model.DocumentTagDef.idDocumentModel == modelid,
            )
            .scalar()
        )
        TaxTag = (
            db.query(model.DocumentTagDef.idDocumentTagDef)
            .filter(
                model.DocumentTagDef.TagLabel == "TotalTax",
                model.DocumentTagDef.idDocumentModel == modelid,
            )
            .scalar()
        )
        TotalTag = (
            db.query(model.DocumentTagDef.idDocumentTagDef)
            .filter(
                model.DocumentTagDef.TagLabel == "InvoiceTotal",
                model.DocumentTagDef.idDocumentModel == modelid,
            )
            .scalar()
        )
        header_edited = (
            db.query(model.DocumentData)
            .filter(
                model.DocumentData.documentID == inv_id,
                model.DocumentData.IsUpdated == 1,
                model.DocumentData.documentTagDefID.in_(
                    [SubTotalTag, TotalTag, TaxTag]
                ),
            )
            .first()
        )
        QuantityTag = (
            db.query(model.DocumentLineItemTags.idDocumentLineItemTags)
            .filter(
                model.DocumentLineItemTags.TagName == "Quantity",
                model.DocumentLineItemTags.idDocumentModel == modelid,
            )
            .scalar()
        )
        UnitpriceTag = (
            db.query(model.DocumentLineItemTags.idDocumentLineItemTags)
            .filter(
                model.DocumentLineItemTags.TagName == "UnitPrice",
                model.DocumentLineItemTags.idDocumentModel == modelid,
            )
            .scalar()
        )
        line_edited = (
            db.query(model.DocumentLineItems)
            .filter(
                model.DocumentLineItems.documentID == inv_id,
                model.DocumentLineItems.IsUpdated == 1,
                model.DocumentLineItems.lineItemtagID.in_([QuantityTag, UnitpriceTag]),
            )
            .first()
        )
        if line_edited or header_edited:
            return True
        else:
            return False
    except Exception as e:
        return False


def po_vs_inv_total_equal(po_id, invoice_total, db):
    try:
        po_total = (
            db.query(model.Document.totalAmount).filter_by(idDocument=po_id).scalar()
        )
        if round(float(po_total), 2) == round(float(invoice_total), 2):
            return True
    except Exception as e:
        return False
    return False


def po_flip(po_id, inv_id, db):
    try:
        # delete invoice lines
        modelid = (
            db.query(model.Document.documentModelID)
            .filter(model.Document.idDocument == inv_id)
            .scalar()
        )
        all_lines = (
            db.query(model.DocumentLineItems.idDocumentLineItems)
            .filter(model.DocumentLineItems.documentID == inv_id)
            .all()
        )
        for a in all_lines:
            db.query(model.DocumentUpdates).filter(
                model.DocumentUpdates.documentLineItemID == a.idDocumentLineItems
            ).delete()
            db.commit()
        db.query(model.DocumentLineItems).filter(
            model.DocumentLineItems.documentID == inv_id
        ).delete()
        db.commit()
        # get po lines
        po_lines = (
            db.query(model.DocumentLineItems)
            .filter(model.DocumentLineItems.documentID == po_id)
            .all()
        )
        for po_line in po_lines:
            tagName = (
                db.query(model.DocumentLineItemTags.TagName)
                .filter(
                    model.DocumentLineItemTags.idDocumentLineItemTags
                    == po_line.lineItemtagID
                )
                .scalar()
            )
            if tagName in [
                "PurchQty",
                "UnitPrice",
                "Name",
                "DiscAmount",
                "DiscPercent",
            ]:
                if tagName == "PurchQty":
                    Tag = (
                        db.query(model.DocumentLineItemTags.idDocumentLineItemTags)
                        .filter(
                            model.DocumentLineItemTags.TagName == "Quantity",
                            model.DocumentLineItemTags.idDocumentModel == modelid,
                        )
                        .scalar()
                    )
                if tagName == "UnitPrice":
                    Tag = (
                        db.query(model.DocumentLineItemTags.idDocumentLineItemTags)
                        .filter(
                            model.DocumentLineItemTags.TagName == "UnitPrice",
                            model.DocumentLineItemTags.idDocumentModel == modelid,
                        )
                        .scalar()
                    )
                if tagName == "Name":
                    Tag = (
                        db.query(model.DocumentLineItemTags.idDocumentLineItemTags)
                        .filter(
                            model.DocumentLineItemTags.TagName == "Description",
                            model.DocumentLineItemTags.idDocumentModel == modelid,
                        )
                        .scalar()
                    )
                if tagName == "DiscAmount":
                    Tag = (
                        db.query(model.DocumentLineItemTags.idDocumentLineItemTags)
                        .filter(
                            model.DocumentLineItemTags.TagName == "Discount",
                            model.DocumentLineItemTags.idDocumentModel == modelid,
                        )
                        .scalar()
                    )
                    if Tag is None:
                        to_insert = {"idDocumentModel": modelid, "TagName": "Discount"}
                        db.add(model.DocumentLineItemTags(**to_insert))
                        db.commit()
                        Tag = (
                            db.query(model.DocumentLineItemTags.idDocumentLineItemTags)
                            .filter(
                                model.DocumentLineItemTags.TagName == "Discount",
                                model.DocumentLineItemTags.idDocumentModel == modelid,
                            )
                            .scalar()
                        )
                if tagName == "DiscPercent":
                    Tag = (
                        db.query(model.DocumentLineItemTags.idDocumentLineItemTags)
                        .filter(
                            model.DocumentLineItemTags.TagName == "DiscPercent",
                            model.DocumentLineItemTags.idDocumentModel == modelid,
                        )
                        .scalar()
                    )
                    if Tag is None:
                        to_insert = {
                            "idDocumentModel": modelid,
                            "TagName": "DiscPercent",
                        }
                        db.add(model.DocumentLineItemTags(**to_insert))
                        db.commit()
                        Tag = (
                            db.query(model.DocumentLineItemTags.idDocumentLineItemTags)
                            .filter(
                                model.DocumentLineItemTags.TagName == "DiscPercent",
                                model.DocumentLineItemTags.idDocumentModel == modelid,
                            )
                            .scalar()
                        )
                line_to_add = {
                    "documentID": inv_id,
                    "itemCode": po_line.itemCode,
                    "invoice_itemcode": po_line.itemCode,
                    "Value": po_line.Value,
                    "IsUpdated": 0,
                    "isError": 0,
                    "CreatedDate": datetime.utcnow(),
                    "lineItemtagID": Tag,
                }
                data = model.DocumentLineItems(**line_to_add)
                db.add(data)
                db.commit()
        return "success"
    except BaseException:
        db.rollback()
        return "failure"


# update status if in success or exception


def batch_update_db(
    data,
    Over_all_ck_data_status,
    GrnCreationType,
    grn_found,
    id_doc,
    enablegrncreation,
    ismultigrn,
    po_doc_id,
    invoice_header_cleaned,
    grnNotApproved,
    flip_done,
    db,
    u_id,
):
    global desc, error
    value = data[id_doc]
    erp_rule = data[id_doc]["erp_vd_st"]
    po_total_check = po_vs_inv_total_equal(
        po_doc_id, invoice_header_cleaned["SubTotal"], db
    )
    check_ocr_edited = check_if_ocr_updated(id_doc, db)
    if Over_all_ck_data_status == 0:
        complete_batch_update(
            1, {"status": 0, "msg": "Matching Failed - Batch Failed", "sub_status": 1}
        )
        update_document_status(db, id_doc, 1, 1, "Server Issue")
        return
    elif Over_all_ck_data_status == 10:
        update_document_status(db, id_doc, 10, 1, "PO is Closed/Fully booked!")
        log_history(
            db,
            id_doc,
            1,
            10,
            "PO is closed or fully booked!",
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            u_id,
        )
        complete_batch_update(
            0,
            {
                "status": 0,
                "msg": "PO is Fully Invoiced, Moving invoice to rejected queue!",
                "sub_status": 0,
            },
        )
        return
    elif Over_all_ck_data_status == 5:
        # No OCR Lines Picked, Check if po flip is possible:
        if flip_done == False and po_total_check and check_ocr_edited == False:
            status = po_flip(po_doc_id, id_doc, db)
            if status == "success":
                log_history(
                    db, id_doc, 1, 4, "Po Flip Successfull", datetime.utcnow(), u_id
                )
                single_doc_prc(id_doc, u_id)
            else:
                complete_batch_update(
                    1,
                    {
                        "status": 0,
                        "msg": "Matching Failed - OCR Lines Missing",
                        "sub_status": 1,
                    },
                )
                update_document_status(
                    db, id_doc, 4, 29, "OCR Error, Missing Line Items"
                )
                log_history(
                    db,
                    id_doc,
                    29,
                    4,
                    "OCR Error, Missing Line Items",
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    u_id,
                )
                return
        else:
            complete_batch_update(
                1,
                {
                    "status": 0,
                    "msg": "Matching Failed - OCR Lines Missing",
                    "sub_status": 1,
                },
            )
            update_document_status(db, id_doc, 4, 29, "OCR Error, Missing Line Items")
            log_history(
                db,
                id_doc,
                29,
                4,
                "OCR Error, Missing Line Items",
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                u_id,
            )
            return
    elif Over_all_ck_data_status == 34:
        # PO line mismatch, Check if po flip is possible:
        if flip_done == False and po_total_check and check_ocr_edited == False:
            status = po_flip(po_doc_id, id_doc, db)
            if status == "success":
                log_history(
                    db, id_doc, 1, 4, "Po Flip Successfull", datetime.utcnow(), u_id
                )
                single_doc_prc(id_doc, u_id)
            else:
                complete_batch_update(
                    34,
                    {
                        "status": 0,
                        "msg": "Matching Failed - PO Lines are less than Invoice Lines",
                        "sub_status": 34,
                    },
                )
                update_document_status(db, id_doc, 4, 34, "PO lines less than invoice")
                log_history_rules(db, id_doc, 34, "PO lines less than invoice")
                log_history(
                    db,
                    id_doc,
                    34,
                    4,
                    "PO Line Issue!",
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    u_id,
                )
                return
        else:
            complete_batch_update(
                34,
                {
                    "status": 0,
                    "msg": "Matching Failed - PO Lines are less than Invoice Lines",
                    "sub_status": 34,
                },
            )
            update_document_status(db, id_doc, 4, 34, "PO lines less than invoice")
            log_history_rules(db, id_doc, 34, "PO lines less than invoice")
            log_history(
                db,
                id_doc,
                34,
                4,
                "PO Line Issue!",
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                u_id,
            )
            return
    elif Over_all_ck_data_status == 2:
        if erp_rule == 2:
            # reference mapping issue for type 2(Stationary):
            complete_batch_update(
                1,
                {
                    "status": 0,
                    "msg": "Invoice Total does not match with PO total!",
                    "sub_status": 1,
                },
            )
            update_document_status(
                db, id_doc, 1, 1, "Invoice Total does not match with PO total"
            )
            log_history_rules(db, id_doc, 1, "")
            log_history(
                db,
                id_doc,
                1,
                1,
                "Invoice Total does not match with PO total!",
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                u_id,
            )
        else:
            # reference mapping issue for type 2(Stationary):
            complete_batch_update(
                33,
                {
                    "status": 0,
                    "msg": "Matching Failed - Reference Mapping issue",
                    "sub_status": 33,
                },
            )
            update_document_status(db, id_doc, 4, 33, "Price mismatch issue")
            log_history_rules(db, id_doc, 33, "Reference Mapping issue")
            log_history(
                db,
                id_doc,
                33,
                4,
                "Reference Mapping Issue!",
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                u_id,
            )
        return
    elif Over_all_ck_data_status == 8:
        log_history(
            db,
            id_doc,
            16,
            4,
            "Invoice Total Mismatch!",
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            u_id,
        )
        complete_batch_update(
            18,
            {
                "status": 0,
                "msg": "Invoice Total NOT EQUAL TO (SubTotal + Tax)",
                "sub_status": 18,
            },
        )
        update_document_status(db, id_doc, 4, 16, None)
        return
    elif Over_all_ck_data_status == 6:
        log_history(
            db,
            id_doc,
            16,
            4,
            "Invoice Total Mismatch!",
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            u_id,
        )
        complete_batch_update(
            19,
            {"status": 0, "msg": "Lines Total NOT EQUAL TO SubTotal", "sub_status": 19},
        )
        update_document_status(db, id_doc, 4, 16, None)
        return
    elif Over_all_ck_data_status == 1:
        docStatus, doc_substatus, docDesc = update_overall_status(
            db,
            id_doc,
            enablegrncreation,
            GrnCreationType,
            grn_found,
            1,
            grnNotApproved,
            u_id,
        )
        return

    over_all_status = 1
    po_grn_data = value["po_grn_data"]
    po_ch_qty = 1
    unitpr_ch_qty = 1
    doc_substatus = 27
    type_s = "error"
    if grn_found == 0 and GrnCreationType == 1:
        complete_batch_update(
            0,
            {
                "status": 1,
                "msg": "Matching only Invoice and PO items!",
                "sub_status": 0,
            },
        )
    else:
        complete_batch_update(
            0,
            {"status": 1, "msg": "Matching Invoice,PO and GRN items!", "sub_status": 0},
        )
    for key, val in po_grn_data.items():
        for subkey, subval in val.items():  # item check PO n GRn
            if GrnCreationType == 1 and grn_found == 0:
                if subkey == "qty":
                    if subval["po_status"] == 1:
                        error = 0
                        desc = None
                    elif subval["po_status"] == 0:
                        po_ch_qty = 0
                        desc = subkey + " is not matching with PO"
                        update_LineItems(db, subval, 1, desc)
                elif subkey == "unit_price":
                    if subval["po_status"] == 0:  # Unitprice PO
                        unitpr_ch_qty = 0
                        desc = subkey + " is not matching with PO"
                        update_LineItems(db, subval, 1, desc)
                    else:
                        error = 0
                        desc = "Unit price matching"
                        update_LineItems(db, subval, error, desc)
                if subval["ck_status"] == 7:
                    error = 0

            elif grn_found == 1:
                if subkey == "qty" and Over_all_ck_data_status != 7:
                    if subval["po_status"] == 1 and subval["grn_status"] == 1:
                        error = 0
                        desc = None
                    elif subval["po_status"] == 1 and subval["grn_status"] == 0:
                        error = 1
                        desc = subkey + " is not matching with GRN"
                    elif subval["po_status"] == 0 and subval["grn_status"] == 1:
                        error = 1
                        po_ch_qty = 0
                        desc = subkey + " is not matching with PO"
                    elif subval["po_status"] == 0 and subval["grn_status"] == 0:
                        error = 1
                        po_ch_qty = 0
                        desc = subkey + " is not matching with PO and GRN"
                    update_LineItems(db, subval, error, desc)
                    if po_ch_qty == 0 and (
                        subval["ck_status"] == 1 or subval["ck_status"] == 2
                    ):  # Substatus check
                        doc_substatus = 21
                        over_all_status = 0
                        # UPDATE query here
                        log_history_rules(db, id_doc, doc_substatus, type_s)

                    if unitpr_ch_qty == 0 and (
                        subval["ck_status"] == 1 or subval["ck_status"] == 2
                    ):
                        doc_substatus = 16
                        over_all_status = 0
                        # UPDATE query here
                        log_history_rules(db, id_doc, doc_substatus, type_s)
                    if (po_ch_qty == 1) and (subval["ck_status"] == 1):
                        doc_substatus = 23
                    if (po_ch_qty == 1) and (subval["ck_status"] == 2):
                        doc_substatus = 23
                    log_history_rules(db, id_doc, doc_substatus, type_s)
                elif subkey == "unit_price":
                    if subval["po_status"] == 1 and subval["grn_status"] == 1:
                        error = 0
                        desc = None
                    elif subval["po_status"] == 1 and subval["grn_status"] == 0:
                        error = 1
                        # grn_item_ck = grn_item_ck*0
                        desc = subkey + " is not matching with GRN"
                        unitpr_ch_qty = 0
                    elif (
                        subval["po_status"] == 0 and subval["grn_status"] == 1
                    ):  # Unitprice PO
                        error = 1
                        desc = subkey + " is not matching with PO"
                        unitpr_ch_qty = 0
                    elif subval["po_status"] == 0 and subval["grn_status"] == 0:
                        error = 1
                        unitpr_ch_qty = 0
                        desc = subkey + " is not matching with PO and GRN"
                    else:
                        error = 0
                        desc = ""
                    update_LineItems(db, subval, error, desc)
                if subval["ck_status"] == 7:
                    error = 0
    docStatus = 4
    if GrnCreationType == 1 and grn_found != 1:
        if po_ch_qty == 1 and unitpr_ch_qty == 1:
            docStatus, doc_substatus, docDesc = update_overall_status(
                db,
                id_doc,
                enablegrncreation,
                GrnCreationType,
                grn_found,
                1,
                grnNotApproved,
                u_id,
            )
            created_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            log_history(
                db,
                id_doc,
                doc_substatus,
                docStatus,
                "Processing Document",
                created_date,
                u_id,
            )
        elif po_ch_qty == 0:
            doc_substatus = 21
            over_all_status = 0
            update_document_status(db, id_doc, docStatus, doc_substatus, None)
            log_history(
                db,
                id_doc,
                21,
                4,
                "PO Quantity Check",
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                u_id,
            )
            complete_batch_update(
                21, {"status": 0, "msg": "PO Quantity Mismatch!", "sub_status": 21}
            )
            # UPDATE query here
            log_history_rules(db, id_doc, doc_substatus, type_s)
        elif unitpr_ch_qty == 0:
            doc_substatus = 16
            over_all_status = 0
            complete_batch_update(
                16, {"status": 0, "msg": "Unit Price Mismatch!", "sub_status": 16}
            )
            log_history(
                db,
                id_doc,
                16,
                4,
                "Unit Price Mismatch!",
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                u_id,
            )
            # UPDATE query here
            update_document_status(db, id_doc, docStatus, doc_substatus, None)
            log_history_rules(db, id_doc, doc_substatus, type_s)
        else:
            # mismatch values
            doc_substatus = 27
            log_history(
                db,
                id_doc,
                27,
                4,
                "Mismatch Values!",
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                u_id,
            )
            complete_batch_update(
                27, {"status": 0, "msg": "Multiple Mismatch values!", "sub_status": 27}
            )
            update_document_status(db, id_doc, docStatus, doc_substatus, None)
    elif grn_found == 1:
        docDesc = ""
        if po_ch_qty == 0:
            doc_substatus = 21
            over_all_status = 0
            docDesc = "PO Quantity Mismatch!"
            update_document_status(db, id_doc, 4, 21, "PO Quantity Mismatch!")
            complete_batch_update(
                21, {"status": 0, "msg": "PO Quantity Mismatch!", "sub_status": 21}
            )
            log_history_rules(db, id_doc, doc_substatus, type_s)
            log_history(
                db,
                id_doc,
                doc_substatus,
                4,
                docDesc,
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                u_id,
            )
        elif unitpr_ch_qty == 0:
            doc_substatus = 16
            over_all_status = 0
            docDesc = "Unit Price Mismatch!"
            update_document_status(db, id_doc, 4, 16, "Unit Price Mismatch!")
            complete_batch_update(
                16, {"status": 0, "msg": "Unit Price Mismatch!", "sub_status": 16}
            )
            log_history_rules(db, id_doc, doc_substatus, type_s)
            log_history(
                db,
                id_doc,
                doc_substatus,
                4,
                docDesc,
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                u_id,
            )

        if po_ch_qty == 1 and unitpr_ch_qty == 1:
            doc_substatus = 23
            over_all_status = 1
        if Over_all_ck_data_status == 7:
            # check if PO Flip possible
            over_all_status = 0
            if flip_done == False and po_total_check and check_ocr_edited == False:
                status = po_flip(po_doc_id, id_doc, db)
                if status == "success":
                    log_history(
                        db, id_doc, 1, 4, "Po Flip Successfull", datetime.utcnow(), u_id
                    )
                    single_doc_prc(id_doc, u_id)
                else:
                    complete_batch_update(
                        17,
                        {
                            "status": 0,
                            "msg": "Invoice Total vs GRN Total Mismatch!",
                            "sub_status": 17,
                        },
                    )
                    update_document_status(db, id_doc, 4, 17, "GRN Total Mismatch!")
                    log_history(
                        db,
                        id_doc,
                        17,
                        4,
                        "GRN Total Mismatch!",
                        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        u_id,
                    )
            else:
                complete_batch_update(
                    17,
                    {
                        "status": 0,
                        "msg": "Invoice Total vs GRN Total Mismatch!",
                        "sub_status": 17,
                    },
                )
                update_document_status(db, id_doc, 4, 17, "GRN Total Mismatch!")
                log_history(
                    db,
                    id_doc,
                    17,
                    4,
                    "GRN Total Mismatch!",
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    u_id,
                )
        if Over_all_ck_data_status == 4:
            over_all_status = 0
            docStatus = 4
            doc_substatus = 33
            docDesc = "Price mismatch issue"
            type_s = "Reference Mapping issue"
            complete_batch_update(
                33, {"status": 0, "msg": "Reference Mapping issue!", "sub_status": 33}
            )
            update_document_status(db, id_doc, docStatus, doc_substatus, docDesc)
            log_history_rules(db, id_doc, doc_substatus, type_s)
            log_history(
                db,
                id_doc,
                doc_substatus,
                4,
                docDesc,
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                u_id,
            )
        if over_all_status == 1:
            doc_substatus = 23
            update_overall_status_with_grn(
                db,
                GrnCreationType,
                id_doc,
                grn_found,
                enablegrncreation,
                grnNotApproved,
                u_id,
            )
        log_history_rules(db, id_doc, doc_substatus, type_s)
    print(flip_done, po_total_check, check_ocr_edited)
    if flip_done == False and po_total_check and check_ocr_edited == False:
        # check if po flip possible
        if po_ch_qty == 0 or unitpr_ch_qty == 0:
            status = po_flip(po_doc_id, id_doc, db)
            if status == "success":
                log_history(
                    db, id_doc, 1, 4, "Po Flip Successfull", datetime.utcnow(), u_id
                )
                single_doc_prc(id_doc, u_id)

    if Over_all_ck_data_status == 9:
        complete_batch_update(
            8,
            {
                "status": 0,
                "msg": "Invoice to PO Auto Mapping Failed. Please map it manually",
                "sub_status": 8,
            },
        )
        update_document_status(db, id_doc, 4, 8, "PO Item Check")
        log_history(
            db,
            id_doc,
            8,
            4,
            "PO Item Check",
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            u_id,
        )
        return


# prepare po header data to compare
def prepare_po_header_data(header_data, po_tag_map):
    po_header_cleaned = {}
    for po_header in header_data:
        if po_header.DocumentTagDef.TagLabel in po_tag_map.keys():
            po_header_cleaned[po_tag_map[po_header.DocumentTagDef.TagLabel]] = (
                po_header.DocumentData.Value
            )
    return po_header_cleaned


# prepare line details to compare


def prepare_line_data(line_data, po_tag_map):
    numberic_labels = [
        "InvoiceTotal",
        "SubTotal",
        "TotalTax",
        "Amount",
        "UnitPrice",
        "Quantity",
        "AmountExcTax",
        "Tax",
        "DiscAmount",
        "DiscPercent",
        "Discount",
    ]
    # Sort the data based on itemCode
    line_data.sort(key=lambda x: x.DocumentLineItems.itemCode)
    line_cleaned = []

    for key, group in groupby(line_data, key=lambda x: x.DocumentLineItems.itemCode):
        obj = {}
        for line in group:
            tag_name = line.DocumentLineItemTags.TagName
            if tag_name == "UnitPrice":
                obj["idDocumentLineItems"] = line.DocumentLineItems.idDocumentLineItems
            if tag_name == "Quantity":
                obj["QuantityTag"] = line.DocumentLineItems.idDocumentLineItems
            if tag_name == "Description":
                obj["DescriptionTag"] = line.DocumentLineItems.idDocumentLineItems
            # Check if the tag name exists in the po_tag_map
            if tag_name in po_tag_map.keys():
                # If it exists, use the mapped key from the po_tag_map
                obj[po_tag_map[tag_name]] = (
                    cln_amt(line.DocumentLineItems.Value)
                    if po_tag_map[tag_name] in numberic_labels
                    else line.DocumentLineItems.Value
                )
            else:
                # If it doesn't exist in po_tag_map, use the original key
                obj[tag_name] = (
                    cln_amt(line.DocumentLineItems.Value)
                    if tag_name in numberic_labels
                    else line.DocumentLineItems.Value
                )

        obj["itemCode"] = key
        obj["invoice_itemcode"] = line.DocumentLineItems.invoice_itemcode
        line_cleaned.append(obj)

    return line_cleaned


# prepare grn line details to compare


def prepare_grn_line_data(line_data, po_tag_map):
    numberic_labels = [
        "InvoiceTotal",
        "SubTotal",
        "TotalTax",
        "Amount",
        "UnitPrice",
        "Quantity",
        "AmountExcTax",
        "Tax",
        "DiscAmount",
        "DiscPercent",
    ]
    line_cleaned_obj = {}

    for doc in line_data.keys():
        # Sort the data based on itemCode
        line_data[doc].sort(
            key=lambda x: (
                x.DocumentLineItems.itemCode
                if x.DocumentLineItems.itemCode
                else x.DocumentLineItems.invoice_itemcode
            )
        )
        line_cleaned_obj[doc] = []

        for key, group in groupby(
            line_data[doc],
            key=lambda x: (
                x.DocumentLineItems.itemCode
                if x.DocumentLineItems.itemCode
                else x.DocumentLineItems.invoice_itemcode
            ),
        ):
            obj = {}
            for line in group:
                tag_name = line.DocumentLineItemTags.TagName
                # Check if the tag name exists in the po_tag_map
                if tag_name in po_tag_map.keys():
                    # If it exists, use the mapped key from the po_tag_map
                    obj[po_tag_map[tag_name]] = (
                        cln_amt(line.DocumentLineItems.Value)
                        if po_tag_map[tag_name] in numberic_labels
                        else line.DocumentLineItems.Value
                    )
                else:
                    # If it doesn't exist in po_tag_map, use the original key
                    obj[tag_name] = (
                        cln_amt(line.DocumentLineItems.Value)
                        if tag_name in numberic_labels
                        else line.DocumentLineItems.Value
                    )

                obj["itemCode"] = key
                obj["invoice_itemcode"] = line.DocumentLineItems.invoice_itemcode
                obj["idDocumentLineItems"] = line.DocumentLineItems.idDocumentLineItems

            line_cleaned_obj[doc].append(obj)

    return line_cleaned_obj


# prepare invoice header to compare


def prepare_inv_header_data(header_data):
    invoice_header_cleaned = {}
    for invoice_header in header_data:
        invoice_header_cleaned[invoice_header.DocumentTagDef.TagLabel] = (
            invoice_header.DocumentData.Value
        )
    return invoice_header_cleaned


# prepare item mapping data


def prepare_item_mapping(item_mapping, doc_id, db):
    item_mapping_list = []
    for item in item_mapping:
        invo_itemcode = (
            db.query(model.DocumentLineItems.invoice_itemcode)
            .filter(
                model.DocumentLineItems.documentID == doc_id,
                model.DocumentLineItems.itemCode == item.mappedinvoiceitemcode,
            )
            .first()
        )
        if invo_itemcode:
            obj = {
                "itemCode": str(invo_itemcode[0]),
                "user_map": item.mappedinvoitemdescription,
                "map_invoice_itemcode": item.mappedinvoiceitemcode,
                "documentID": item.documentID,
            }
            item_mapping_list.append(obj)
    return item_mapping_list


def round_up(n, decimals=0):
    multiplier = 10**decimals
    return math.ceil(n * multiplier) / multiplier


# rule to check only header data


def check_only_header_data(
    po_line_cleaned, invoice_header_cleaned, invo_doc_id, db, u_id
):
    invo_value_check = False
    po_unitprice_total = 0
    checked_value = {"InvoiceTotal": {}, "SubTotal": {}}
    Over_all_ck_data_status = 0
    try:
        if "InvoiceTotal" in invoice_header_cleaned.keys():
            checked_value["InvoiceTotal"] = {
                "Value": invoice_header_cleaned["InvoiceTotal"],
                "key": "InvoiceTotal",
            }
        if "SubTotal" in invoice_header_cleaned.keys():
            checked_value["SubTotal"] = {
                "Value": invoice_header_cleaned["SubTotal"],
                "key": "SubTotal",
            }
    except BaseException:
        checked_value = {
            "InvoiceTotal": {"Value": 0, "key": ""},
            "SubTotal": {"Value": 0, "key": ""},
        }
        update_document_status(
            db, invo_doc_id, 1, 1, "SubTotal and Invoice Total Missing or incorrect"
        )
        created_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        log_history(
            db,
            invo_doc_id,
            1,
            1,
            "SubTotal and Invoice Total Missing or incorrect",
            created_date,
            u_id,
        )

    for po_line in po_line_cleaned:
        po_discamount = po_line["DiscAmount"] if "DiscAmount" in po_line else 0
        po_discpercent = po_line["DiscPercent"] if "DiscPercent" in po_line else 0
        if po_discamount > 0:
            po_unitprice_total += (po_line["UnitPrice"] - po_discamount) * po_line[
                "Quantity"
            ]
        elif po_discpercent > 0:
            po_unitprice_total += po_line["UnitPrice"] - (
                po_line["UnitPrice"] * (po_discpercent / 100)
            )
        else:
            po_unitprice_total += po_line["UnitPrice"] * po_line["Quantity"]
    invo_value_check = (
        float(checked_value["InvoiceTotal"]["Value"]) == po_unitprice_total
    )
    subtotal_value_check = (
        float(checked_value["SubTotal"]["Value"]) == po_unitprice_total
    )
    istotal = True
    if invo_value_check:
        istotal = True
    if subtotal_value_check:
        istotal = False
    if invo_value_check or subtotal_value_check:
        complete_batch_update(
            23,
            {
                "status": 1,
                "msg": "Invoice Total matches with PO total!",
                "sub_status": 23,
            },
        )
        if istotal:
            prd_tg_iv = checked_value["InvoiceTotal"]["key"]
        else:
            prd_tg_iv = checked_value["SubTotal"]["key"]
        if prd_tg_iv != "":
            Over_all_ck_data_status = 1
        else:
            Over_all_ck_data_status = 2
    else:
        Over_all_ck_data_status = 2
    return Over_all_ck_data_status


# rule to check single po vs multiple invoice lines


def check_single_po_vs_multple_invoice_lines(
    v_model_id, invoice_line_cleaned, po_line_cleaned, invo_doc_id, db, u_id
):
    Over_all_ck_data_status = 0
    try:
        check = (
            db.query(model.DocumentTagDef)
            .filter(
                model.DocumentTagDef.idDocumentModel == v_model_id,
                model.DocumentTagDef.TagLabel == "Total Quantity",
            )
            .first()
        )
        if check is None:
            res = db.add(
                model.DocumentTagDef(
                    **{"idDocumentModel": v_model_id, "TagLabel": "Total Quantity"}
                )
            )
            db.commit()
            totalQty_tagID = res.idDocumentTagDef
        else:
            totalQty_tagID = check.idDocumentTagDef
        invo_total_QTY = 0
        for inv in invoice_line_cleaned:
            invo_total_QTY += inv["Quantity"]
        PO_total_QTY = 0
        for po in po_line_cleaned:
            PO_total_QTY += po["Quantity"]
        po_temCode = po_line_cleaned[0]["itemcode"]
        if float(invo_total_QTY) == float(PO_total_QTY):
            totalQTY_error = 0
            qty_ErrorDesc = "Total invoice QTY matching with PO Qty"
            db.query(model.DocumentLineItems).filter(
                model.DocumentLineItems.documentID == invo_doc_id
            ).update({"invoice_itemcode": po_temCode})
            db.commit()
            Over_all_ck_data_status = 1
            complete_batch_update(
                23, {"status": 1, "msg": qty_ErrorDesc, "sub_status": 23}
            )
        else:
            totalQTY_error = 1
            qty_ErrorDesc = (
                "Invoice Total qty("
                + str(invo_total_QTY)
                + ") not matching wit PO qty("
                + str(PO_total_QTY)
                + ")."
            )
            Over_all_ck_data_status = 2
            complete_batch_update(
                1, {"status": 0, "msg": qty_ErrorDesc, "sub_status": 1}
            )
        db.add(
            model.DocumentData(
                **{
                    "documentID": invo_doc_id,
                    "documentTagDefId": totalQty_tagID,
                    "Value": invo_total_QTY,
                    "isError": totalQTY_error,
                    "ErrorDesc": qty_ErrorDesc,
                }
            )
        )
        db.commit()
    except Exception as ert:
        update_document_status(db, invo_doc_id, 1, 1, ert)
        created_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        log_history(db, invo_doc_id, 1, 1, ert, created_date, u_id)
        Over_all_ck_data_status = 2
    return Over_all_ck_data_status


# remove unwanted po description which are already invoiced


def get_po_description(document, po_line, db):
    invs = (
        db.query(model.Document.idDocument)
        .filter(
            model.Document.idDocumentType == 3,
            model.Document.PODocumentID == document.PODocumentID,
            model.Document.documentStatusID.in_([7, 14, 2]),
        )
        .all()
    )
    used_grn_quantity = {}
    result_list = po_line.copy()  # Initialize result list with a copy of po_line
    for inv in invs:
        if inv[0] and inv[0] != document.idDocument:
            invlines_quantity = (
                db.query(model.DocumentLineItems, model.DocumentLineItemTags)
                .options(
                    Load(model.DocumentLineItems).load_only(
                        "invoice_itemcode", "Value"
                    ),
                    Load(model.DocumentLineItemTags).load_only("TagName"),
                )
                .join(
                    model.DocumentLineItemTags,
                    model.DocumentLineItems.lineItemtagID
                    == model.DocumentLineItemTags.idDocumentLineItemTags,
                )
                .filter(
                    model.DocumentLineItems.documentID == inv[0],
                    model.DocumentLineItemTags.TagName == "Quantity",
                )
                .all()
            )
            for grn_qty in invlines_quantity:
                if grn_qty.DocumentLineItems.invoice_itemcode in used_grn_quantity:
                    used_grn_quantity[
                        grn_qty.DocumentLineItems.invoice_itemcode
                    ] += cln_amt(grn_qty.DocumentLineItems.Value)
                else:
                    used_grn_quantity[grn_qty.DocumentLineItems.invoice_itemcode] = (
                        cln_amt(grn_qty.DocumentLineItems.Value)
                    )
    for item_code in used_grn_quantity.keys():
        po_total_quantity = next(
            (
                d["RemainInventPhysical"]
                for d in po_line
                if d.get("itemCode") == item_code
            ),
            None,
        )
        if po_total_quantity is not None and po_total_quantity == 0:
            result_list.remove(
                next((d for d in po_line if d.get("itemCode") == item_code), None)
            )
    return result_list


# 2 way / 3 way check


def twoWay_3Way_check(
    document,
    invo_doc_id,
    po_doc_id,
    invoice_header_cleaned,
    invoice_line_cleaned,
    po_line_cleaned,
    grn_line_cleaned,
    item_mapping_cleaned,
    grn_found,
    qty_tol_percent,
    ut_tol_percent,
    invoice_line_data,
    skipGrnMatch,
    db,
    u_id,
):
    auto_mapped = True
    try:
        Over_all_ck_data_status = 0
        po_line_cleaned = get_po_description(document, po_line_cleaned, db)
        po_desc = [cmp_po["Description"] for cmp_po in po_line_cleaned]
        new_itm_mh = {}
        db.query(model.DocumentLineItems).filter(
            model.DocumentLineItems.documentID == invo_doc_id
        ).update({"isError": 0, "ErrorDesc": ""})
        db.commit()
        if len(po_desc) > 0:
            for cmp in invoice_line_cleaned:
                ck_decp = cmp["Description"]
                invo_itemcode = cmp["itemCode"]
                best_match = process.extractBests(
                    ck_decp, po_desc, scorer=fuzz.token_sort_ratio
                )
                max_value = max(best_match, key=lambda x: x[1])[1]
                max_matches = [t for t in best_match if t[1] == max_value]
                mp_invo_dt = list(
                    filter(
                        lambda d: d.get("map_invoice_itemcode") == invo_itemcode
                        and d.get("documentID") == invo_doc_id,
                        item_mapping_cleaned,
                    )
                )
                if len(mp_invo_dt) > 0:
                    new_itm_mh[mp_invo_dt[0]["itemCode"]] = {
                        "invo_itm_code": mp_invo_dt[0]["map_invoice_itemcode"],
                        "fuzz_scr": 100,
                    }
                    desc_match = list(
                        filter(
                            lambda d: d.get("itemCode") == mp_invo_dt[0]["itemCode"],
                            po_line_cleaned,
                        )
                    )[0]["Description"]
                    try:
                        po_desc.remove(desc_match)
                    except BaseException:
                        pass
                else:
                    best_description = max_matches[0][0]
                    all_desc_match = list(
                        filter(
                            lambda d: d.get("Description") == best_description,
                            po_line_cleaned,
                        )
                    )
                    if len(all_desc_match) == 1:
                        po_item_code = all_desc_match[0]["itemCode"]
                        try:
                            po_desc.remove(best_description)
                        except BaseException:
                            pass
                    else:
                        desc_tag = cmp["DescriptionTag"]
                        update_LineItems(
                            db,
                            {"idDocumentLineItems": desc_tag, "ck_status": 2},
                            1,
                            "Invoice Description multiple matches with PO Description",
                        )
                        auto_mapped = False
                        po_item_code = invo_itemcode
                    if po_item_code not in new_itm_mh.keys():
                        new_itm_mh[po_item_code] = {
                            "invo_itm_code": invo_itemcode,
                            "fuzz_scr": max_matches[0][1],
                        }
            print(new_itm_mh)
            po_grn_data = {}
            doc_sel_rulid = document.ruleID
            hasissues = False
            grn_total = 0.0
            inv_sub_total = 0.0
            query_resp = (
                db.query(model.DocumentSubStatus, model.DocumentRulemapping)
                .join(
                    model.DocumentRulemapping,
                    model.DocumentSubStatus.idDocumentSubstatus
                    == model.DocumentRulemapping.DocumentstatusID,
                )
                .filter(model.DocumentRulemapping.DocumentRulesID == doc_sel_rulid)
                .all()
            )
            sel_rul_ck = [r.DocumentSubStatus.status for r in query_resp]
            for mt_ in new_itm_mh:
                invo_itm_cd = new_itm_mh[mt_]["invo_itm_code"]
                po_qty = list(
                    filter(lambda d: d.get("itemCode") == mt_, po_line_cleaned)
                )[0]["Quantity"]
                invo_qty = list(
                    filter(
                        lambda d: d.get("itemCode") == invo_itm_cd, invoice_line_cleaned
                    )
                )[0]["Quantity"]
                # invo_lineid = list(filter(lambda d: d.get('itemCode') == invo_itm_cd, invoice_line_cleaned))[0]["idDocumentLineItems"]
                db.query(model.DocumentLineItems).filter(
                    model.DocumentLineItems.itemCode == invo_itm_cd,
                    model.DocumentLineItems.documentID == invo_doc_id,
                ).update({"invoice_itemcode": mt_})
                db.commit()
                list(
                    filter(
                        lambda d: d.get("itemCode") == invo_itm_cd, invoice_line_cleaned
                    )
                )[0]["invoice_itemcode"] = mt_
                qty_tol_percent = 10
                unitprice_threshold_cal = 0.02
                qty_threshold = po_qty * (qty_tol_percent / 100)
                po_unitprice = list(
                    filter(lambda d: d.get("itemCode") == mt_, po_line_cleaned)
                )[0]["UnitPrice"]
                po_discamount = (
                    list(filter(lambda d: d.get("itemCode") == mt_, po_line_cleaned))[
                        0
                    ]["DiscAmount"]
                    if "DiscAmount"
                    in list(
                        filter(lambda d: d.get("itemCode") == mt_, po_line_cleaned)
                    )[0]
                    else 0
                )
                po_discpercent = (
                    list(filter(lambda d: d.get("itemCode") == mt_, po_line_cleaned))[
                        0
                    ]["DiscPercent"]
                    if "DiscPercent"
                    in list(
                        filter(lambda d: d.get("itemCode") == mt_, po_line_cleaned)
                    )[0]
                    else 0
                )
                if po_discamount > 0:
                    po_actualprice = po_unitprice - po_discamount
                elif po_discpercent > 0:
                    po_actualprice = po_unitprice - (
                        po_unitprice * (po_discpercent / 100)
                    )
                else:
                    po_actualprice = po_unitprice
                invo_unitprice = list(
                    filter(
                        lambda d: d.get("itemCode") == invo_itm_cd, invoice_line_cleaned
                    )
                )[0]["UnitPrice"]
                # unitprice_threshold_cal = (po_actualprice * (int(ut_tol_percent) / 100))
                invo_discamount = (
                    list(
                        filter(
                            lambda d: d.get("itemCode") == invo_itm_cd,
                            invoice_line_cleaned,
                        )
                    )[0]["Discount"]
                    if "Discount"
                    in list(
                        filter(
                            lambda d: d.get("itemCode") == invo_itm_cd,
                            invoice_line_cleaned,
                        )
                    )[0]
                    and list(
                        filter(
                            lambda d: d.get("itemCode") == invo_itm_cd,
                            invoice_line_cleaned,
                        )
                    )[0]["Discount"]
                    != ""
                    else 0
                )
                invo_discpercent = (
                    list(
                        filter(
                            lambda d: d.get("itemCode") == invo_itm_cd,
                            invoice_line_cleaned,
                        )
                    )[0]["DiscPercent"]
                    if "DiscPercent"
                    in list(
                        filter(
                            lambda d: d.get("itemCode") == invo_itm_cd,
                            invoice_line_cleaned,
                        )
                    )[0]
                    and list(
                        filter(
                            lambda d: d.get("itemCode") == invo_itm_cd,
                            invoice_line_cleaned,
                        )
                    )[0]["DiscPercent"]
                    != ""
                    else 0
                )
                if invo_discamount != 0 or invo_discamount != "":
                    invo_unitprice = invo_unitprice - invo_discamount
                elif invo_discpercent != 0 or invo_discpercent != "":
                    invo_unitprice = invo_unitprice - (
                        invo_unitprice * (invo_discpercent / 100)
                    )
                if (
                    round(abs(po_actualprice - invo_unitprice), 2)
                    < unitprice_threshold_cal
                ):
                    unitprice_status = 1
                elif po_actualprice == invo_unitprice:
                    unitprice_status = 1
                elif invo_unitprice < po_actualprice:
                    tol_unitPrice_PO = po_actualprice + (po_actualprice * (-5 / 100))
                    if invo_unitprice >= tol_unitPrice_PO:
                        unitprice_status = 1
                    else:
                        unitprice_status = 0
                else:
                    unitprice_status = 0
                try:
                    remInvPy = list(
                        filter(
                            lambda d: d.get("itemCode") == invo_itm_cd, po_line_cleaned
                        )
                    )
                    if len(remInvPy) > 0:
                        PO_bal = remInvPy[0]["Quantity"]
                    else:
                        PO_bal = 0
                except Exception as er:
                    update_document_status(db, invo_doc_id, 1, 1, er)
                    PO_bal = 0
                PO_bal = po_qty
                po_qty_status = 0
                print("invoice vs po qty", invo_qty, po_qty, qty_threshold)
                print(
                    "invoice vs po price",
                    invo_unitprice,
                    po_actualprice,
                    unitprice_threshold_cal,
                )
                inv_sub_total += invo_unitprice * invo_qty
                try:
                    invo_qty = float(invo_qty)
                except BaseException:
                    invo_qty = 0.0
                if PO_bal == invo_qty:
                    po_qty_status = 1
                elif invo_qty > PO_bal:
                    if abs(invo_qty - po_qty) <= qty_threshold:
                        po_qty_status = 1
                    else:
                        po_qty_status = 0
                elif PO_bal > invo_qty:
                    po_qty_status = 1
                else:
                    po_qty_status = 0

                for inv in invoice_line_data:
                    if (
                        inv.DocumentLineItems.itemCode == invo_itm_cd
                        and inv.DocumentLineItemTags.TagName == "UnitPrice"
                    ):
                        unitprice_tag = inv.DocumentLineItems.idDocumentLineItems
                    if (
                        inv.DocumentLineItems.itemCode == invo_itm_cd
                        and inv.DocumentLineItemTags.TagName == "Quantity"
                    ):
                        qty_tag = inv.DocumentLineItems.idDocumentLineItems
                po_grn_data[mt_] = {
                    "qty": {
                        "po_status": po_qty_status,
                        "grn_status": 1,
                        "idDocumentLineItems": qty_tag,
                        "ck_status": 0,
                    },
                    "unit_price": {
                        "po_status": unitprice_status,
                        "grn_status": 1,
                        "idDocumentLineItems": unitprice_tag,
                        "ck_status": 0,
                    },
                }
                if po_qty_status == 0:
                    update_LineItems(
                        db,
                        {"idDocumentLineItems": qty_tag, "ck_status": 2},
                        1,
                        "Invoice Quantity not match with PO quantity",
                    )
                    hasissues = True
                if unitprice_status == 0:
                    update_LineItems(
                        db,
                        {"idDocumentLineItems": unitprice_tag, "ck_status": 2},
                        1,
                        "Invoice Unit price not matching with PO unit price",
                    )
                    hasissues = True
                if ("Po Qty Check" and "GRN Qty Check") in sel_rul_ck:
                    po_grn_data[mt_]["qty"]["ck_status"] = 1

                elif ("Po Qty Check") in sel_rul_ck:
                    po_grn_data[mt_]["qty"]["ck_status"] = 2

                elif ("GRN Qty Check") in sel_rul_ck:
                    po_grn_data[mt_]["qty"]["ck_status"] = 3
                else:
                    po_grn_data[mt_]["qty"]["ck_status"] = 7
                if ("Unit Price Mismatch") in sel_rul_ck:
                    po_grn_data[mt_]["unit_price"]["ck_status"] = 2
                else:
                    po_grn_data[mt_]["unit_price"]["ck_status"] = 7
            hasheaderissues = False
            try:
                header_subtotal = float(invoice_header_cleaned["SubTotal"])
                total_amount = float(invoice_header_cleaned["InvoiceTotal"])
                total_tax = float(invoice_header_cleaned["TotalTax"])
                if round(float(header_subtotal + total_tax), 2) == round(
                    float(total_amount), 2
                ):
                    hasheaderissues = False
                else:
                    hasheaderissues = True
            except Exception as e:
                hasheaderissues = True
                total_amount = 0.0
                total_tax = 0.0
                header_subtotal = 0.0
            hasGrnissues = False
            hastotalIssues = False
            # calculate grn total if found
            fully_invoiced = False
            if grn_found == 1:
                if len(grn_line_cleaned.keys()) > 0:
                    total_quantities = {}
                    total_inv_quantity = {}
                    grns = []
                    # Iterate through the existing dictionary and update the
                    # total quantities
                    for key in grn_line_cleaned:
                        for item in grn_line_cleaned[key]:
                            item_code = item["itemCode"]
                            quantity = (
                                item["Quantity"] if "Quantity" in item else item["Qty"]
                            )
                            check_invoice_line = list(
                                filter(
                                    lambda d: d.get("invoice_itemcode") == item_code,
                                    invoice_line_cleaned,
                                )
                            )
                            if item_code in new_itm_mh.keys():
                                if len(check_invoice_line) > 0:
                                    if key not in grns:
                                        grns.append(key)
                                    if item_code in total_quantities:
                                        total_quantities[item_code] += quantity
                                        total_inv_quantity[
                                            item_code
                                        ] += check_invoice_line[0]["Quantity"]
                                    else:
                                        total_quantities[item_code] = quantity
                                        total_inv_quantity[item_code] = (
                                            check_invoice_line[0]["Quantity"]
                                        )
                                else:
                                    total_quantities[item_code] = quantity
                    if len(total_inv_quantity.keys()) > 0 and check_po_fully_invoiced(
                        document, total_inv_quantity, po_line_cleaned, db
                    ):
                        fully_invoiced = True
                    else:
                        for itemcode in total_quantities.keys():
                            po_unitprice = list(
                                filter(
                                    lambda d: d.get("itemCode") == itemcode,
                                    po_line_cleaned,
                                )
                            )[0]["UnitPrice"]
                            po_discamount = (
                                list(
                                    filter(
                                        lambda d: d.get("itemCode") == itemcode,
                                        po_line_cleaned,
                                    )
                                )[0]["DiscAmount"]
                                if "DiscAmount"
                                in list(
                                    filter(
                                        lambda d: d.get("itemCode") == mt_,
                                        po_line_cleaned,
                                    )
                                )[0]
                                else 0
                            )
                            po_discpercent = (
                                list(
                                    filter(
                                        lambda d: d.get("itemCode") == itemcode,
                                        po_line_cleaned,
                                    )
                                )[0]["DiscPercent"]
                                if "DiscPercent"
                                in list(
                                    filter(
                                        lambda d: d.get("itemCode") == mt_,
                                        po_line_cleaned,
                                    )
                                )[0]
                                else 0
                            )
                            if po_discamount > 0:
                                po_actualprice = po_unitprice - po_discamount
                            elif po_discpercent > 0:
                                po_actualprice = po_unitprice - (
                                    po_unitprice * (po_discpercent / 100)
                                )
                            else:
                                po_actualprice = po_unitprice
                            if itemcode in total_inv_quantity:
                                grn_total += total_quantities[itemcode] * po_actualprice
                            if (
                                itemcode not in total_inv_quantity
                                or total_quantities[itemcode] * po_actualprice
                                != total_inv_quantity[itemcode] * po_actualprice
                            ):
                                invo_itm_cd = (
                                    new_itm_mh[itemcode]["invo_itm_code"]
                                    if itemcode in new_itm_mh
                                    else itemcode
                                )
                                idDocumentLineItems = list(
                                    filter(
                                        lambda d: d.get("itemCode") == invo_itm_cd,
                                        invoice_line_cleaned,
                                    )
                                )
                                if len(idDocumentLineItems) > 0:
                                    update_LineItems(
                                        db,
                                        {
                                            "idDocumentLineItems": idDocumentLineItems[
                                                0
                                            ]["QuantityTag"],
                                            "ck_status": 2,
                                        },
                                        1,
                                        "GRN Line not matching with invoice line",
                                    )
                        polines = (
                            db.query(model.DocumentLineItems.itemCode)
                            .filter(model.DocumentLineItems.documentID == po_doc_id)
                            .distinct()
                        )
                        lines = []
                        if polines[0][0]:
                            for po in polines:
                                lines.append(po[0])
                        grnlist = []
                        for grn in grns:
                            for line in lines:
                                obj = {}
                                obj["PackingSlip"] = grn
                                obj["POLineNumber"] = line
                                grnlist.append(obj)
                        if len(grns) > 0:
                            db.query(model.Document).filter_by(
                                idDocument=invo_doc_id
                            ).update(
                                {
                                    "grn_documentID": [grn for grn in grns],
                                    "MultiPoList": json.dumps(grnlist),
                                }
                            )
                            db.commit()
                        else:
                            grn_found = 0
                            db.query(model.Document).filter_by(
                                idDocument=invo_doc_id
                            ).update({"grn_documentID": None, "MultiPoList": None})
            try:
                inv_sub_total = round(float(inv_sub_total), 2)
                grn_total = round(float(grn_total), 2)
                header_subtotal = round(float(header_subtotal), 2)
            except BaseException:
                inv_sub_total = inv_sub_total
                grn_total = grn_total
                header_subtotal = header_subtotal
            print(grn_total, inv_sub_total, header_subtotal)
            if grn_found == 1 and grn_total != inv_sub_total:
                hasGrnissues = True
            if inv_sub_total != header_subtotal:
                if abs(inv_sub_total - header_subtotal) > 0.1:
                    hastotalIssues = True
                else:
                    hastotalIssues = False

            if fully_invoiced:
                Over_all_ck_data_status = 10
            elif hasheaderissues:
                Over_all_ck_data_status = 8
            elif hastotalIssues:
                Over_all_ck_data_status = 6
            elif hasGrnissues:
                Over_all_ck_data_status = 7
            elif hasissues:
                Over_all_ck_data_status = 3
            else:
                Over_all_ck_data_status = 1
        else:
            po_grn_data = {}
            new_itm_mh = {}
            grn_found = 0
            auto_mapped = True
            Over_all_ck_data_status = 10
    except BaseException:
        print(traceback.format_exc())
        Over_all_ck_data_status = 0
    return po_grn_data, new_itm_mh, Over_all_ck_data_status, auto_mapped, grn_found


# check if fully invoiced


def check_po_fully_invoiced(document, grn_quantity, po_line, db):
    invs = (
        db.query(model.Document.idDocument)
        .filter(
            model.Document.idDocumentType == 3,
            model.Document.PODocumentID == document.PODocumentID,
            model.Document.documentStatusID.in_([7, 14, 2]),
        )
        .all()
    )
    used_grn_quantity = {}
    for inv in invs:
        if inv[0] and inv[0] != document.idDocument:
            invlines_quantity = (
                db.query(model.DocumentLineItems, model.DocumentLineItemTags)
                .options(
                    Load(model.DocumentLineItems).load_only(
                        "invoice_itemcode", "Value"
                    ),
                    Load(model.DocumentLineItemTags).load_only("TagName"),
                )
                .join(
                    model.DocumentLineItemTags,
                    model.DocumentLineItems.lineItemtagID
                    == model.DocumentLineItemTags.idDocumentLineItemTags,
                )
                .filter(
                    model.DocumentLineItems.documentID == inv[0],
                    model.DocumentLineItemTags.TagName == "Quantity",
                )
                .all()
            )
            for grn_qty in invlines_quantity:
                if grn_qty.DocumentLineItems.invoice_itemcode in used_grn_quantity:
                    used_grn_quantity[
                        grn_qty.DocumentLineItems.invoice_itemcode
                    ] += cln_amt(grn_qty.DocumentLineItems.Value)
                else:
                    used_grn_quantity[grn_qty.DocumentLineItems.invoice_itemcode] = (
                        cln_amt(grn_qty.DocumentLineItems.Value)
                    )
    for item_code in grn_quantity:
        if item_code in used_grn_quantity.keys():
            po_total_quantity = list(
                filter(lambda d: d.get("itemCode") == item_code, po_line)
            )[0]["RemainInventPhysical"]
            if po_total_quantity == 0:
                return True
        else:
            continue
    return False


# clean amount function


def cln_amt(amt):
    amt = str(amt)
    if len(amt) > 0:
        if len(re.findall("\\d+\\,\\d+\\d+\\.\\d+", amt)) > 0:
            cl_amt = re.findall("\\d+\\,\\d+\\d+\\.\\d+", amt)[0]
            cl_amt = float(cl_amt.replace(",", ""))
        elif len(re.findall("\\d+\\.\\d+", amt)) > 0:
            cl_amt = re.findall("\\d+\\.\\d+", amt)[0]
            cl_amt = float(cl_amt)
        elif len(re.findall("\\d+", amt)) > 0:
            cl_amt = re.findall("\\d+", amt)[0]
            cl_amt = float(cl_amt)
        else:
            cl_amt = amt
    else:
        cl_amt = amt
    return cl_amt


# checks the invoice with all rules


def invo_process(processing_data):
    try:
        Over_all_ck_data_status = 0
        invoice_data, po_data, grn_data, item_mapping, config = map(
            processing_data.get, ("invoice", "po", "grn", "item_mapping", "config")
        )
        grn_found = grn_data["grn_found"]
        skipGrnMatch = grn_data["skipGrnMatch"]
        grn_line_data = grn_data["grn_line_data"]
        invo_doc_id = invoice_data["invoice_id"]
        u_id = invoice_data["u_id"]
        invoice_header_data = invoice_data["invoice_header_data"]
        invoice_line_data = invoice_data["invoice_line_data"]
        po_doc_id = po_data["po_id"]
        po_line_data = po_data["po_line_data"]
        po_tag_map = config["po_tag_map"]
        ut_tol_percent = config["ut_tol_percent"]
        db = config["db"]
        config["GrnCreationType"]
        erp_vd_status = config["erp_vd_status"]
        config["ck_threshold"]
        qty_tol_percent = config["qty_tol_percent"]
        v_model_id = config["model_id"]
        document = config["document"]
        invoice_header_cleaned = prepare_inv_header_data(invoice_header_data)
        invoice_line_cleaned = prepare_line_data(invoice_line_data, po_tag_map)
        po_line_cleaned = prepare_line_data(po_line_data, po_tag_map)
        grn_line_cleaned = prepare_grn_line_data(grn_line_data, po_tag_map)
        item_mapping_cleaned = prepare_item_mapping(item_mapping, invo_doc_id, db)
        po_grn_data = {}
        new_itm_mh = {}
        if erp_vd_status == 2:
            # No Line items - only headers data check
            complete_batch_update(
                0, {"status": 1, "msg": "Matching Header Data Only!", "sub_status": 0}
            )
            Over_all_ck_data_status = check_only_header_data(
                po_line_cleaned, invoice_header_cleaned, invo_doc_id, db, u_id
            )
        elif erp_vd_status == 6:
            complete_batch_update(
                0,
                {
                    "status": 1,
                    "msg": "Matching single line PO with multiple line invoice!",
                    "sub_status": 0,
                },
            )
            # single line PO vs multiple line invoice:
            Over_all_ck_data_status = check_single_po_vs_multple_invoice_lines(
                v_model_id, invoice_line_cleaned, po_line_cleaned, invo_doc_id, db, u_id
            )
        elif erp_vd_status not in (2, 6):
            if (
                len(po_line_cleaned) >= len(invoice_line_cleaned)
                and len(invoice_line_cleaned) != 0
            ):
                (
                    po_grn_data,
                    new_itm_mh,
                    Over_all_ck_data_status,
                    auto_mapped,
                    grn_found,
                ) = twoWay_3Way_check(
                    document,
                    invo_doc_id,
                    po_doc_id,
                    invoice_header_cleaned,
                    invoice_line_cleaned,
                    po_line_cleaned,
                    grn_line_cleaned,
                    item_mapping_cleaned,
                    grn_found,
                    qty_tol_percent,
                    ut_tol_percent,
                    invoice_line_data,
                    skipGrnMatch,
                    db,
                    u_id,
                )
                if not auto_mapped:
                    Over_all_ck_data_status = 9
            elif len(invoice_line_cleaned) == 0:
                Over_all_ck_data_status = 5
            else:
                Over_all_ck_data_status = 34
                # metadata issue, - PO lines less than  invo line item!
    except BaseException:
        Over_all_ck_data_status = 0
        print(traceback.format_exc())
    return (
        po_grn_data,
        new_itm_mh,
        Over_all_ck_data_status,
        invoice_header_cleaned,
        grn_found,
    )


# check po is open or closed


def check_po_open(poid, db):
    try:
        headers = ERPapis.gettoken()
        pomodel = (
            db.query(model.DocumentModel.idDocumentModel)
            .filter(model.DocumentModel.modelID == "POMid000909")
            .scalar()
        )
        PurchId = (
            db.query(model.DocumentTagDef.idDocumentTagDef)
            .filter(
                model.DocumentTagDef.TagLabel == "PurchId",
                model.DocumentTagDef.idDocumentModel == pomodel,
            )
            .scalar()
        )
        PurchStatusId = (
            db.query(model.DocumentTagDef.idDocumentTagDef)
            .filter(
                model.DocumentTagDef.TagLabel == "PurchStatus",
                model.DocumentTagDef.idDocumentModel == pomodel,
            )
            .scalar()
        )
        totalAmountTag = (
            db.query(model.DocumentTagDef.idDocumentTagDef)
            .filter(
                model.DocumentTagDef.TagLabel == "TotalAmount",
                model.DocumentTagDef.idDocumentModel == pomodel,
            )
            .scalar()
        )
        value = (
            db.query(model.DocumentData.Value)
            .filter(
                model.DocumentData.documentTagDefID == PurchId,
                model.DocumentData.documentID == poid,
            )
            .scalar()
        )
        poheader_url = f"{os.getenv('DYNAMICS_ENDPOINT')}data/SER_PurchaseOrderHeaderDatas?cross-company=true&$top=1&$filter=(PurchId eq '{value}')"
        poheader_data = requests.get(poheader_url, headers=headers).json()
        povalue = poheader_data["value"]
        if len(povalue) > 0:
            total_amount = povalue[0]["TotalAmount"]
            if povalue[0]["PurchStatus"] != "Invoiced":
                db.query(model.Document).filter(
                    model.Document.idDocument == poid
                ).update(
                    {
                        "documentStatusID": 12,
                        "totalAmount": float(total_amount.replace(",", "")),
                    }
                )
            else:
                db.query(model.Document).filter(
                    model.Document.idDocument == poid
                ).update(
                    {
                        "documentStatusID": 13,
                        "totalAmount": float(total_amount.replace(",", "")),
                    }
                )
            db.query(model.DocumentData).filter(
                model.DocumentData.documentTagDefID == PurchStatusId,
                model.DocumentData.documentID == poid,
            ).update({"Value": povalue[0]["PurchStatus"]})
            db.query(model.DocumentData).filter(
                model.DocumentData.documentTagDefID == totalAmountTag,
                model.DocumentData.documentID == poid,
            ).update({"Value": povalue[0]["TotalAmount"]})
            db.commit()
            if povalue[0]["PurchStatus"] == "Invoiced":
                return False
    except BaseException:
        return True
    return True


# check invoice vendor matches PO vendor


def check_po_vendor(vendor_id, po_vendorac_id, db):
    inv_vendor_id = (
        db.query(model.VendorAccount.vendorID)
        .filter_by(idVendorAccount=int(vendor_id))
        .scalar()
    )
    po_vendor_id = (
        db.query(model.VendorAccount.vendorID)
        .filter_by(idVendorAccount=po_vendorac_id)
        .scalar()
    )
    inv_vendor_name = (
        db.query(model.Vendor.VendorCode).filter_by(idVendor=inv_vendor_id).scalar()
    )
    po_vendor_name = (
        db.query(model.Vendor.VendorCode).filter_by(idVendor=po_vendor_id).scalar()
    )
    if inv_vendor_name.lower() != po_vendor_name.lower():
        return False
    return True


# check Entity of invoice matches with PO


def check_po_match(entity, po, db):
    entityCode = (
        db.query(model.Entity.EntityCode)
        .filter(model.Entity.idEntity == entity)
        .scalar()
    )
    POEntitycodelist = po.split("-")
    if len(POEntitycodelist) > 0:
        POEntitycode = POEntitycodelist[0]
        if entityCode.lower() != POEntitycode.lower():
            return entityCode, False
    return "", True


# check duplicate invoice


def check_duplicate(id_doc, u_id, db):
    document = (
        db.query(model.Document).filter(model.Document.idDocument == id_doc).first()
    )
    if document:
        vendor_id = int(document.vendorAccountID)
        vendorId = (
            db.query(model.VendorAccount.vendorID)
            .filter_by(idVendorAccount=vendor_id)
            .scalar()
        )
        vendorName = (
            db.query(model.Vendor.VendorName).filter_by(idVendor=vendorId).scalar()
        )
        vendor_ids = (
            db.query(model.Vendor.idVendor)
            .filter(model.Vendor.VendorName == vendorName)
            .all()
        )
        vendor_accounts = (
            db.query(model.VendorAccount.idVendorAccount)
            .filter(model.VendorAccount.vendorID.in_([v[0] for v in vendor_ids]))
            .all()
        )
        doc = (
            db.query(model.Document.idDocument)
            .filter(
                func.lower(model.Document.docheaderID)
                == func.lower(document.docheaderID),
                model.Document.vendorAccountID.in_([va[0] for va in vendor_accounts]),
                model.Document.idDocumentType == 3,
                model.Document.documentStatusID.notin_([0, 10]),
            )
            .all()
        )
        if len(doc) > 1:
            # Duplicate Invoice!
            update_document_status(db, id_doc, 10, 1, "")
            type_s = "Duplicate Invoice"
            to_insert = {
                "documentID": id_doc,
                "documentSubStatusID": 1,
                "IsActive": 1,
                "type": type_s,
            }
            db.add(model.DocumentRuleupdates(**to_insert))
            db.commit()
            log_history(
                db,
                id_doc,
                1,
                10,
                "Duplicate Invoice!",
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                u_id,
            )
            return True
    return False


# check if po exists in erp


def add_po_header(ponumber, entityId, vendorAccountId, db):
    headers = ERPapis.gettoken()
    url = f"{os.getenv('DYNAMICS_ENDPOINT')}data/SER_PurchaseOrderHeaderDatas?cross-company=true&$top=1&$filter=(PurchId eq '{ponumber}')"
    resp = requests.get(url, headers=headers)
    docModelId = (
        db.query(model.DocumentModel.idDocumentModel)
        .filter(model.DocumentModel.modelID == "POMid000909")
        .scalar()
    )
    doc_tags = (
        db.query(model.DocumentTagDef.idDocumentTagDef, model.DocumentTagDef.TagLabel)
        .filter(model.DocumentTagDef.idDocumentModel == docModelId)
        .all()
    )
    docId = None
    if resp.status_code == 200:
        if len(resp.json()["value"]) > 0:
            r = resp.json()["value"][0]
            to_insert = {
                "idDocumentType": 1,
                "documentModelID": docModelId,
                "entityID": entityId,
                "vendorAccountID": vendorAccountId,
                "docheaderID": r["PurchId"],
                "documentStatusID": 12 if r["PurchStatus"] != "Invoiced" else 13,
                "PODocumentID": r["PurchId"],
                "IsRuleUpdated": 0,
                "CreatedOn": datetime.now(),
            }
            check = (
                db.query(model.Document.idDocument)
                .filter(
                    model.Document.docheaderID == r["PurchId"],
                    model.Document.idDocumentType == 1,
                    model.Document.entityID == entityId,
                    model.Document.vendorAccountID == vendorAccountId,
                )
                .first()
            )
            if check is None:
                res = model.Document(**to_insert)
                db.add(res)
                db.commit()
                docId = res.idDocument
                for dtag in doc_tags:
                    label = dtag.TagLabel
                    value = r[label] if label in r else ""
                    tagid = dtag.idDocumentTagDef
                    check = (
                        db.query(model.DocumentData)
                        .filter(
                            model.DocumentData.documentID == docId,
                            model.DocumentData.documentTagDefID == tagid,
                        )
                        .first()
                    )
                    if check is None:
                        data_insert = {
                            "documentID": docId,
                            "documentTagDefID": tagid,
                            "Value": value,
                            "IsUpdated": 0,
                            "isError": 0,
                            "CreatedOn": datetime.now(),
                        }
                        dt = model.DocumentData(**data_insert)
                        db.add(dt)
                        db.commit()
    return docId


# add po lines from erp
def add_po_line(docId, ponumber, db):
    headers = ERPapis.gettoken()
    url = f"{os.getenv('DYNAMICS_ENDPOINT')}data/SER_PurchaseOrderLinesDatas?cross-company=true&$top=1&$filter=(PurchId eq '{ponumber}')"
    resp = requests.get(url, headers=headers)
    docModelId = (
        db.query(model.DocumentModel.idDocumentModel)
        .filter(model.DocumentModel.modelID == "POMid000909")
        .scalar()
    )
    doc_tags = (
        db.query(model.DocumentLineItemTags)
        .filter(model.DocumentLineItemTags.idDocumentModel == docModelId)
        .all()
    )
    po_lines = []
    if resp.status_code == 200:
        if len(resp.json()["value"]) > 0:
            for r in resp.json()["value"]:
                line_number = r["LineNumber"]
                for dtag in doc_tags:
                    label = dtag.TagName
                    value = r[label] if label in r else ""
                    tagid = dtag.idDocumentLineItemTags
                    check = (
                        db.query(model.DocumentLineItems)
                        .filter(
                            model.DocumentLineItems.documentID == docId,
                            model.DocumentLineItems.lineItemtagID == tagid,
                        )
                        .first()
                    )
                    if check is None:
                        data_insert = {
                            "lineItemtagID": tagid,
                            "documentID": docId,
                            "Value": value,
                            "IsUpdated": 0,
                            "isError": 0,
                            "itemCode": line_number,
                            "invoice_itemcode": line_number,
                            "CreatedDate": datetime.now(),
                        }
                        dt = model.DocumentLineItems(**data_insert)
                        db.add(dt)
                        db.commit()
                        po_lines.append(data_insert)
    return po_lines


# check if grn exists in erp


def check_grn_in_erp(invoiceNumber, entity, vend_code, db):
    try:
        headers = ERPapis.gettoken()
        entityCode = (
            db.query(model.Entity.EntityCode)
            .filter(model.Entity.idEntity == entity)
            .scalar()
        )
        vend_acc = (
            db.query(model.VendorAccount.Account)
            .filter(model.VendorAccount.idVendorAccount == vend_code)
            .scalar()
        )
        check_statusurl = f"{os.getenv('DYNAMICS_ENDPOINT')}api/services/SER_POInvoiceServiceGroup/SER_POPartialInvoiceService/checkGRNNumber"
        status_checkdata = {
            "dataArea": entityCode,
            "vendAccount": vend_acc,
            "GRNNumber": invoiceNumber,
        }
        status_response = requests.post(
            check_statusurl, json=status_checkdata, headers=headers
        )
        status_response_json = status_response.json()
        status_response_json = json.loads(status_response_json)
        if (
            status_response_json["IsPosted"] == "No"
            and status_response_json["Message"] == "GRN Number Not Exists"
        ):
            return False
        return True
    except BaseException:
        return False


# add grn header from erp


def add_grn_header(invNumber, entityId, vendorAccountId, db):
    headers = ERPapis.gettoken()
    url = f"{os.getenv('DYNAMICS_ENDPOINT')}/data/SER_PackingSlipHeaderDatas?cross-company=true&$filter=PackingSlipId eq '{invNumber}'"
    resp = requests.get(url, headers=headers)
    docModelId = (
        db.query(model.DocumentModel.idDocumentModel)
        .filter(model.DocumentModel.modelID == "GRNMid000910")
        .scalar()
    )
    doc_tags = (
        db.query(model.DocumentTagDef.idDocumentTagDef, model.DocumentTagDef.TagLabel)
        .filter(model.DocumentTagDef.idDocumentModel == docModelId)
        .all()
    )
    docId = None
    if resp.status_code == 200:
        if len(resp.json()["value"]) > 0:
            r = resp.json()["value"][0]
            to_insert = {
                "idDocumentType": 2,
                "documentModelID": docModelId,
                "entityID": entityId,
                "vendorAccountID": vendorAccountId,
                "docheaderID": r["PackingSlipId"],
                "documentStatusID": 23,
                "PODocumentID": r["PurchId"],
                "IsRuleUpdated": 0,
                "CreatedOn": datetime.now(),
            }
            check = (
                db.query(model.Document.idDocument)
                .filter(
                    model.Document.docheaderID == r["PackingSlipId"],
                    model.Document.idDocumentType == 2,
                    model.Document.entityID == entityId,
                    model.Document.vendorAccountID == vendorAccountId,
                )
                .first()
            )
            if check is None:
                res = model.Document(**to_insert)
                db.add(res)
                db.commit()
                docId = res.idDocument
                for dtag in doc_tags:
                    label = dtag.TagLabel
                    value = r[label] if label in r else ""
                    tagid = dtag.idDocumentTagDef
                    check = (
                        db.query(model.DocumentData)
                        .filter(
                            model.DocumentData.documentID == docId,
                            model.DocumentData.documentTagDefID == tagid,
                        )
                        .first()
                    )
                    if check is None:
                        data_insert = {
                            "documentID": docId,
                            "documentTagDefID": tagid,
                            "Value": value,
                            "IsUpdated": 0,
                            "isError": 0,
                            "CreatedOn": datetime.now(),
                        }
                        dt = model.DocumentData(**data_insert)
                        db.add(dt)
                        db.commit()
    return docId


# add grn lines from erp


def add_grn_line(docId, invNumber, db):
    headers = ERPapis.gettoken()
    url = f"{os.getenv('DYNAMICS_ENDPOINT')}data/SER_PackingSlipLinesDatas?cross-company=true&$filter=PackingSlipId eq '{invNumber}'"
    resp = requests.get(url, headers=headers)
    docModelId = (
        db.query(model.DocumentModel.idDocumentModel)
        .filter(model.DocumentModel.modelID == "GRNMid000910")
        .scalar()
    )
    doc_tags = (
        db.query(model.DocumentLineItemTags)
        .filter(model.DocumentLineItemTags.idDocumentModel == docModelId)
        .all()
    )
    grn_lines = []
    if resp.status_code == 200:
        if len(resp.json()["value"]) > 0:
            for r in resp.json()["value"]:
                line_number = r["LineNum"]
                for dtag in doc_tags:
                    label = dtag.TagName
                    value = r[label] if label in r else ""
                    tagid = dtag.idDocumentLineItemTags
                    check = (
                        db.query(model.DocumentLineItems)
                        .filter(
                            model.DocumentLineItems.documentID == docId,
                            model.DocumentLineItems.lineItemtagID == tagid,
                        )
                        .first()
                    )
                    if check is None:
                        data_insert = {
                            "lineItemtagID": tagid,
                            "documentID": docId,
                            "Value": value,
                            "IsUpdated": 0,
                            "isError": 0,
                            "itemCode": line_number,
                            "invoice_itemcode": line_number,
                            "CreatedDate": datetime.now(),
                        }
                        dt = model.DocumentLineItems(**data_insert)
                        db.add(dt)
                        db.commit()
                        grn_lines.append(data_insert)
    return grn_lines


# main function


def single_doc_prc(id_doc, u_id):
    global complete_batch_cycle
    complete_batch_cycle.clear()
    flip_done = False
    db: Session = next(get_db())  # type: ignore
    try:
        complete_batch_update(
            0, {"status": 1, "msg": "Batch Process Started", "sub_status": 0}
        )
        enablegrncreation = 1
        invo_pro_data = {}
        grn_found = 0
        erp_tag_map = db.query(model.ERPTAGMAP).all()
        # get all erp to serina tag mapping
        for erpmp in erp_tag_map:
            po_tag_map[erpmp.cust_tag] = erpmp.serina_tag
        # get model id's for PO and GRN
        document = (
            db.query(model.Document).filter(model.Document.idDocument == id_doc).first()
        )
        if document:
            invId = document.docheaderID
            entityiD = document.entityID
            vendor_id = int(document.vendorAccountID)
            duplicate_status = check_duplicate(id_doc, u_id, db)
            if duplicate_status:
                complete_batch_update(
                    1,
                    {
                        "status": 1,
                        "msg": "Duplicate Invoice Found! Moved to Rejected Queue",
                        "sub_status": 1,
                    },
                )
                invo_pro_data[id_doc] = {
                    "map_item": {},
                    "po_grn_data": {},
                    "inline_rule": 0,
                    "complete_status": complete_batch_cycle,
                }
                return invo_pro_data
            # get vendor metadata:
            v_model_id = int(document.documentModelID)
            # get template based meta data: w.r.t model id
            fr_bh_df = (
                db.query(model.FRMetaData)
                .filter(model.FRMetaData.idInvoiceModel == v_model_id)
                .first()
            )
            bh_vd_status = fr_bh_df.batchmap
            erp_vd_status = fr_bh_df.erprule
            qty_tol_percent = fr_bh_df.QtyTol_percent
            ut_tol_percent = fr_bh_df.UnitPriceTol_percent
            GrnCreationType = fr_bh_df.GrnCreationType
            ck_threshold = int(fr_bh_df.AccuracyFeild)
            if bh_vd_status == 1:
                # PO base invo processing:
                invoice_header_data = (
                    db.query(model.DocumentData, model.DocumentTagDef)
                    .options(Load(model.DocumentTagDef).load_only("TagLabel"))
                    .join(
                        model.DocumentTagDef,
                        model.DocumentData.documentTagDefID
                        == model.DocumentTagDef.idDocumentTagDef,
                    )
                    .filter(model.DocumentData.documentID == id_doc)
                    .all()
                )
                invoice_lines_data = (
                    db.query(model.DocumentLineItems, model.DocumentLineItemTags)
                    .options(Load(model.DocumentLineItemTags).load_only("TagName"))
                    .join(
                        model.DocumentLineItemTags,
                        model.DocumentLineItems.lineItemtagID
                        == model.DocumentLineItemTags.idDocumentLineItemTags,
                    )
                    .filter(model.DocumentLineItems.documentID == id_doc)
                    .all()
                )
                itemuser_map_data = (
                    db.query(model.ItemUserMap)
                    .options(
                        Load(model.ItemUserMap).load_only(
                            "mappedinvoitemdescription",
                            "mappedinvoiceitemcode",
                            "documentID",
                        )
                    )
                    .filter(
                        model.ItemUserMap.vendoraccountID == vendor_id,
                        model.ItemUserMap.batcherrortype.in_([1, 5]),
                    )
                    .all()
                )
                po_doc = (
                    db.query(model.Document)
                    .filter(
                        model.Document.PODocumentID == document.PODocumentID,
                        model.Document.idDocumentType == 1,
                    )
                    .first()
                )
                flip_status = (
                    db.query(model.DocumentHistoryLogs)
                    .options(load_only("documentID", "documentdescription"))
                    .filter(
                        model.DocumentHistoryLogs.documentID == id_doc,
                        model.DocumentHistoryLogs.documentdescription
                        == "Po Flip Successfull",
                    )
                    .first()
                )
                # check if po in ERP
                if not po_doc:
                    docId = add_po_header(
                        document.PODocumentID, entityiD, vendor_id, db
                    )
                    if docId:
                        add_po_line(docId, document.PODocumentID, db)
                        po_doc = (
                            db.query(model.Document)
                            .filter(
                                model.Document.PODocumentID == document.PODocumentID,
                                model.Document.idDocumentType == 1,
                            )
                            .first()
                        )
                if po_doc:
                    PurchId = (
                        db.query(model.DocumentTagDef.idDocumentTagDef)
                        .filter(
                            model.DocumentTagDef.TagLabel == "PurchaseOrder",
                            model.DocumentTagDef.idDocumentModel == v_model_id,
                        )
                        .scalar()
                    )
                    db.query(model.DocumentData).filter(
                        model.DocumentData.documentID == document.idDocument,
                        model.DocumentData.documentTagDefID == PurchId,
                    ).update({"isError": 0, "ErrorDesc": 0})
                    db.commit()
                    po_status_check = check_po_open(po_doc.idDocument, db)
                    if not po_status_check:
                        update_document_status(db, id_doc, 10, 1, "PO is Closed!")
                        log_history(
                            db,
                            id_doc,
                            1,
                            10,
                            "PO is closed or fully booked!",
                            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                            u_id,
                        )
                        complete_batch_update(
                            0,
                            {
                                "status": 0,
                                "msg": "PO is Fully Invoiced, Moving invoice to rejected queue!",
                                "sub_status": 0,
                            },
                        )
                        invo_pro_data[id_doc] = {
                            "map_item": {},
                            "po_grn_data": {},
                            "inline_rule": 0,
                            "complete_status": complete_batch_cycle,
                        }
                        return invo_pro_data
                    po_vendor_check = check_po_vendor(
                        vendor_id, po_doc.vendorAccountID, db
                    )
                    if not po_vendor_check:
                        update_document_status(db, id_doc, 4, 7, "PO Not Found")
                        log_history(
                            db,
                            id_doc,
                            7,
                            4,
                            "PO Not Found!",
                            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                            u_id,
                        )
                        complete_batch_update(
                            0,
                            {
                                "status": 0,
                                "msg": "Selected PO does not belong to this Vendor! Please Check.",
                                "sub_status": 0,
                            },
                        )
                        invo_pro_data[id_doc] = {
                            "map_item": {},
                            "po_grn_data": {},
                            "inline_rule": 0,
                            "complete_status": complete_batch_cycle,
                        }
                        return invo_pro_data
                    complete_batch_update(
                        0, {"status": 1, "msg": "PO Found!", "sub_status": 0}
                    )
                    code, entity_check = check_po_match(
                        entityiD, po_doc.docheaderID, db
                    )
                    if not entity_check:
                        update_document_status(
                            db,
                            id_doc,
                            4,
                            40,
                            "Selected Entity does not match with PO Format!",
                        )
                        log_history(
                            db,
                            id_doc,
                            40,
                            4,
                            "Selected Entity does not match with PO Format!",
                            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                            u_id,
                        )
                        complete_batch_update(
                            1,
                            {
                                "status": 0,
                                "msg": f"Wrong Entity! Selected Entity: {code}, PO : {po_doc.docheaderID}.",
                                "sub_status": 1,
                            },
                        )
                        invo_pro_data[id_doc] = {
                            "map_item": {},
                            "po_grn_data": {},
                            "inline_rule": 0,
                            "complete_status": complete_batch_cycle,
                        }
                        return invo_pro_data
                    po_doc_id = int(po_doc.idDocument)
                    po_line_data = (
                        db.query(model.DocumentLineItems, model.DocumentLineItemTags)
                        .options(Load(model.DocumentLineItemTags).load_only("TagName"))
                        .join(
                            model.DocumentLineItemTags,
                            model.DocumentLineItems.lineItemtagID
                            == model.DocumentLineItemTags.idDocumentLineItemTags,
                        )
                        .filter(model.DocumentLineItems.documentID == po_doc_id)
                        .all()
                    )
                    po_header_data = (
                        db.query(model.DocumentData, model.DocumentTagDef)
                        .options(Load(model.DocumentTagDef).load_only("TagLabel"))
                        .join(
                            model.DocumentTagDef,
                            model.DocumentData.documentTagDefID
                            == model.DocumentTagDef.idDocumentTagDef,
                        )
                        .filter(model.DocumentData.documentID == po_doc_id)
                        .all()
                    )
                    grn_line_data = {}
                    grn_doc_id = None
                    grn_found = 0
                    ismultigrn = False
                    skipGrnMatch = False
                    grnNotApproved = False
                    grn_doc_all = db.query(
                        model.Document.idDocument,
                        model.Document.documentModelID,
                        model.Document.docheaderID,
                        model.Document.documentStatusID,
                        model.Document.documentsubstatusID,
                        model.Document.JournalNumber,
                    ).filter(
                        model.Document.idDocumentType == 2,
                        model.Document.PODocumentID == document.PODocumentID,
                        model.Document.documentStatusID.in_([7, 20, 23]),
                        or_(
                            model.Document.docheaderID.like(f"%{invId}%"),
                            model.Document.docheaderID.like(
                                f"%{document.PODocumentID}%"
                            ),
                        ),
                    )
                    if len(grn_doc_all.all()) > 0:
                        grn_found = 1
                        try:
                            # GRN doc ID found!
                            grn_doc = []
                            if len(grn_doc_all.all()) > 1:
                                grn_docId = (
                                    db.query(model.Document.grn_documentID)
                                    .filter(model.Document.idDocument == id_doc)
                                    .scalar()
                                )
                                if grn_docId is None:
                                    grn_doc = grn_doc_all.all()
                                else:
                                    grn_doc = grn_doc_all.filter(
                                        model.Document.docheaderID.in_(grn_docId)
                                    ).all()
                                ismultigrn = True
                            elif len(grn_doc_all.all()) == 1:
                                grn_doc = grn_doc_all.all()
                                ismultigrn = False
                            else:
                                grn_found = 0
                            if grn_found == 1:
                                for grn_doc_id in grn_doc:
                                    if grn_doc_id[3] == 20:
                                        grnNotApproved = True
                                    grn_line = (
                                        db.query(
                                            model.DocumentLineItems,
                                            model.DocumentLineItemTags,
                                        )
                                        .options(
                                            Load(model.DocumentLineItemTags).load_only(
                                                "TagName"
                                            )
                                        )
                                        .join(
                                            model.DocumentLineItemTags,
                                            model.DocumentLineItems.lineItemtagID
                                            == model.DocumentLineItemTags.idDocumentLineItemTags,
                                        )
                                        .filter(
                                            model.DocumentLineItems.documentID
                                            == grn_doc_id[0]
                                        )
                                        .all()
                                    )
                                    grn_line_data[grn_doc_id[2]] = grn_line
                                    if grn_doc_id[1] in [1, 327]:
                                        skipGrnMatch = False
                                    else:
                                        skipGrnMatch = True
                        except Exception as e:
                            update_document_status(db, id_doc, 1, 1, e)
                            created_date = datetime.utcnow().strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                            log_history(db, id_doc, 1, 1, e, created_date, u_id)
                            grn_line_data = {}
                            grn_found = 0
                    else:
                        # GRN data not found:
                        status = check_grn_in_erp(invId, entityiD, vendor_id, db)
                        if status:
                            doc_id = add_grn_header(invId, entityiD, vendor_id, db)
                            if doc_id:
                                add_grn_line(doc_id, invId, db)
                            grn_line_data = {}
                            grn_found = 1
                        else:
                            grn_line_data = {}
                            grn_found = 0
                    if flip_status is not None:
                        try:
                            flip_done = True
                            complete_batch_update(
                                0,
                                {
                                    "status": 1,
                                    "msg": "PO Flip Completed for the invoice!",
                                    "sub_status": 0,
                                },
                            )
                        except BaseException:
                            flip_done = False
                    invoice_data = {
                        "invoice_id": id_doc,
                        "invoice_header_data": invoice_header_data,
                        "invoice_line_data": invoice_lines_data,
                        "u_id": u_id,
                    }
                    po_data = {
                        "po_id": po_doc_id,
                        "po_header_data": po_header_data,
                        "po_line_data": po_line_data,
                    }
                    grn_data = {
                        "grn_id": grn_doc_id,
                        "grn_found": grn_found,
                        "grn_header_data": [],
                        "grn_line_data": grn_line_data,
                        "skipGrnMatch": skipGrnMatch,
                    }
                    addition_config = {
                        "document": document,
                        "po_tag_map": po_tag_map,
                        "model_id": v_model_id,
                        "GrnCreationType": GrnCreationType,
                        "ck_threshold": ck_threshold,
                        "erp_vd_status": erp_vd_status,
                        "qty_tol_percent": qty_tol_percent,
                        "ut_tol_percent": ut_tol_percent,
                        "db": db,
                    }
                    processing_data = {
                        "invoice": invoice_data,
                        "po": po_data,
                        "grn": grn_data,
                        "item_mapping": itemuser_map_data,
                        "config": addition_config,
                    }
                    (
                        po_grn_data,
                        new_itm_mh,
                        Over_all_ck_data_status,
                        invoice_header_cleaned,
                        grn_found,
                    ) = invo_process(processing_data)
                    invo_pro_data[id_doc] = {
                        "map_item": new_itm_mh,
                        "po_grn_data": po_grn_data,
                        "inline_rule": 1,
                        "complete_status": complete_batch_cycle,
                        "erp_vd_st": erp_vd_status,
                    }
                    batch_update_db(
                        invo_pro_data,
                        Over_all_ck_data_status,
                        GrnCreationType,
                        grn_found,
                        id_doc,
                        enablegrncreation,
                        ismultigrn,
                        po_doc_id,
                        invoice_header_cleaned,
                        grnNotApproved,
                        flip_done,
                        db,
                        u_id,
                    )
                else:
                    duplicate_status = check_duplicate(id_doc, u_id, db)
                    if duplicate_status:
                        complete_batch_update(
                            1,
                            {
                                "status": 1,
                                "msg": "Duplicate Invoice Found! Moved to Rejected Queue",
                                "sub_status": 1,
                            },
                        )
                        invo_pro_data[id_doc] = {
                            "map_item": {},
                            "po_grn_data": {},
                            "inline_rule": 0,
                            "complete_status": complete_batch_cycle,
                        }
                        return invo_pro_data
                    complete_batch_update(
                        7, {"status": 0, "msg": "PO not Found!", "sub_status": 7}
                    )
                    # PO NOT FOUND!
                    update_document_status(db, id_doc, 4, 7, "PO Not Found")
                    type_s = "Po Not Found - " + str(document.PODocumentID)
                    to_insert = {
                        "documentID": id_doc,
                        "documentSubStatusID": 7,
                        "IsActive": 1,
                        "type": type_s,
                    }
                    db.add(model.DocumentRuleupdates(**to_insert))
                    db.commit()
                    log_history(
                        db,
                        id_doc,
                        7,
                        4,
                        "PO Not Found!",
                        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        u_id,
                    )
                    invo_pro_data[id_doc] = {
                        "map_item": {},
                        "po_grn_data": {},
                        "inline_rule": 0,
                        "complete_status": complete_batch_cycle,
                    }

            else:
                duplicate_status = check_duplicate(id_doc, u_id, db)
                if duplicate_status:
                    complete_batch_update(
                        1,
                        {
                            "status": 0,
                            "msg": "Duplicate Invoice Found! Moved to Rejected Queue",
                            "sub_status": 1,
                        },
                    )
                    invo_pro_data[id_doc] = {
                        "map_item": {},
                        "po_grn_data": {},
                        "inline_rule": 0,
                        "complete_status": complete_batch_cycle,
                    }
                    return invo_pro_data
                complete_batch_update(
                    0, {"status": 1, "msg": "Batch not Required!", "sub_status": 0}
                )
                docDesc = "No Batch Required"
                update_document_status(db, id_doc, 2, 31, None)
                log_history(
                    db,
                    id_doc,
                    31,
                    2,
                    docDesc,
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    u_id,
                )
                invo_pro_data[id_doc] = {
                    "map_item": {},
                    "po_grn_data": {},
                    "inline_rule": 0,
                    "complete_status": complete_batch_cycle,
                }
        else:
            complete_batch_update(
                0, {"status": 0, "msg": "Invoice not Found!", "sub_status": 0}
            )
            print("NO invo docId FOUND!")
            invo_pro_data[id_doc] = {
                "map_item": {},
                "po_grn_data": {},
                "inline_rule": 0,
                "complete_status": complete_batch_cycle,
            }
    except Exception as mn_error:
        db.rollback()
        complete_batch_update(
            1, {"status": 0, "msg": "Batch ran to an Exception!", "sub_status": 1}
        )
        print(traceback.format_exc())
        invo_pro_data[id_doc] = {
            "map_item": {},
            "po_grn_data": {},
            "inline_rule": 0,
            "complete_status": complete_batch_cycle,
        }
        update_document_status(db, id_doc, 1, 1, "Batch Exception")
        log_history(
            db,
            id_doc,
            1,
            1,
            mn_error,
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            u_id,
        )
    return invo_pro_data

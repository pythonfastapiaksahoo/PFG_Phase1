import re
from pfg_app import model
from pfg_app.FROps.corp_payloadValidation import payload_dbUpdate
from pfg_app.FROps.customCall import date_cnv
from pfg_app.session.session import SQLALCHEMY_DATABASE_URL, get_db
from sqlalchemy import func
from sqlalchemy.orm.attributes import flag_modified

import pandas as pd
from pfg_app.crud.CorpIntegrationCrud import corp_update_docHistory
from pfg_app.logger_module import logger
import traceback
import pytz as tz
tz_region = tz.timezone("US/Pacific")
# doc_id = 144
# userID = 1

from datetime import datetime

def check_date_format(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False
    

def names_match(name1, name2):
    name1 = (name1 or "").strip().lower()  # Convert None to empty string
    name2 = (name2 or "").strip().lower()
    return sorted(name1.split()) == sorted(name2.split())


def email_belongs_to_name(name, email):
    email_prefix = email.split("@")[0].lower()  # Extract email username
    name_parts = set(name.lower().split())  # Convert name into a set of lowercase words
    return any(part in email_prefix for part in name_parts)  # Check if any name part is in the email

# def is_amount_approved(amount: float, title: str) -> bool:
#     approval_limits = {
#         (0, 24999): {"Supervisor", "Manager"},
#         (25000, 74999): {"Senior Manager", "Sr. Manager"},
#         (75000, 499999): {"Director", "Regional Manager", "General Manager"},
#         (500000, float("inf")): {"Managing Director", "VP", "Vice President"},
#     }
    
#     title = title.strip().lower()
#     title_variants = {
#         "senior manager": "Senior Manager",
#         "sr. manager": "Senior Manager",
#         "sr manager": "Senior Manager",
#         "vice president": "VP"
#     }
    
#     normalized_title = title_variants.get(title, title.title())
    
#     for (lower, upper), allowed_titles in approval_limits.items():
#         if lower <= amount <= upper:
#             return normalized_title in allowed_titles
    
#     return False

def is_amount_approved(amount: float, title: str) -> bool:
    approval_limits = {
        (0, 24999): {"Supervisor", "Manager"},
        (0, 74999): {"Senior Manager", "Sr. Manager"},
        (0, 499999): {"Director", "Regional Manager", "General Manager"},
        (0, float("inf")): {"Managing Director", "VP", "Vice President"},
    }
    
    title = title.strip().lower()
    title_variants = {
        "senior manager": "Senior Manager",
        "sr. manager": "Senior Manager",
        "sr manager": "Senior Manager",
        "vice president": "VP"
    }

    # Check if any key in title_variants is a substring of the title
    for key, normalized in title_variants.items():
        if key in title:
            normalized_title = normalized
            break
    else:
        normalized_title = title.title()
    
    for (lower, upper), allowed_titles in approval_limits.items():
        if lower <= amount <= upper:
            return normalized_title in allowed_titles
    
    return False




def update_Credit_data(doc_id, db):
    try:
        # Fetch the records from all tables using joins
        record = db.query(model.corp_document_tab, model.corp_coding_tab, model.corp_docdata) \
                .join(model.corp_coding_tab, model.corp_coding_tab.corp_doc_id == model.corp_document_tab.corp_doc_id) \
                .join(model.corp_docdata, model.corp_docdata.corp_doc_id == model.corp_document_tab.corp_doc_id) \
                .filter(model.corp_document_tab.corp_doc_id == doc_id) \
                .first()

        if record:
            # Update the values for corp_document_tab
            if record[0]:  # Checking if the corp_document_tab record exists
                record[0].invoicetotal = round(-abs(record[0].invoicetotal), 2) if record[0].invoicetotal else None
                record[0].gst = round(-abs(record[0].gst), 2) if record[0].gst else None

            # Update the values for corp_coding_tab
            if record[1]:  # Checking if the corp_coding_tab record exists
                record[1].invoicetotal = round(-abs(record[1].invoicetotal), 2) if record[1].invoicetotal else None
                record[1].gst = round(-abs(record[1].gst), 2) if record[1].gst else None

                # Update amount values inside coding_details
                if record[1].coding_details:
                    for key, value in record[1].coding_details.items():
                        if 'amount' in value and value['amount']:
                            value['amount'] = round(-abs(value['amount']), 2)

                    # Mark JSON column as modified
                    flag_modified(record[1], "coding_details")

            # Update the values for corp_docdata
            if record[2]:  # Checking if the corp_docdata record exists
                fields = [
                    "invoicetotal", "subtotal", "bottledeposit", "shippingcharges",
                    "litterdeposit", "gst", "pst", "pst_sk", "pst_bc",
                    "ecology_fee", "misc"
                ]
                
                for field in fields:
                    value = getattr(record[2], field)  # Get current value
                    if value:  # Check if value is not None
                        setattr(record[2], field, round(-abs(value), 2))  # Convert to negative and round

            # Commit changes for all records in a single call
            db.commit()
            print(f"Updated invoicetotal, gst, and other fields for docID {doc_id} successfully.")
        else:
            logger.info(f"No record found for docID {doc_id}")

    except Exception as e:
        logger.error(f"Error in validate_corpdoc: {e}")
        logger.info(traceback.format_exc())



def validate_corpdoc(doc_id,userID,skipConf,db):
    timeStmp = datetime.now(tz_region)
    return_status = {}
    invoTotal_status = 0
    invoTotal_msg = ""
    gst_status = 0
    gst_msg = ""
    invDate_status = 0
    invDate_msg = ""
    subTotal_status = 0
    subTotal_msg = ""
    document_type_status = 0
    document_type_msg = ""
    approvrd_ck = 1
    cod_lnMatch = 0
    currency_ck = 0
    currency_ck_msg = ""
    approval_check_req = 1
    validation_status_ck = 1
    credit_ck = 0
    gst_15_ck =0
    try:
        corp_document_data = (
            db.query(model.corp_document_tab)
            .filter(model.corp_document_tab.corp_doc_id == doc_id)
            .all()
        )

        # Convert ORM objects to dictionaries
        df_corp_document = pd.DataFrame([{k: v for k, v in vars(row).items() if k != "_sa_instance_state"} for row in corp_document_data])
    
        docStatus = list(df_corp_document['documentstatus'])[0]
        docSubStatus = list(df_corp_document['documentsubstatus'])[0]
        vendor_id = list(df_corp_document['vendor_id'])[0]
        document_type = list(df_corp_document['document_type'])[0]
        invoice_id = list(df_corp_document['invoice_id'])[0]
        vendor_code = list(df_corp_document['vendor_code'])[0]
        logger.info(f"doc_id: {doc_id}, vendor_id: {vendor_id}, document_type: {document_type}, invoice_id: {invoice_id}")
        sentToPPlSft = {
            7: "Sent to PeopleSoft",
            29: "Voucher Created",
            30: "Voucher Not Found",
            27: "Quick Invoice",
            14: "Posted In PeopleSoft",
            28: "Recycled Invoice",
        }

        if docStatus in sentToPPlSft.keys():
            if isinstance(docStatus, int):
                return_status[sentToPPlSft[docStatus]] = {
                    "status": 1,
                    "StatusCode":0,
                    "response": ["Invoice sent to peopleSoft"],
                }
                return return_status
        elif docStatus == 10 and docSubStatus in [153,154,155,156,157,158,159]:
            # if invoSubStatus == 13:
            return_status["Rejected"] = {
                "status": 1,
                "StatusCode":0,
                "response": ["Invoice rejected by user"],
            }
            return return_status
            
        elif docStatus in (26,25):

            if vendor_id is not None:
                if vendor_code is not None:
                    if vendor_id == 0:
                        try: 
                            vendor_id_update = int(list(df_corp_metadata['vendorid'])[0])
                            if type(vendor_id_update) == int:
                                vendor_id = vendor_id_update
                        except Exception as e:
                            logger.info(f"Error in updating vendorid: {e}")

                        
                docStatus = 4
                docSubStatus = 11
                db.query(model.corp_document_tab).filter( model.corp_document_tab.corp_doc_id == doc_id
                    ).update(
                        {
                            model.corp_document_tab.documentstatus: docStatus,  # noqa: E501
                            model.corp_document_tab.documentsubstatus: docSubStatus,  # noqa: E501
                            model.corp_document_tab.last_updated_by: userID,
                            model.corp_document_tab.updated_on: timeStmp,
                            model.corp_document_tab.vendor_id: vendor_id,

                        }
                    )
                db.commit()
            else:
                
                print("Vendor mapping required")
                return_status["Status overview"] = {"status": 0,
                                                "StatusCode":0,
                                                "response": [
                                                                "Vendor mapping required"
                                                            ],
                                                    }
                logger.info(f"return corp validations(ln 37): {return_status}")
                return return_status
            
        elif docStatus in (10,):
            print("Document rejected")
            return_status["Status overview"] = {"status": 0,
                                            "StatusCode":0,
                                            "response": [
                                                            "Document rejected."
                                                        ],
                                                    }
            logger.info(f"return corp validations(ln 48): {return_status}")
            return return_status
            
        if docStatus in (32,2,4,24):
            
            if docSubStatus == 134:
                nocoding_ck = 0
                corp_coding_data = (
                    db.query(model.corp_coding_tab)
                    .filter(model.corp_coding_tab.corp_doc_id == doc_id)
                    .all()
                )

                # Check if any records exist
                if not corp_coding_data:
                    logger.info(f"docID: {doc_id} - No records found for the given doc_id.")
                else:
                    for row in corp_coding_data:
                        coding_details = row.coding_details  # Assuming this is a dictionary

                        if coding_details and isinstance(coding_details, dict) and len(coding_details) > 0:
                            logger.info(f"Records found in coding_details- docID: {doc_id} - : {coding_details}")
                            nocoding_ck = 1
                        else:
                            logger.info(f"docID: {doc_id} - coding_details is empty or not a dictionary.")
                    #--
                    if nocoding_ck == 1:
                        docSubStatus = 7
                    else:
                        logger.info(f"docID: {doc_id} - Coding - No Coding Lines Found")
                        return_status["Status overview"] = {"status": 0,
                                                "StatusCode":0,
                                                "response": [
                                                                "Coding - No Coding Lines Found"
                                                            ],
                                                        }
                        logger.info(f"return corp validations(ln 61): {return_status}")
                        return return_status
            elif docSubStatus == 130:
                return_status["Status overview"] = {"status": 0,
                                            "StatusCode":0,
                                            "response": [
                                                            "Invoice - Document missing"
                                                        ],
                                                    }
                logger.info(f"docID: {doc_id} - return corp validations(ln 70): {return_status}")
                return return_status
            
            
            else:

                #----------------------------
                vdr_id_map = 0
                if vendor_id in [None,0]:
                    
                    # if vendor_id is not None:
                    if vendor_code is not None:
                        if vendor_id == 0:
                            try: 
                                corp_VrdID_qry = (
                                            db.query(model.corp_metadata)
                                            .filter(model.corp_metadata.vendorcode == vendor_code)
                                            .all()
                                        )
                                df_corp_VrdID = pd.DataFrame([row.__dict__ for row in corp_VrdID_qry])
                                vendor_id_update = int(list(df_corp_VrdID['vendorid'])[0])
                                if type(vendor_id_update) == int:
                                    vendor_id = vendor_id_update
                                    corp_metadata_qry = (
                                                db.query(model.corp_metadata)
                                                .filter(model.corp_metadata.vendorid == vendor_id)
                                                .all()
                                            )
                                    df_corp_metadata = pd.DataFrame([row.__dict__ for row in corp_metadata_qry])
                                    db.query(model.corp_document_tab).filter( model.corp_document_tab.corp_doc_id == doc_id
                                        ).update(
                                            {
                                                
                                                model.corp_document_tab.vendor_id: vendor_id,

                                            }
                                        )
                                    db.commit()
                                    vdr_id_map = 1
                                    docStatus = 4
                                    docSubStatus = 11
                                else:
                                    return_status["Vendor mapping required"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Vendor mapping required"
                                                                ],
                                                        }
                                    logger.info(f"return corp validations(ln 70): {return_status}")
                                    return return_status
                            except Exception as e:
                                logger.info(f"Error in updating vendorid: {e}")

                    
                    if vdr_id_map==0:
                        docStatus = 26
                        docSubStatus = 107
                        db.query(model.corp_document_tab).filter( model.corp_document_tab.corp_doc_id == doc_id
                            ).update(
                                {
                                    model.corp_document_tab.documentstatus: docStatus,  # noqa: E501
                                    model.corp_document_tab.documentsubstatus: docSubStatus,  # noqa: E501
                                    model.corp_document_tab.last_updated_by: userID,
                                    model.corp_document_tab.updated_on: timeStmp,

                                }
                            )
                        db.commit()
                        return_status["Status overview"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Vendor Mapping required"
                                                                ],
                                                            }
                        logger.info(f"return corp validations(ln 70): {return_status}")
                        return return_status
                #----------------------------
                #date validation:
                try:
                    corp_coding_data = (
                        db.query(model.corp_coding_tab)
                        .filter(model.corp_coding_tab.corp_doc_id == doc_id)
                        .all()
                    )

                    # Query corp_docdata
                    corp_docdata = (
                        db.query(model.corp_docdata)
                        .filter(model.corp_docdata.corp_doc_id == doc_id)
                        .all()
                    )

                    df_corp_coding = pd.DataFrame([row.__dict__ for row in corp_coding_data])

                    df_corp_docdata = pd.DataFrame([row.__dict__ for row in corp_docdata])
                    # if (document_type is not None):
                    #     if document_type !="":
                    #         if str(document_type).lower() in ('invoice','credit'):
                    #             print(document_type)

                    # Get metadata for the document:
                    

                    
                    
                    
                    # Check for mandatory fields:
                    mand_invoTotal = list(df_corp_docdata['invoicetotal'])[0]
                    mand_gst = list(df_corp_docdata['gst'])[0]
                    mand_invDate = list(df_corp_docdata['invoice_date'])[0]
                    mand_subTotal = list(df_corp_docdata['subtotal'])[0]
                    mand_document_type = list(df_corp_docdata['document_type'])[0]
                    mand_currency = list(df_corp_docdata['currency'])[0]
                    try:
                        corp_metadata_qry = (
                                    db.query(model.corp_metadata)
                                    .filter(model.corp_metadata.vendorid == vendor_id)
                                    .all()
                                )
                        df_corp_metadata = pd.DataFrame([row.__dict__ for row in corp_metadata_qry])
                                
                    except Exception as e:
                        logger.info(f"Error in getting metadata: {e}")
                    
                    if not df_corp_metadata.empty:
                        metadata_currency = list(df_corp_metadata['currency'])[0]
                        date_format = list(df_corp_metadata['dateformat'])[0]
                        if check_date_format(mand_invDate) == False:
                            req_date, date_status = date_cnv(mand_invDate, date_format)
                            if date_status == 1:
                                invDate_msg = "Valid Date Format"
                                invDate_status = 1
                                #update date to table:
                                # Update corp_document_tab
                                db.query(model.corp_document_tab).filter(
                                    model.corp_document_tab.corp_doc_id == doc_id
                                ).update({model.corp_document_tab.invoice_date: req_date})

                                # Update corp_docdata
                                db.query(model.corp_docdata).filter(
                                    model.corp_docdata.corp_doc_id == doc_id
                                ).update({model.corp_docdata.invoice_date: req_date})

                                db.commit()
                                return_status["Invoice date validation"] = {"status": 1,
                                                        "StatusCode":0,
                                                        "response": [
                                                                        "Success."
                                                                    ],
                                                                }
                                
                            else:
                                invDate_msg = "Invalid Date Format"
                                invDate_status = 0
                                return_status["Invoice date validation"] = {"status": 0,
                                                        "StatusCode":0,
                                                        "response": [
                                                                        "Invalid Date Format."
                                                                    ],
                                                                }
                        else:
                            return_status["Invoice date validation"] = {"status": 1,
                                                        "StatusCode":0,
                                                        "response": [
                                                                        "Success."
                                                                    ],
                                                                }
                            invDate_msg = "Valid Date Format"
                            invDate_status = 1
                    else:
                        return_status["Invoice date validation"] = {"status": 0,
                                                        "StatusCode":0,
                                                        "response": [
                                                                        "Metadata is not valid."
                                                                    ],
                                                                }
                        invDate_msg = "Valid Date Format"
                        invDate_status = 0
                except Exception as e:
                    logger.error(f"Error in validate_corpdoc: {e}")
                    logger.info(traceback.format_exc())
                    invDate_msg = "Please review Date format"
                    invDate_status = 0  
                    return_status["Invoice date validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Error:"+str(e)+"."
                                                                ],
                                                            }
                    
                try:
                    cl_invoID =  re.sub(r'[^a-zA-Z0-9\s]', '', invoice_id)
                    if len(cl_invoID)==0:
                        return_status["Invoice mandatory fields validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Invoice ID not valid."
                                                                ],
                                                            }
                        return return_status
                    else:
                        #clean invoice ID
                        try:
                            if cl_invoID != invoice_id:
                                db.query(model.corp_document_tab).filter(
                                    model.corp_document_tab.corp_doc_id == doc_id
                                ).update(
                                    {
                                        model.corp_document_tab.invoice_id: cl_invoID,
                                        model.corp_document_tab.last_updated_by: userID,
                                        model.corp_document_tab.updated_on: timeStmp,
                                    }
                                )

                                db.query(model.corp_docdata).filter(
                                    model.corp_docdata.corp_doc_id == doc_id
                                ).update(
                                    {model.corp_docdata.invoice_id: cl_invoID}
                                )

                                db.query(model.corp_coding_tab).filter(
                                    model.corp_coding_tab.corp_doc_id == doc_id
                                ).update(
                                    {model.corp_coding_tab.invoice_id: cl_invoID}
                                )

                                # Commit once after all updates
                                db.commit()
                        except Exception as e:
                            logger.info(f"Error in cleaning invoice ID: {e}")
                            logger.info(traceback.format_exc())

                except Exception as e:
                            logger.info(f"Error in cleaning invoice ID: {e}")
                            logger.info(traceback.format_exc())

                dupCk_document_data = (
                db.query(model.corp_document_tab)
                .filter(
                    model.corp_document_tab.corp_doc_id != doc_id,
                    model.corp_document_tab.vendor_id == vendor_id,
                    model.corp_document_tab.documentstatus != 10,
                    model.corp_document_tab.invoice_id == invoice_id
                )
                .all()
                )

                
                df_dupCk_document = pd.DataFrame([
                    {col: getattr(row, col) for col in model.corp_document_tab.__table__.columns.keys()}
                    for row in dupCk_document_data
                ])

                if len(df_dupCk_document)>0:
                    # duplicate invoice
                    docStatus = 32
                    documentdesc = f"Duplicate invoice"
                    substatus = 128
                    return_status["Status overview"] = {"status": 0,
                                                "StatusCode":0,
                                                "response": [
                                                                "Duplicate invoice."
                                                            ],
                                                        }
                    
                    corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,substatus)
                    db.query(model.corp_document_tab).filter( model.corp_document_tab.corp_doc_id == doc_id
                    ).update(
                        {
                            model.corp_document_tab.documentstatus: docStatus,  # noqa: E501
                            model.corp_document_tab.documentsubstatus: substatus,  # noqa: E501
                            model.corp_document_tab.last_updated_by: userID,
                            model.corp_document_tab.updated_on: timeStmp,

                        }
                    )
                    db.commit()
                    logger.info(f"return corp validations(ln 111): {return_status}")
                    return return_status
                else:
                    try:
                        return_status["Invoice duplicate validation"] = {"status": 1,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Success."
                                                                ],
                                                            }
                    # invoice total validation:
                        logger.info(f"ready for validations-docID: {doc_id}")
                        if "1" in str(skipConf):
                            zero_dlr_ck = 1
                            documentdesc = "Zero $ invoice approved by user"
                            corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                        else:    
                            zero_dlr_ck = 0

                        if "2" in str(skipConf):
                            amt_threshold_ck = 1
                            documentdesc = "Amount limit approved by user"
                            corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                        else:
                            amt_threshold_ck = 0

                        if "3" in str(skipConf):    
                            skip_approval_ck = 1
                            documentdesc = "Invoice manually approved by user"
                            corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                        else:
                            skip_approval_ck = 0

                        if "4" in str(skipConf):
                            skip_name_check = 1
                            documentdesc = "User skipped name check"
                            corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                        else:
                            skip_name_check = 0 

                        if "5" in str(skipConf):
                            skip_email_check = 1
                            documentdesc = "User skipped email check"
                            corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                        else:
                            skip_email_check = 0
                        
                        if "6" in str(skipConf):
                            skip_title_check = 1
                            documentdesc = "User skipped title check"
                            corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                        else:
                            skip_title_check = 0

                        if "7" in str(skipConf):
                            amount_approval_check = 1
                            documentdesc = "Invoice amount approved by user"
                            corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                        else:
                            amount_approval_check = 0

                        #currency validation: 
                        try:
                            if mand_currency != metadata_currency:
                                return_status["Currency validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    f"Currency validation failed: invoice currency {mand_currency} does not match metadata currency {metadata_currency}"
                                                                ],
                                                            }
                                currency_ck_msg = f"Currency validation failed: invoice currency {mand_currency} does not match metadata currency {metadata_currency}"
                            else:
                                currency_ck = 1
                                return_status["Currency validation"] = {"status": 1,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    f"Success."
                                                                ],
                                                            }
                        except Exception as e:
                            logger.info(f"Error in currency validation: {e}")
                            logger.info(traceback.format_exc())
                            currency_ck = 0

                            
                        
                        try:

                            logger.info(f"Validating invoice total- invoicetotal:{mand_invoTotal}, subtotal:{mand_subTotal}, gst:{mand_gst}")
                            if float(mand_invoTotal) or  float(mand_invoTotal)==0:
                                subtotal = float(mand_invoTotal)- float(mand_gst)
                                if (float(mand_invoTotal) - (float(mand_subTotal)+float(mand_gst))) != 0:
                                    db.query(model.corp_docdata).filter(
                                    
                                        model.corp_docdata.corp_doc_id == doc_id
                                    ).update(
                                        {
                                            
                                            model.corp_docdata.subtotal: subtotal,
                                        }
                                    )
                                    db.commit()

                                invoTotal_status = 1
                                gst_status = 1
                                subTotal_status = 1
                                subTotal_msg = "Subtotal present"
                                gst_msg = "GST present"
                                invoTotal_msg = "Invoice total present"
                                return_status["Invoice mandatory fields validation"] = {"status": 1,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Success."
                                                                ],
                                                            }
                            else:
                                invoTotal_status = 0
                                invoTotal_msg = "Invoice total mismatch"
                                return_status["Invoice mandatory fields validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Invoice total not valid."
                                                                ],
                                                            }
                        except Exception as e:
                            logger.error(f"Error in validate_corpdoc: {e}")
                            logger.info(traceback.format_exc())
                            invoTotal_status = 0
                            invoTotal_msg = "Please review Total"
                            return_status["Invoice mandatory fields validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Please review Total."
                                                                ],
                                                            }

                        # document type validation:

                        try:
                            logger.info(f"Validating document type- document_type:{mand_document_type}")
                            if mand_document_type.lower() in ['invoice','credit']:
                                document_type_status = 1
                                try:
                                    if mand_document_type.lower() == 'credit':
                                        credit_ck = 1
                                        update_Credit_data(doc_id, db)
                                    else:
                                        credit_ck = 0
                                except Exception:
                                    logger.error(f"Error in update_Credit_data:")
                                    logger.info(traceback.format_exc())
                                document_type_msg = "Document type validation success"
                                return_status["Document identifier validation"] = {"status": 1,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Success."
                                                                ],
                                                            }
                                # -ve amt check: 
                                #check corp_documentTab,corp_coding_tab,corp_docdata 
                                

                            else:
                                document_type_status = 0
                                document_type_msg = "Document type mismatch"
                                return_status["Document identifier validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Please review Document Type."
                                                                ],
                                                            }
                                return return_status
                        except Exception as e:
                            logger.error(f"Error in validate_corpdoc: {e}")
                            logger.info(traceback.format_exc())
                            document_type_status = 0
                            document_type_msg = "Please review Document Type"


                        
                        # Mandatory Header Validation:
                        if invDate_status==0:
                            docStatus = 4
                            substatus = 132
                            return_status["Invoice date validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Invoice date is invalid, Please review."
                                                                ],
                                                            }
                        elif invoTotal_status==0:
                            docStatus = 4
                            substatus = 131
                            return_status["invoice Total validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Invoice total mismatch, Please review."
                                                                ],
                                                            }
                        elif document_type_status==0:
                            docStatus = 4
                            substatus = 129
                            return_status["Document identifier validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Document identifier mismatch, Please review."
                                                                ],
                                                            }
                        elif currency_ck==0:
                            docStatus = 4
                            substatus = 100
                        
                        else:
                            return_status["Document identifier validation"] = {"status": 1,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Success."
                                                                ],
                                                            }
                            line_sum = 0
                            amt_threshold = 25000
                            cod_invoTotal =  df_corp_coding['invoicetotal']
                            cod_gst = df_corp_coding['gst']
                            template_type = df_corp_coding['template_type']
                            # if template_type is None or (isinstance(template_type, pd.Series) and template_type.isna().all()):
                            #     template_type = ""

                            invoTotal_15 = (cod_invoTotal * 0.15)

                            pdf_invoTotal = list(df_corp_docdata['invoicetotal'])[0]
                            lt = []
                            for ln_amt in (list(df_corp_coding['coding_details'])[0]):
                                lt.append(list(df_corp_coding['coding_details'])[0][ln_amt]['amount'])
                                amount_value = list(df_corp_coding['coding_details'])[0][ln_amt].get('amount', 0)  # Get value or default to 0

                                try:
                                    line_sum += float(amount_value) if amount_value not in (None, "", " ") else 0
                                except ValueError:
                                    print(f"Invalid amount value: {amount_value}, skipping...")
                                    # line_sum = 0

                                # line_sum = line_sum + list(df_corp_coding['coding_details'])[0][ln_amt]['amount']
                            # if template_type.iloc[0].lower() in ['template 3', 'template 1']:
                                # consider GST
                            if abs(float(cod_invoTotal.values[0])- (line_sum + float(cod_gst.values[0])) )> 0.09:
                                docStatus = 4
                                substatus = 136
                                documentdesc = "Coding - Line total mismatch"
                                return_status["Coding Line validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Coding - Line total mismatch"
                                                                ],
                                                            }
                            else:
                                return_status["Coding Line validation"] = {"status": 1,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Success."
                                                                ],
                                                            }
                                cod_lnMatch = 1
                            # else:
                            #     if abs(float(cod_invoTotal.values[0])- line_sum )> 0.09:
                            #             docStatus = 4
                            #             substatus = 136
                            #             documentdesc = "Coding - Line total mismatch"
                            #             return_status["Coding Line validation"] = {"status": 0,
                            #                                 "StatusCode":0,
                            #                                 "response": [
                            #                                                 "Coding - Line total mismatch"
                            #                                             ],
                            #                                     }
                            #     else:
                            #         cod_lnMatch = 1
                            #         return_status["Coding Line validation"] = {"status": 1,
                            #                             "StatusCode":0,
                            #                             "response": [
                            #                                             "Success."
                            #                                         ],
                            #                                     }
                                # return return_status
                            # else:
                            #line total match success
                            if cod_lnMatch==1:
                                if (abs(float(cod_invoTotal.values[0]) - pdf_invoTotal) >0.09):
                                    docStatus = 4
                                    substatus = 131
                                    documentdesc = "Invoice - Total mismatch with coding total"
                                    return_status["Coding validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    f"Invoice - Total mismatch with coding total"
                                                                ],
                                                            }
                                else:
                                    if credit_ck == 1:
                                        try: 
                                            if (cod_gst.abs() > invoTotal_15.abs()).any():
                                                docStatus = 4
                                                substatus = 138
                                                documentdesc = "Coding -GST exceeding 15%"
                                                return_status["Coding validation"] = {"status": 0,
                                                            "StatusCode":0,
                                                            "response": [
                                                                            f"Coding - GST exceeding 15% of invoice total"
                                                                        ],
                                                                    }
                                            else:
                                                gst_15_ck = 1
                                        except Exception as e:
                                            logger.info(traceback.format_exc())    


                                    elif credit_ck==0:
                                        if (cod_gst > invoTotal_15).any():
                                            docStatus = 4
                                            substatus = 138
                                            documentdesc = "Coding -GST exceeding 15%"
                                            return_status["Coding validation"] = {"status": 0,
                                                        "StatusCode":0,
                                                        "response": [
                                                                        f"Coding - GST exceeding 15% of invoice total"
                                                                    ],
                                                                }
                                        else:
                                            gst_15_ck = 1
                                        # return return_status
                                    #total match pass:
                                    if gst_15_ck==1:
                                        if pdf_invoTotal == 0 and zero_dlr_ck == 0:
                                            validation_status_ck = validation_status_ck * 0
                                            docStatus = 4
                                            substatus = 139
                                            documentdesc = "Zero $ invoice approval required"
                                            # Zero $ invoice - need approval'
                                            return_status["Coding validation"] = {"status": 0,
                                                        "StatusCode":1,
                                                        "response": [
                                                                        f"Zero $ invoice approval required"
                                                                    ],
                                                                }
                                            # return return_status
                                            # print("Zero $ invoice approved")
                                        if (pdf_invoTotal > amt_threshold) and amt_threshold_ck == 0:
                                            # need approval
                                            validation_status_ck = validation_status_ck * 0
                                            docStatus = 4
                                            substatus = 140
                                            documentdesc =  "Approval needed: Invoice ≥ threshold"
                                            print("need approval")
                                            return_status["Amount approval needed"] = {"status": 0,
                                                        "StatusCode":2,
                                                        "response": [
                                                                        f"User approval required for amount"
                                                                    ],
                                                                }
                                            # return return_status
                                    
                                        if approval_check_req == 1:
                                            # name match check: 
                                            #skip_name_check ==4
                                            
                                            approval_nm_val_status = 0
                                            approval_nm_val_msg = ""
                                            coding_approver_name = list(df_corp_coding['approver_name'])[0]
                                            invo_approver_name =  list(df_corp_docdata['approver'])[0]
                                            
                                            if (names_match(coding_approver_name, invo_approver_name) or (skip_name_check==1)):
                                                logger.info("Names match (ignoring case & order)")
                                                approval_nm_val_status = 1
                                                approval_nm_val_msg = "Success"
                                                name_ck_status = 0
                                            else:
                                                name_ck_status = 4
                                                approvrd_ck =approvrd_ck * 0
                                                logger.info("Approver and Sender Mismatch")
                                                approval_nm_val_msg = "Approver and Sender Mismatch"
                                            return_status["Approval name validation"] = {"status": approval_nm_val_status,
                                                                "StatusCode":name_ck_status,
                                                                "response": [
                                                                                approval_nm_val_msg
                                                                            ],
                                                                }
                                            
                                            #email check:
                                            # skip_email_check = 5
                                            approval_email_val_status = 0
                                            approval_email_val_msg = ""
                                            coding_approver_email = list(df_corp_coding['sender_email'])[0]
                                            if( email_belongs_to_name(coding_approver_name, coding_approver_email) or (skip_email_check==1)):
                                                logger.info(f"Email '{coding_approver_email}' belongs to '{coding_approver_name}'")
                                                approval_email_val_status = 1
                                                approval_email_val_msg = "Success"
                                                emal_status_code = 0
                                            else:
                                                emal_status_code = 5
                                                approvrd_ck=approvrd_ck * 0
                                                logger.info(f"Email '{coding_approver_email}' does NOT belong to '{coding_approver_name}'")
                                                approval_email_val_msg = f"Email '{coding_approver_email}' does NOT belong to '{coding_approver_name}'"
                                            return_status["Approval email validation"] = {"status": approval_email_val_status,
                                                                "StatusCode":emal_status_code,
                                                                "response": [
                                                                                approval_email_val_msg
                                                                            ],
                                                                }


                                            # title check:
                                            # skip_title_check = 6
                                            approval_title_val_msg = ""
                                            approval_title_val_status = 0
                                            invo_approver_title =str(list(df_corp_docdata['approver_title'])[0]).lower()
                                            coding_approver_title = str(list(df_corp_coding['approver_title'])[0]).lower()
                                            if (invo_approver_title == coding_approver_title) or (skip_title_check==1):
                                                title_status_code = 0
                                                logger.info("Approver title match")
                                                approval_title_val_status = 1
                                                approval_title_val_msg = "Success"
                                                approval_Amt_val_status = 0
                                                approval_Amt_val_msg = ""
                                                #amount_approval_check = 7
                                                if (is_amount_approved(float(pdf_invoTotal), invo_approver_title) or (amount_approval_check == 1)):
                                                    logger.info("Amount approved")
                                                    approval_Amt_val_status = 1
                                                    approval_Amt_val_msg = "Amount approved"
                                                    eml_status_code = 0
                                                else:
                                                    approvrd_ck= approvrd_ck * 0
                                                    eml_status_code = 7
                                                    logger.info("Approval limits conformance mismatch")
                                                    approval_Amt_val_msg = "Approval limits conformance mismatch"
                                                return_status["Approval amount validation"] = {"status": approval_Amt_val_status,
                                                                    "StatusCode":eml_status_code,
                                                                    "response": [
                                                                                    approval_Amt_val_msg
                                                                                ],
                                                                    }

                                                #--
                                            else:
                                                approvrd_ck = approvrd_ck * 0
                                                title_status_code = 6
                                                logger.info("Approver title mismatch")
                                                approval_title_val_msg = "Approver title mismatch"
                                            return_status["Approval title validation"] = {"status": approval_title_val_status,
                                                                "StatusCode":title_status_code,
                                                                "response": [
                                                                                approval_title_val_msg
                                                                            ],
                                                                }

                                             


                                            # approver_title =list(df_corp_document['approver_title'])[0] 
                                            # if (('sr' in approver_title.lower()) or ('senior' in approver_title.lower())) or ('vp' in approver_title.lower()) or (('vice' in approver_title.lower()) and ('president' in approver_title.lower())):
                                            #     approvrd_ck = 1
                                            # elif pdf_invoTotal <= 25000:
                                            #     if "assistant" in approver_title.lower():
                                            #         approvrd_ck = 0
                                            #     elif ("manager" in approver_title.lower()) or ("director" in approver_title.lower()) or ("Sr. Vice President" in approver_title):
                                            #         approvrd_ck = 1
                                            # elif pdf_invoTotal<= 1000000:
                                            #     if ("director" in approver_title.lower()):
                                            #         approvrd_ck = 1
                                            # if skip_approval_ck == 1:
                                            #     approvrd_ck = 1
                                            #     return_status["Approval validation"] = {"status": 1,
                                            #                     "StatusCode":0,
                                            #                     "response": [
                                            #                                     f"Invoice manually approved by user"
                                            #                                 ],
                                            #                     }
                                            
                                                
                                            if approvrd_ck==0:
                                                validation_status_ck = validation_status_ck * 0
                                                docStatus = 24
                                                substatus = 70
                                                documentdesc = "Invoice - Not Approved"
                                                return_status["Approval Validation"] = {"status": 0,
                                                        "StatusCode":0,
                                                        "response": [
                                                                        f"Failed Approval Validation"
                                                                    ],
                                                                }
                                                # return return_status
                                            elif approvrd_ck ==1:
                                                    
                                                if (list(df_corp_coding['approval_status'])[0].lower() == "approved") or (skip_approval_ck == 1):
                                                    docStatus = 2
                                                    substatus = 31
                                                    documentdesc = "Invoice approved"
                                                    if validation_status_ck ==1:
                                                        try:
                                                            for row in corp_coding_data:
                                                                coding_details = row.coding_details  # Assuming this is a dictionary

                                                                for st in coding_details:
                                                                    store_value = coding_details[st]["store"]
                                                                    coding_details[st]["store"] = store_value.zfill(4)  # Pad with leading zeros

                                                                # Explicitly mark field as modified
                                                                flag_modified(row, "coding_details")

                                                            # Commit changes to DB
                                                            db.commit()
                                                        except Exception as e:
                                                            logger.error(f"Error in updating coding_details: {e}")
                                                        payload_status = payload_dbUpdate(doc_id,userID,db)
                                                        try:
                                                            return_status.update(payload_status)
                                                        except Exception as e:
                                                            logger.error(f"Error in updating return_status: {e}")
                                                            return_status["Payload validation"] = {"status": 0,
                                                                    "StatusCode":0,
                                                                    "response": [
                                                                                    f"Error: {e}"
                                                                                ],
                                                                    }
                                                    else:
                                                        return_status["Overview"] = {"status": 0,
                                                            "StatusCode":0,
                                                            "response": [
                                                                            f"Validation failed"
                                                                        ],
                                                            }
                                                    # return return_status
                                                    # return_status["Approval needed"] = {"status": 0,
                                                    #     "StatusCode":0,
                                                    #     "response": [
                                                    #                     f"Payload data ready for PeopleSoft"
                                                    #                 ],
                                                    #             }
                                                    return return_status
                                                else: 
                                                    docStatus = 24
                                                    substatus = 137
                                                    documentdesc = "Pending Approval"
                                                    return_status["Approval needed"] = {"status": 0,
                                                        "StatusCode":3,
                                                        "response": [
                                                                        f"Invoice - Pending Approval"
                                                                    ],
                                                                }
                                                    return return_status
                                            else:
                                                return_status["Approval needed"] = {"status": 0,
                                                        "StatusCode":3,
                                                        "response": [
                                                                        f"Invoice - Not Approved."
                                                                    ],
                                                                }
                    except Exception as e:
                        logger.error(f"Error in validate_corpdoc: {e}")
                        logger.info(traceback.format_exc())
                        return_status["Validation failed"] = {"status": 0,
                                                    "StatusCode":3,
                                                    "response": [
                                                                    f"Error: {e}"
                                                                ],
                                                            }
        try:
            doc_updates_status = {'invoicetotal':{'status':invoTotal_status,'status_message':invoTotal_msg},
                    'gst':{'status':gst_status,'status_message':gst_msg},
                    'invoice_date':{'status':invDate_status,'status_message':invDate_msg},
                    'subtotal':{'status':subTotal_status,'status_message':subTotal_msg},
                    'document_type':{'status':document_type_status,'status_message':document_type_msg},
                    'currency':{'status':currency_ck,'status_message':currency_ck_msg}
                    }
            
            logger.info(f"doc_id: {doc_id}, doc_updates_status: {doc_updates_status}")
            db.query(model.corp_docdata).filter( model.corp_docdata.corp_doc_id == doc_id
            ).update(
                {
                    model.corp_docdata.doc_updates: doc_updates_status,  # noqa: E501
                    

                }

            )
            db.commit()
        except Exception as e:
            db.rollback()   
            logger.info(f"Error in validate_corpdoc: {e}")
            logger.info(traceback.format_exc())

        try:
            db.query(model.corp_document_tab).filter( model.corp_document_tab.corp_doc_id == doc_id
                ).update(
                    {
                        model.corp_document_tab.documentstatus: docStatus,  # noqa: E501
                        model.corp_document_tab.documentsubstatus: docSubStatus,  # noqa: E501
                        model.corp_document_tab.last_updated_by: userID,
                        model.corp_document_tab.updated_on: timeStmp,

                    }
                )
            db.commit()
        except Exception as e:
            logger.error(f"Error in validate_corpdoc: {e}")
            logger.info(traceback.format_exc())

        logger.info(f"return corp validations(ln 250): {return_status}")

        
        return return_status
                                
    except Exception as e:
        logger.error(f"Error in validate_corpdoc: {e}")
        logger.info(traceback.format_exc())
        return_status["Validation failed"] = {"status": 0,
                                                "StatusCode":0,
                                                "response": [
                                                                f"Error: {e}"
                                                            ],
                                            }
        return return_status
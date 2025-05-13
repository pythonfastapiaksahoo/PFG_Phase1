#corp_validations:

import re
from pfg_app import model
from pfg_app.FROps.corp_payloadValidation import payload_dbUpdate
from pfg_app.FROps.customCall import date_cnv
from pfg_app.session.session import SQLALCHEMY_DATABASE_URL, get_db
from sqlalchemy import func
from sqlalchemy.orm.attributes import flag_modified
from fuzzywuzzy import fuzz
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
    
def normalize_title(title):
    """Remove extra spaces, punctuation, and convert to lowercase."""
    title = re.sub(r'[^a-zA-Z0-9\s]', '', title)  # Remove special characters like commas
    title = re.sub(r'\s+', ' ', title).strip().lower()
    return title

def is_acronym_match(full_title, acronym):
    """Check if title2 is an acronym of title1."""
    words = full_title.split()
    acronym_generated = "".join(word[0].upper() for word in words)
    return acronym_generated == acronym.upper()

def is_match(title1, title2, threshold=85):
    """Check for acronym match, fuzzy match, or if one title is a subset of the other."""
    # Normalize titles (remove punctuation, extra spaces, lowercase)
    title1_norm = normalize_title(title1)
    title2_norm = normalize_title(title2)
    
    # Check for direct subset match (either title1 in title2 OR title2 in title1)
    if title1_norm in title2_norm or title2_norm in title1_norm:
        return True, 100  # Perfect subset match

    # Fuzzy match score
    similarity = fuzz.ratio(title1_norm, title2_norm)

    # # Check if acronym match OR fuzzy similarity is high
    # if is_acronym_match(title1, title2) or similarity >= threshold:
    #     return True, similarity

     # Check if acronym match OR fuzzy similarity is high
    if is_acronym_match(title1, title2) or similarity >= threshold:
        return True, similarity
    elif fuzz.token_set_ratio(title1_norm, title2_norm)==100:
        try:
            logger.info(f"token_set_ratio: { fuzz.token_set_ratio(title1_norm, title2_norm)}")
        except Exception as e:
            logger.info(f"Error in token_set_ratio: {e}")
            logger.info(traceback.format_exc())
        similarity = fuzz.token_set_ratio(title1_norm, title2_norm)
        return True, similarity


    return False, similarity


def names_match(name1, name2):
    name1 = (name1 or "").strip().lower()  # Convert None to empty string
    name2 = (name2 or "").strip().lower()
    return sorted(name1.split()) == sorted(name2.split())


def email_belongs_to_name(name, email):
    email_prefix = email.split("@")[0].lower()  # Extract email username
    name_parts = set(name.lower().split())  # Convert name into a set of lowercase words
    return any(part in email_prefix for part in name_parts)  # Check if any name part is in the email

def clean_coding_amount(amount_str):
    if amount_str in [None, ""]:
        return 0.0
    if isinstance(amount_str, (float, int)):
        return round(float(amount_str), 2)
    
    try:
        # Check if the value is negative due to a '-' sign
        is_negative = "-" in amount_str or ("(" in amount_str and ")" in amount_str)
        
        # Extract numeric values including decimal points
        cleaned_amount = re.findall(r"[\d.]+", amount_str)
        
        if cleaned_amount:
            amount = float("".join(cleaned_amount))
            return round(-amount if is_negative else amount, 2)
    except Exception:
        return 0.0
    
    return 0.0

# def is_valid_title(title, title_variants, approval_limits):
#     # Normalize input
#     title_key = title.strip().lower()
    
#     # Map to standardized title
#     normalized_title = title_variants.get(title_key)
#     if not normalized_title:
#         return False  # Unrecognized title

#     # Check if normalized title appears in any approval level
#     for roles in approval_limits.values():
#         if normalized_title in roles:
#             return True

    # return False
# def is_valid_title(title, title_variants, approval_limits):
#     try:
#         # Normalize input
#         title_key = title.strip().lower()

#         # Map to standardized title
#         normalized_title = title_variants.get(title_key)
#         if not normalized_title:
#             return 0  # Unrecognized title

#         # Check if normalized title appears in any approval level
#         for roles in approval_limits.values():
#             if normalized_title in roles:
#                 return 1

#         return 0
#     except Exception:
#         return 0

def is_valid_title(title, title_variants, approval_limits):
    try:
        title_cleaned = re.sub(r"[^a-zA-Z\s]", "", title).lower()
        title_cleaned = re.sub(r"\s+", " ", title_cleaned).strip()

        # Try to match using substring (same logic as your main function)
        for key in title_variants:
            if key in title_cleaned:
                normalized_title = title_variants[key]
                # Check if normalized title appears in any approval level
                for roles in approval_limits.values():
                    if normalized_title in roles:
                        return 1
                return 0  # Title recognized but not in allowed roles

        return 0  # No title matched
    except Exception as e:
        return 0



def is_amount_approved(amount: float, title: str) -> bool:
    print(f"Approval limits: {amount}, {title}")

    approval_limits = {
        (0, 24999): {"Supervisor", "Manager", "Senior Manager", "Director", "Regional Manager", "General Manager", "Managing Director", "VP","General Counsel"},

        (0, 74999): {"Senior Manager", "Director", "Regional Manager", "General Manager", "Managing Director", "VP"},
        (0, 499999): {"Director", "Regional Manager", "General Manager", "Managing Director", "VP"},
        (0, float("inf")): {"Managing Director", "VP"},
    }

    title_variants = {
        "supervisor": "Supervisor",
        # "superviso": "Supervisor",
        "manager": "Manager",
        "senior manager": "Senior Manager",
        "sr manager": "Senior Manager",
        "sr. manager": "Senior Manager",
        "director": "Director",
        "regional manager": "Regional Manager",
        "general manager": "General Manager",
        "general counsel" : "General Counsel",
        "managing director": "Managing Director",
        "vice president": "VP",
        "vp": "VP",
        
        "rmpo": "Regional Manager",
        "generalmanager, pattisson food group":"General Manager",
        "generalmanager": "General Manager",
    }

    title_cleaned = re.sub(r"[^a-zA-Z\s]", "", title)  # Remove special characters
    title_cleaned = re.sub(r"\s+", " ", title_cleaned).strip().lower()  # Normalize spaces
    print("title_cleaned: ",title_cleaned)
    validTitle = is_valid_title(title_cleaned, title_variants, approval_limits)
    normalized_title = None
    for key in title_variants:
        print(key)
        if key in title_cleaned:  # Check if title contains a known designation
            normalized_title = title_variants[key]
            print("normalized_title: ",normalized_title)
            if normalized_title!="Manager":
            
                break

    if not normalized_title:
        print(f"Title '{title_cleaned}' not recognized. Defaulting to unmatched.")
        return False,validTitle  # If no title matches, return False

    # **Step 3: Check Approval Limits**
    for (lower, upper), allowed_titles in approval_limits.items():
        if lower <= amount <= upper:
            # logger.info(f"Final Normalized Title: {normalized_title}")
            # logger.info(f"Allowed Titles for {amount}: {allowed_titles}")
            return normalized_title in allowed_titles,validTitle

    return False,validTitle

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
                # logger.info(f"Before record[0].invoicetotal: {record[0].invoicetotal}, record[0].gst: {record[0].gst}")
                record[0].invoicetotal = round(-abs(record[0].invoicetotal or 0), 2)
                record[0].gst = round(-abs(record[0].gst or 0), 2)
                # logger.info(f"After record[0].invoicetotal: {record[0].invoicetotal}, record[0].gst: {record[0].gst}")

            # Update the values for corp_coding_tab
            if record[1]:  # Checking if the corp_coding_tab record exists
                # record[1].invoicetotal = round(-abs(record[1].invoicetotal or 0), 2)
                logger.info(f"Before record[1].invoicetotal: {record[1].invoicetotal}, record[1].gst: {record[1].gst}")
                record[1].invoicetotal = clean_coding_amount(record[1].invoicetotal or 0)
                # record[1].gst = round(-abs(record[1].gst or 0), 2)
                record[1].gst = clean_coding_amount(record[1].gst or 0)
                logger.info(f"After record[1].invoicetotal: {record[1].invoicetotal}, record[1].gst: {record[1].gst}")

                # Update amount values inside coding_details
                if record[1].coding_details:
                    for key, value in record[1].coding_details.items():
                        if 'amount' in value and value['amount']:
                            # value['amount'] = round(-abs(value['amount']), 2)
                            logger.info(f"Before value['amount']: {value['amount']}")
                            value['amount'] = clean_coding_amount(value['amount'])
                            logger.info(f"After value['amount']: {value['amount']}")


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
                    setattr(record[2], field, round(-abs(value or 0), 2))  # Convert to negative and round

            # Commit changes for all records in a single call
            db.commit()
            print(f"Updated invoicetotal, gst, and other fields for docID {doc_id} successfully.")
        else:
            logger.info(f"No record found for docID {doc_id}")

    except Exception as e:
        logger.error(f"Error in validate_corpdoc: {e}")
        logger.info(traceback.format_exc())


def update_Credit_data_to_positive(doc_id, db):
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
                record[0].invoicetotal = round(abs(record[0].invoicetotal or 0), 2)
                record[0].gst = round(abs(record[0].gst or 0), 2)

            # Update the values for corp_coding_tab
            if record[1]:  # Checking if the corp_coding_tab record exists
                # record[1].invoicetotal = round(abs(record[1].invoicetotal or 0), 2)
                record[1].invoicetotal = clean_coding_amount(record[1].invoicetotal or 0)
                # record[1].gst = round(abs(record[1].gst or 0), 2)
                record[1].gst = clean_coding_amount(record[1].gst or 0)

                # Update amount values inside coding_details
                if record[1].coding_details:
                    for key, value in record[1].coding_details.items():
                        if 'amount' in value and value['amount']:
                            # value['amount'] = round(abs(value['amount']), 2)
                            value['amount'] = clean_coding_amount(value['amount'])

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
                    setattr(record[2], field, round(abs(value or 0), 2))  # Convert to positive and round

            # Commit changes for all records in a single call
            db.commit()
            print(f"Updated invoicetotal, gst, and other fields to positive for docID {doc_id} successfully.")
        else:
            logger.info(f"No record found for docID {doc_id}")

    except Exception as e:
        logger.error(f"Error in update_Credit_data_to_positive: {e}")
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
    vrd_status = 0
    process_inactive = 0
    approval_Amt_val_status = 0
    metadata_currency = ""
    VB_status = 0
    invo_cod_total_mismatch = 0
    validation_ck_all = 1
    invo_cod_gst_mismatch = 0
    rounding_threshold = 0.005
    try:
        logger.info(f"docID: {doc_id}, skipConf: {skipConf}")
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
                
                # ------------------------------------------------------ 
                if vendor_code is not None:
                    if vendor_id == 0:
                        try: 
                            vendor_id_update = int(list(df_corp_metadata['vendorid'])[0])
                            if type(vendor_id_update) == int:
                                vendor_id = vendor_id_update
                        except Exception as e:
                            logger.info(f"Error in updating vendorid: {e}")

                        
                docStatus = 4
                docSubStatus = 7
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
            
        if docStatus in (32,2,4,24,21):
            if vendor_code in (None,'') and vendor_id==0:
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
                try:
                    documentdesc = "Vendor mapping required"
                    corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                except Exception as e:
                    logger.info(traceback.format_exc())
                return_status["Vendor mapping required"] = {"status": 0,
                                "StatusCode":0,
                                "response": [
                                                "Vendor mapping required"
                                            ],
                                    }
                
                logger.info(f"return corp validations(ln 70): {return_status}")

                return return_status
            #----------
            #check if vendor is active or not: -
            try:
                if "9" in str(skipConf):
                    process_inactive = 1
                    documentdesc = "Inactive vendor invoice processed by user"
                    corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)

                else:
                    try:
                        query = db.query(
                            model.Vendor.idVendor,
                            func.jsonb_extract_path_text(model.Vendor.miscellaneous, "VENDOR_STATUS").label("VENDOR_STATUS")
                        ).filter(
                            model.Vendor.idVendor == vendor_id
                        )

                        result = query.first()
                    
                        if result:
                            vendor_status = result.VENDOR_STATUS
                            if vendor_status == "A":
                                vrd_status = 1
                                logger.info(f"Vendor {result.idVendor} is active")
                            else:
                                vrd_status = 0
                                logger.info(f"Vendor {result.idVendor} is inactive")
                        else:
                            logger.info("Vendor not found")
                        if vrd_status==0:
                            docStatus = 4
                            docSubStatus = 22
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
                            documentdesc = "Inactive vendor"
                            corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                            return_status["Status overview"] = {"status": 0,
                                                        "StatusCode":9,
                                                        "response": [
                                                                        "Inactive vendor"
                                                                    ],
                                                            }
                            return return_status
                    except Exception as e:
                        logger.error(f"Vendor not found {e}")
                        logger.info(traceback.format_exc())

            except Exception:
                logger.info(traceback.format_exc())
            
            try:
                logger.info(f"docID: {doc_id}, invoice_id: {invoice_id},process_inactive: {process_inactive}")
                if invoice_id in [None, ""]:
                 # Query corp_docdata
                    corp_docdata = (
                        db.query(model.corp_docdata)
                        .filter(model.corp_docdata.corp_doc_id == doc_id)
                        .all()
                    )

                    # df_corp_coding = pd.DataFrame([row.__dict__ for row in corp_coding_data])

                    df_corp_docdata = pd.DataFrame([row.__dict__ for row in corp_docdata])
                    try:
                        # Drop SQLAlchemy internal state column
                        df_corp_docdata.drop(columns=["_sa_instance_state"], inplace=True, errors="ignore")

                        # Check for empty fields (None or empty strings)
                        empty_fields_report = df_corp_docdata.isnull() | (df_corp_docdata == '')

                        missing_values_list = []

                        for index, row in df_corp_docdata.iterrows():
                            missing_columns = empty_fields_report.columns[empty_fields_report.loc[index]].tolist()
                            missing_values_list.append(missing_columns)
                        logger.info(f"missing_values_list: {missing_values_list}")
                        if ['vendor_name', 'vendoraddress', 'customeraddress', 'doc_updates', 'customername', 'invoice_id', 'invoice_date'] in missing_values_list:
                            docStatus = 4
                            documentdesc = f"OpenAI extraction failed -  Rate Limit exceeded"
                            docSubStatus = 165
                            return_status["OpenAI extraction failed"] = {"status": 0,
                                                        "StatusCode":0,
                                                        "response": [
                                                                        "Rate limit exceeded,please update the data manually to proceed."
                                                                    ],
                                                                }
                            
                            corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
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
                            logger.info(f"Rate Limit exceeded: {return_status}")
                        else:
                            return_status["InvoiceID"] = {"status": 0,
                                                        "StatusCode":0,
                                                        "response": [
                                                                        "Invalid."
                                                                    ],
                                                                }
                        return return_status
                    except Exception as e:
                        logger.info(traceback.format_exc())
                        logger.info(f"Error: {e}")

                
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
                    try:
                        try:
                            corp_metadata_qry = (
                                        db.query(model.corp_metadata)
                                        .filter(model.corp_metadata.vendorid == vendor_id)
                                        .all()
                                    )
                            df_corp_metadata = pd.DataFrame([row.__dict__ for row in corp_metadata_qry])
                                    
                        except Exception as e:
                            logger.info(f"Error in getting metadata: {e}")
                            df_corp_metadata = pd.DataFrame()

                        if not df_corp_metadata.empty:
                            try:
                                corp_docdata = (
                                    db.query(model.corp_docdata)
                                    .filter(model.corp_docdata.corp_doc_id == doc_id)
                                    .all()
                                )
                                df_corp_docdata = pd.DataFrame([row.__dict__ for row in corp_docdata])
                                mand_invDate = list(df_corp_docdata['invoice_date'])[0]
                            except Exception as e:
                                mand_invDate = ""
                                logger.info(f"Error: {e}")
                            # VB_status = 1
                            # metadata_currency = list(df_corp_metadata['currency'])[0]
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
                                
                    except Exception as e:
                        logger.info(f"Error: {e}")

                    docStatus = 32
                    documentdesc = f"Duplicate invoice"
                    docSubStatus = 128
                    return_status["Status overview"] = {"status": 0,
                                                "StatusCode":0,
                                                "response": [
                                                                "Duplicate invoice."
                                                            ],
                                                        }
                    
                    corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
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
                    logger.info(f"return corp validations(ln 111): {return_status}")
                    return return_status
            except Exception as e:
                logger.info(f"Error in validate_corpdoc: {e}")
                logger.info(traceback.format_exc())
                
            # ------
            
            #-----

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
                # docSubStatus = 7
                logger.info(f"docID: {doc_id} - coding lines found")
            else:
                docSubStatus = 134
                docStatus = 4
                
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
                try:
                    documentdesc = "Coding - No Coding Lines Found"
                    corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                except Exception as e:
                    logger.info(f"docID: {doc_id} - Coding - No Coding Lines Found")
                    logger.info(traceback.format_exc())
                logger.info(f"docID: {doc_id} - Coding - No Coding Lines Found")
                return_status["Status overview"] = {"status": 0,
                                        "StatusCode":0,
                                        "response": [
                                                        "Coding - No Coding Lines Found"
                                                    ],
                                                }
                logger.info(f"return corp validations(ln 61): {return_status}")
                return return_status
            if docSubStatus == 130:

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
                                    docSubStatus = 7
                                else:
                                    try:
                                        documentdesc = "Vendor mapping required"
                                        corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                    except Exception as e:
                                        logger.info(traceback.format_exc())
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
                        try:
                            documentdesc = "Vendor mapping required"
                            corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                        except Exception as e:
                            logger.info(traceback.format_exc())
                        return_status["Status overview"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Vendor Mapping required"
                                                                ],
                                                            }
                        logger.info(f"return corp validations(ln 70): {return_status}")
                        return return_status
                #----------------------------   
                try:   
                    logger.info(f"line 730: vendor_code: {vendor_code}")
                    if vendor_code in [None,0]:
                        if vendor_id is not None:
                                try: 
                                    corp_VrdID_cd_qry = (
                                                db.query(model.Vendor)
                                                .filter(model.Vendor.idVendor == vendor_id)
                                                .all()
                                            )
                                    df_corp_VrdID_cd = pd.DataFrame([row.__dict__ for row in corp_VrdID_cd_qry])
                                    vendor_cd_update = str(list(df_corp_VrdID_cd['VendorCode'])[0])
                                    logger.info(f"line 741: vendor_cd_update: {vendor_cd_update}")
                                    if len(str(vendor_cd_update))>0:

                                        db.query(model.corp_document_tab).filter( model.corp_document_tab.corp_doc_id == doc_id
                                            ).update(
                                                {
                                                    
                                                    model.corp_document_tab.vendor_code: vendor_cd_update,

                                                }
                                            )
                                        db.commit()

                                    else:
                                        try:
                                            documentdesc = "Vendor Code missing."
                                            corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                        except Exception as e:
                                            logger.info(traceback.format_exc())
                                        return_status["Vendor mapping required"] = {"status": 0,
                                                        "StatusCode":0,
                                                        "response": [
                                                                        "Vendor Code missing."
                                                                    ],
                                                            }
                                        logger.info(f"return corp validations(ln 70): {return_status}")
                                        return return_status
                                except Exception as e:
                                    logger.info(traceback.format_exc())
                                    logger.info(f"Error in updating vendorid: {e}")

                        #------
                except Exception as e:
                    logger.info(traceback.format_exc())
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
                        df_corp_metadata = pd.DataFrame()
                    logger.info(f"df_corp_metadata: {df_corp_metadata}")
                    try: 
                        if df_corp_metadata.empty: 
                            documentdesc = "Vendor not onboarded."
                            docSubStatus = 106
                            docStatus = 25
                            try:

                                corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
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
                                logger.error(f"Error in updating corp_docdata: {e}")
                                logger.info(traceback.format_exc())
                            if "A" in str(skipConf):
                                VB_documentdesc = "User processing invoice manually"
                                VB_status = 1
                                VB_status_code = 0
                                docSubStatus = 106
                                docStatus = 25
                                return_status["Vendor not onboarded"] = {"status": VB_status,
                                                            "StatusCode":VB_status_code,
                                                            "response": [
                                                                            VB_documentdesc
                                                                        ],
                                                                    }
                                try:
                                    documentdesc = "Vendor not onboarded-User processing invoice manually"
                                    corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                except Exception as e:
                                    logger.info(traceback.format_exc())
                            else:
                                VB_documentdesc = "Onboard vendor/proceess manually"
                                docSubStatus = 106
                                docStatus = 25
                                VB_status = 0
                                VB_status_code = 10
                                # corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus) 
                                #---
                                try:
                                    documentdesc = "Vendor not onboarded-Onboard vendor/processing manually required."
                                    corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                except Exception as e:
                                    logger.info(traceback.format_exc())
                            
                                #---
                                return_status["Vendor not onboarded"] = {"status": VB_status,
                                                            "StatusCode":VB_status_code,
                                                            "response": [
                                                                            VB_documentdesc
                                                                        ],
                                                                    }
                                return return_status
                    except Exception as e:
                        logger.info(traceback.format_exc())

                    if not df_corp_metadata.empty:
                        VB_status = 1
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
                                 #---
                                try:
                                    documentdesc =  f"Invalid Date Format: {mand_invDate}."
                                    corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                except Exception as e:
                                    logger.info(traceback.format_exc())
                            
                                #---
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
                        if check_date_format(mand_invDate) == False:

                            return_status["Invoice date validation"] = {"status": 0,
                                                            "StatusCode":0,
                                                            "response": [
                                                                            "Metadata is not valid."
                                                                        ],
                                                                    }
                            invDate_msg = "Invalid Date Format"
                            invDate_status = 0
                        else:
                            return_status["Invoice date validation"] = {"status": 1,
                                                            "StatusCode":0,
                                                            "response": [
                                                                            "Success."
                                                                        ],
                                                                    }
                            invDate_msg = "Valid Date Format"
                            invDate_status = 1
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
                    # cl_invoID =  re.sub(r'[^a-zA-Z0-9\s]', '', invoice_id)
                    cl_invoID = re.sub(r'[^a-zA-Z0-9\s]', '', invoice_id).upper()
                    if len(cl_invoID)==0:
                         #---
                        try:
                            documentdesc =   "Invoice ID not valid."
                            corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                        except Exception as e:
                            logger.info(traceback.format_exc())
                    
                        #---
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

                # dupCk_document_data = (
                # 
                # db.query(model.corp_document_tab)
                # .filter(
                #     model.corp_document_tab.corp_doc_id != doc_id,
                #     model.corp_document_tab.vendor_id == vendor_id,
                #     model.corp_document_tab.documentstatus != 10,
                #     model.corp_document_tab.invoice_id in (invoice_id,invoice_id.lower(),invoice_id.upper())
                # )
                # .all()
                # )
                dupCk_document_data = (
                    db.query(model.corp_document_tab)
                        .filter(
                            model.corp_document_tab.corp_doc_id != doc_id,
                            model.corp_document_tab.vendor_id == vendor_id,
                            model.corp_document_tab.documentstatus != 10,
                            model.corp_document_tab.invoice_id.in_([invoice_id, invoice_id.lower(), invoice_id.upper()])
                        )
                        .all())


                
                df_dupCk_document = pd.DataFrame([
                    {col: getattr(row, col) for col in model.corp_document_tab.__table__.columns.keys()}
                    for row in dupCk_document_data
                ])

                if len(df_dupCk_document)>0:
                    # duplicate invoice
                    try:
                        try:
                            corp_metadata_qry = (
                                        db.query(model.corp_metadata)
                                        .filter(model.corp_metadata.vendorid == vendor_id)
                                        .all()
                                    )
                            
                            df_corp_metadata = pd.DataFrame([row.__dict__ for row in corp_metadata_qry])
                            
                        except Exception as e:
                            logger.info(f"Error in getting metadata: {e}")
                            df_corp_metadata = pd.DataFrame()

                        if not df_corp_metadata.empty:
                            try:
                                corp_docdata = (
                                    db.query(model.corp_docdata)
                                    .filter(model.corp_docdata.corp_doc_id == doc_id)
                                    .all()
                                )
                                df_corp_docdata = pd.DataFrame([row.__dict__ for row in corp_docdata])
                                mand_invDate = list(df_corp_docdata['invoice_date'])[0]
                            except Exception as e:
                                mand_invDate = ""
                                logger.info(f"Error: {e}")
                            VB_status = 1
                            # metadata_currency = list(df_corp_metadata['currency'])[0]
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
                                
                    except Exception as e:
                        logger.info(f"Error: {e}")
                        logger.info(traceback.format_exc())
                    docStatus = 32
                    documentdesc = f"Duplicate invoice"
                    docSubStatus = 128
                    #---
                    try:
                        documentdesc =   "Duplicate invoice."
                        corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                    except Exception as e:
                        logger.info(traceback.format_exc())
                
                    #---
                    return_status["Status overview"] = {"status": 0,
                                                "StatusCode":0,
                                                "response": [
                                                                "Duplicate invoice."
                                                            ],
                                                        }
                    
                    corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
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
                        if "8" in str(skipConf):
                            skip_currency_check = 1
                            documentdesc = "Currency validation skipped by user"
                            corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                        else:
                            skip_currency_check = 0

                        #currency validation: 
                        try:
                            if ("A" in str(skipConf)) or (process_inactive==1) :
                            # if process_inactive==1:
                                    currency_ck = 1
                                    currency_ck_msg = "User processing manually."
                                    return_status["Currency validation"] = {"status": 1,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    f"User processing manually."
                                                                ],
                                                            }
                                    
                            elif mand_currency != metadata_currency:
                                if skip_currency_check == 1:
                                    currency_ck_msg = "Currency validation skipped by user"
                                    currency_ck = 1
                                    return_status["Currency validation"] = {"status": 1,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    f"Currency validation skipped by user"
                                                                ],
                                                            }
                                    currency_ck_msg = f"Currency validation skipped by user"
                                # elif process_inactive==1:
                                #     currency_ck = 1
                                #     currency_ck_msg = "User processing manually."
                                #     return_status["Currency validation"] = {"status": 1,
                                #                     "StatusCode":0,
                                #                     "response": [
                                #                                     f"User processing manually."
                                #                                 ],
                                #                             }
                                else:
                                    return_status["Currency validation"] = {"status": 0,
                                                    "StatusCode":8,
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
                            currency_ck_msg = "Error:"+str(e)

                            
                        
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
                                #---
                                try:
                                    documentdesc =   "Invoice total not valid."
                                    corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                except Exception as e:
                                    logger.info(traceback.format_exc())
                            
                                #---
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
                            try:
                                documentdesc =   f"Invoice total not valid:{str(e)}."
                                corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                            except Exception as e:
                                logger.info(traceback.format_exc())
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
                                        update_Credit_data_to_positive(doc_id, db)
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
                                try:
                                    documentdesc =   f"Invalid Document Type:{mand_document_type}."
                                    corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                except Exception as e:
                                    logger.info(traceback.format_exc())
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
                        line_sum = 0
                        amt_threshold = 25000
                        cod_invoTotal =  df_corp_coding['invoicetotal']
                        cod_gst = df_corp_coding['gst']
                        template_type = df_corp_coding['template_type']
                        
                        try:
                            logger.info(f"invoice total: {float(mand_invoTotal)}, invoice coding total: {float(cod_invoTotal)}")
                            logger.info(f"invoice gst: {float(mand_gst)}, invoice coding gst: {float(cod_gst)}, rounding_threshold: {rounding_threshold}")

                            if abs(clean_coding_amount(str(mand_invoTotal)) - clean_coding_amount(str(float(cod_invoTotal))))>rounding_threshold:
                                invo_cod_total_mismatch = 0
                                
                                # invoice_status_msg ="Invoice total mismatch with coding total"
                                
                            else:
                                invo_cod_total_mismatch = 1
                            if abs(clean_coding_amount(str(mand_gst)) - clean_coding_amount(str(float(cod_gst))))>rounding_threshold:
                                invo_cod_gst_mismatch = 0
                                
                            else:
                                invo_cod_gst_mismatch = 1
                        except Exception:
                            logger.info(f"Error in invoice total mismatch: {traceback.format_exc()}")

                        if invo_cod_total_mismatch==0:
                            return_status["Invoice Total validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Invoice total mismatch with coding total"
                                                                ],
                                                            }
                            validation_ck_all = validation_ck_all*0
                                    
                        if invo_cod_gst_mismatch==0:
                                # invoice_status_msg ="Invoice GST mismatch with coding total"
                            docStatus = 4
                            docSubStatus = 17
                            return_status["Invoice GST validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Invoice GST mismatch with coding total"
                                                                ],
                                                            }
                            
                            validation_ck_all = validation_ck_all*0
                        
                        if invDate_status==0:
                            validation_ck_all = validation_ck_all*0
                            docStatus = 4
                            docSubStatus = 132
                            return_status["Invoice date validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Invoice date is invalid, Please review."
                                                                ],
                                                            }
                            validation_ck_all = validation_ck_all*0
                        if invoTotal_status==0:
                            docStatus = 4
                            docSubStatus = 131
                            return_status["invoice Total validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Invoice total mismatch, Please review."
                                                                ],
                                                            }
                            validation_ck_all = validation_ck_all*0

                        if document_type_status==0:
                            docStatus = 4
                            docSubStatus = 129
                            return_status["Document identifier validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Document identifier mismatch, Please review."
                                                                ],
                                                            }
                            validation_ck_all = validation_ck_all*0
                        if currency_ck==0:
                            validation_ck_all = validation_ck_all*0
                            docStatus = 4
                            docSubStatus = 100
                        
                        if validation_ck_all == 1:
                            return_status["Document identifier validation"] = {"status": 1,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Success."
                                                                ],
                                                            }
                            
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
                            if credit_ck==1:
                                if round(abs(float(cod_invoTotal.values[0])- (line_sum + float(cod_gst.values[0])) ),2)> rounding_threshold:
                                    docStatus = 4
                                    docSubStatus = 136
                                    documentdesc = "Coding - Line total mismatch"
                                    return_status["Coding Line validation"] = {"status": 0,
                                                        "StatusCode":0,
                                                        "response": [
                                                                        "Coding - Line total mismatch"
                                                                    ],
                                                                }
                                    try:
                                        documentdesc =   "Coding - Line total mismatch."
                                        corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                    except Exception as e:
                                        logger.info(traceback.format_exc())

                                else:
                                    return_status["Coding Line validation"] = {"status": 1,
                                                        "StatusCode":0,
                                                        "response": [
                                                                        "Success."
                                                                    ],
                                                                }
                                    cod_lnMatch = 1
                            # else:
                            elif round(abs(float(cod_invoTotal.values[0])- (line_sum + float(cod_gst.values[0])) ),2)>rounding_threshold:
                                docStatus = 4
                                docSubStatus = 136
                                documentdesc = "Coding - Line total mismatch"
                                return_status["Coding Line validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    "Coding - Line total mismatch"
                                                                ],
                                                            }
                                try:
                                    documentdesc =   "Coding - Line total mismatch."
                                    corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                except Exception as e:
                                    logger.info(traceback.format_exc())

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
                                if (abs(float(cod_invoTotal.values[0]) - pdf_invoTotal) >rounding_threshold):
                                    docStatus = 4
                                    docSubStatus = 131
                                    documentdesc = "Invoice - Total mismatch with coding total"
                                    return_status["Coding validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    f"Invoice - Total mismatch with coding total"
                                                                ],
                                                            }
                                    try:
                                        documentdesc =   f"Invoice - Total mismatch with coding total"
                                        corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                    except Exception as e:
                                        logger.info(traceback.format_exc())

                                else:
                                    if credit_ck == 1:
                                        try: 
                                            if (cod_gst.abs() > invoTotal_15.abs()).any():
                                                docStatus = 4
                                                docSubStatus = 138
                                                documentdesc = "Coding -GST exceeding 15%"
                                                return_status["Coding validation"] = {"status": 0,
                                                            "StatusCode":0,
                                                            "response": [
                                                                            f"Coding - GST exceeding 15% of invoice total"
                                                                        ],
                                                                    }
                                                try:
                                                    documentdesc = f"Coding - GST exceeding 15% of invoice total"
                                                    corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                                except Exception as e:
                                                    logger.info(traceback.format_exc())
                                            else:
                                                gst_15_ck = 1
                                        except Exception as e:
                                            logger.info(traceback.format_exc())    


                                    elif credit_ck==0:
                                        if (cod_gst > invoTotal_15).any():
                                            docStatus = 4
                                            docSubStatus = 138
                                            documentdesc = "Coding -GST exceeding 15%"
                                            return_status["Coding validation"] = {"status": 0,
                                                        "StatusCode":0,
                                                        "response": [
                                                                        f"Coding - GST exceeding 15% of invoice total"
                                                                    ],
                                                                }
                                            try:
                                                documentdesc = f"Coding - GST exceeding 15% of invoice total"
                                                corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                            except Exception as e:
                                                logger.info(traceback.format_exc())
                                        else:
                                            gst_15_ck = 1
                                        # return return_status
                                    #total match pass:
                                    if gst_15_ck==1:
                                        if pdf_invoTotal == 0 and zero_dlr_ck == 0:
                                            validation_status_ck = validation_status_ck * 0
                                            docStatus = 4
                                            docSubStatus = 139
                                            documentdesc = "Zero $ invoice approval required"
                                            # Zero $ invoice - need approval'
                                            return_status["Coding validation"] = {"status": 0,
                                                        "StatusCode":1,
                                                        "response": [
                                                                        f"Zero $ invoice approval required"
                                                                    ],
                                                                }
                                            try:
                                                documentdesc = "Zero $ invoice approval required"
                                                corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                            except Exception as e:
                                                logger.info(traceback.format_exc())
                                            # return return_status
                                            # print("Zero $ invoice approved")
                                        if (pdf_invoTotal > amt_threshold) and amt_threshold_ck == 0:
                                            # need approval
                                            validation_status_ck = validation_status_ck * 0
                                            docStatus = 4
                                            docSubStatus = 140
                                            documentdesc =  "Approval needed: Invoice ≥ threshold"
                                            print("need approval")
                                            return_status["Amount approval needed"] = {"status": 0,
                                                        "StatusCode":2,
                                                        "response": [
                                                                        f"User approval required for amount"
                                                                    ],
                                                                }
                                            try:
                                                documentdesc = f"User approval required for Invoice toatl({pdf_invoTotal}) greater than {amt_threshold}"
                                                corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                            except Exception as e:
                                                logger.info(traceback.format_exc())
                                            # return return_status
                                    
                                        if approval_check_req == 1:
                                            # name match check: 
                                            #skip_name_check ==4
                                            
                                            approval_nm_val_status = 0
                                            approval_nm_val_msg = ""
                                            coding_approver_name = list(df_corp_coding['approver_name'])[0]
                                            invo_approver_name =  list(df_corp_docdata['approver'])[0]
                                            
                                            # if (names_match(coding_approver_name, invo_approver_name) or (skip_name_check==1)):
                                            #     logger.info("Names match (ignoring case & order)")
                                            #     approval_nm_val_status = 1
                                            #     if skip_name_check==1:
                                            #         approval_nm_val_msg = "Name check skipped by user"
                                            #     else:
                                            #         approval_nm_val_msg = "Success"
                                            #     name_ck_status = 0
                                            # else:
                                            #     name_ck_status = 4
                                            #     approvrd_ck =approvrd_ck * 0
                                            #     logger.info("Approver and Sender Mismatch")
                                            #     approval_nm_val_msg = "Approver and Sender Mismatch"
                                            # return_status["Approval name validation"] = {"status": approval_nm_val_status,
                                            #                     "StatusCode":name_ck_status,
                                            #                     "response": [
                                            #                                     approval_nm_val_msg
                                            #                                 ],
                                            #                     }
                                            
                                            #email check:
                                            # skip_email_check = 5
                                            approval_email_val_status = 0
                                            approval_email_val_msg = ""
                                            coding_approver_email = list(df_corp_coding['sender_email'])[0]
                                            if coding_approver_name in [None,""]:
                                                return_status["Approval validation"] = {"status": 0,
                                                                "StatusCode":0,
                                                                "response": [
                                                                                "Approver name not found"
                                                                            ],
                                                                }
                                                try:
                                                    documentdesc = "Approver name not found"
                                                    corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                                except Exception as e:
                                                    logger.info(traceback.format_exc())
                                                return return_status
                                            

                                            if( email_belongs_to_name(coding_approver_name, coding_approver_email) or (skip_email_check==1)):
                                                logger.info(f"Email '{coding_approver_email}' belongs to '{coding_approver_name}'")
                                                approval_email_val_status = 1
                                                if skip_email_check==1:
                                                    approval_email_val_msg = "Email check skipped by user"
                                                else:
                                                    approval_email_val_msg = "Success"
                                                emal_status_code = 0
                                            else:
                                                docStatus = 24
                                                docSubStatus = 70
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
                                            sender_title = str(list(df_corp_coding['sender_title'])[0]).lower()
                                            coding_approver_title = str(list(df_corp_coding['approver_title'])[0]).lower()
                                            
                                            if sender_title.lower() not in ["na","",None]:
                                                match, score = is_match(sender_title, coding_approver_title)
                                            else:
                                                score = 0
                                                if skip_title_check!=1:
                                                    docStatus = 24
                                                    docSubStatus = 163
                                                    approvrd_ck = approvrd_ck * 0
                                                    title_status_code = 6
                                                    logger.info("Approver title mismatch")
                                                    approval_title_val_msg = f"Approver title mismatch: Sender title: '{sender_title}' Vs Approver title: '{coding_approver_title}'"
                                                match = False
                                            logger.info(f"match status: {match}, score: {score},skip_title_check: {skip_title_check}")
                                            if match or (skip_title_check==1):
                                                title_status_code = 0
                                                logger.info("Approver title match")
                                                approval_title_val_status = 1
                                                if skip_title_check==1:
                                                    approval_title_val_msg = "Approver title match skipped by user"
                                                else:
                                                    approval_title_val_msg = "Success"
                                                    approval_Amt_val_status = 0
                                                    approval_Amt_val_msg = ""
                                                #amount_approval_check = 7
                                            else:
                                                docStatus = 24
                                                docSubStatus = 163
                                                approvrd_ck = approvrd_ck * 0
                                                title_status_code = 6
                                                logger.info("Approver title mismatch")
                                                approval_title_val_msg = f"Approver title mismatch: Sender title: '{sender_title}' Vs Approver title: '{coding_approver_title}'"
                                            try:
                                                # documentdesc = "Approver name not found"
                                                corp_update_docHistory(doc_id, userID, docStatus, approval_title_val_msg, db,docSubStatus)
                                            except Exception as e:
                                                logger.info(traceback.format_exc())
                                            return_status["Approval title validation"] = {"status": approval_title_val_status,
                                                        "StatusCode":title_status_code,
                                                        "response": [
                                                                        approval_title_val_msg
                                                                    ],
                                                        }
                                            if credit_ck!=1:
                                                amt_approved, validTitle = is_amount_approved(float(pdf_invoTotal), coding_approver_title)
                                                # if (amt_approved or (amount_approval_check == 1)):
                                                #     logger.info("Amount approved")
                                                
                                            if credit_ck==1:
                                                logger.info("Amount limit approval skipped for credit")
                                                approval_Amt_val_status = 1
                                                approval_Amt_val_msg = "Amount limit approval skipped for credit"
                                                eml_status_code = 0
                                            elif validTitle==0 and amount_approval_check!=1:
                                                # if amt_threshold_ck==1 and skip_title_check==1:

                                                docStatus = 24
                                                docSubStatus = 166
                                                approval_Amt_val_status =0
                                                approvrd_ck= approvrd_ck * 0
                                                eml_status_code = 7
                                                logger.info("Unrecognized approver title")
                                                approval_Amt_val_msg = "Unrecognized approver title"
                                                return_status[f"Unrecognized approver title:{coding_approver_title}"] = {"status": approval_Amt_val_status,
                                                                    "StatusCode":eml_status_code,
                                                                    "response": [
                                                                                    approval_Amt_val_msg
                                                                                ],
                                                                    }

                                            elif (amt_approved or (amount_approval_check == 1)):
                                                logger.info("Amount approved")
                                                approval_Amt_val_status = 1
                                                if amount_approval_check==1:
                                                    approval_Amt_val_msg = "Amount limit approval skipped by user"
                                                else:
                                                    approval_Amt_val_msg = "Amount approved"
                                                eml_status_code = 0
                                            else:
                                                docStatus = 24
                                                docSubStatus = 164
                                                approval_Amt_val_status =0
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
                                                try:
                                                    documentdesc = "Approval limits conformance mismatch"
                                                    corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                                except Exception as e:
                                                    logger.info(traceback.format_exc())

                                                #--
                                            
                                             


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
                                                if docStatus != 24:
                                                    docStatus = 24
                                                    docSubStatus = 137
                                                validation_status_ck = validation_status_ck * 0
                                                
                                                documentdesc = "Invoice - Not Approved"
                                                return_status["Approval Validation"] = {"status": 0,
                                                        "StatusCode":0,
                                                        "response": [
                                                                        f"Failed Approval Validation"
                                                                    ],
                                                                }
                                                try:
                                                    # documentdesc = "Approval limits conformance mismatch"
                                                    corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                                except Exception as e:
                                                    logger.info(traceback.format_exc())
                                                # return return_status
                                            elif approvrd_ck ==1:
                                                if ((list(df_corp_coding['approval_status'])[0]) in ["",None]) and skip_approval_ck != 1:
                                                    docStatus = 24
                                                    docSubStatus = 137
                                                    documentdesc = "Pending Approval"
                                                    try:
                                                        documentdesc = f"Invoice - Pending Approval"
                                                        corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                                    except Exception as e:
                                                        logger.info(traceback.format_exc())
                                                    return_status["Approval needed"] = {"status": 0,
                                                        "StatusCode":3,
                                                        "response": [
                                                                        f"Invoice - Pending Approval"
                                                                    ],
                                                                }
                                                    return return_status
                                                elif (list(df_corp_coding['approval_status'])[0].lower() == "approved") or (skip_approval_ck == 1):
                                                    docStatus = 2
                                                    docSubStatus = 31
                                                    documentdesc = "Invoice approved"
                                                    if validation_status_ck ==1:
                                                        try:
                                                            for row in corp_coding_data:
                                                                coding_details = row.coding_details  # Assuming this is a dictionary

                                                                for st in coding_details:
                                                                    store_value = coding_details[st]["store"]
                                                                    coding_details[st]["store"] = store_value.zfill(4)  # Pad with leading zeros
                                                                    dept_value = coding_details[st]["dept"]
                                                                    coding_details[st]["dept"] = dept_value.zfill(4)  # Pad with leading zeros
                                                            
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
                                                    docSubStatus = 137
                                                    documentdesc = "Pending Approval"
                                                    try:
                                                        documentdesc = f"Invoice - Pending Approval"
                                                        corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                                    except Exception as e:
                                                        logger.info(traceback.format_exc())
                                                    return_status["Approval needed"] = {"status": 0,
                                                        "StatusCode":3,
                                                        "response": [
                                                                        f"Invoice - Pending Approval"
                                                                    ],
                                                                }
                                                    return return_status
                                            else:
                                                try:
                                                    documentdesc = "Invoice - Not Approved."
                                                    corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                                                except Exception as e:
                                                    logger.info(traceback.format_exc())
                                                return_status["Approval needed"] = {"status": 0,
                                                        "StatusCode":3,
                                                        "response": [
                                                                        f"Invoice - Not Approved."
                                                                    ],
                                                                }
                    except Exception as e:
                        logger.error(f"Error in validate_corpdoc: {e}")
                        logger.info(traceback.format_exc())
                        try:
                            documentdesc = f"Error: {e}"
                            corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
                        except Exception as e:
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
        try:
            documentdesc = f"Error: {str(e)}"
            corp_update_docHistory(doc_id, userID, docStatus, documentdesc, db,docSubStatus)
        except Exception as e:
            logger.info(traceback.format_exc())
        return_status["Validation failed"] = {"status": 0,
                                                "StatusCode":0,
                                                "response": [
                                                                f"Error: {e}"
                                                            ],
                                            }
        return return_status
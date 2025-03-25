import re
import traceback
from datetime import datetime, timezone
from pfg_app.FROps.corp_validations import validate_corpdoc
from pfg_app.FROps.customCall import date_cnv
from pfg_app.logger_module import logger
import pfg_app.model as model
from pfg_app.session.session import SQLALCHEMY_DATABASE_URL, get_db
from pfg_app.FROps.vendor_map import matchVendorCorp
from sqlalchemy import func
import pandas as pd
from urllib.parse import unquote
# utc_timestamp = datetime.now(timezone.utc)
import pytz as tz
tz_region = tz.timezone("US/Pacific")
 

def clean_amount(amount_str):
    if amount_str in [None,""]:
        return  0.0
    if isinstance(amount_str, float):
        amount_str = str(amount_str)
    try:
        cleaned_amount = re.findall(r"[\d.]+", amount_str)
        if cleaned_amount:
            return round(float("".join(cleaned_amount)), 2)
    except Exception:
        return 0.0
    return 0.0

def crd_clean_amount(amount_str):
    if isinstance(amount_str, float):
        amount_str = str(amount_str)
    elif isinstance(amount_str, int):
        amount_str = str(amount_str)
    try:
        cleaned_amount = re.findall(r"[\d.]+", amount_str)
        if cleaned_amount:
            cleaned_value = round(float("".join(cleaned_amount)), 2) * -1
            # Ensure -0.0 is converted to 0.0
            if cleaned_value == -0.0:
                return 0.0
            return cleaned_value
    except Exception:
        logger.info(traceback.format_exc())
        return 0.0
    return 0.0

def cleanAmt_all(credit_invo, amount_str):
    if credit_invo ==1:
        rtn_amt = crd_clean_amount(amount_str)
    else:
        rtn_amt = clean_amount(amount_str)
    return rtn_amt

# def remove_special_chars(s):
#     return re.sub(r'[^a-zA-Z0-9]', '', s)

# def clean_invoice_ids(data):
#     # Function to clean invoice IDs
#     def clean_value(value):
#         if isinstance(value, str):
#             return re.sub(r'[^a-zA-Z0-9]', '', value)  # Remove special characters
#         return value
#     invoID_lw = {}
#     # Cleaning in invoiceDetails section
#     if 'coding_details' in data and 'invoiceDetails' in data['coding_details']:
#         if 'invoice#' in data['coding_details']['invoiceDetails']:
#             invoice_value = data['coding_details']['invoiceDetails']['invoice#']
#             if isinstance(invoice_value, list):
#                 data['coding_details']['invoiceDetails']['invoice#'] = [clean_value(v) for v in invoice_value]
#             else:
#                 data['coding_details']['invoiceDetails']['invoice#'] = clean_value(invoice_value)
    
#     # Cleaning in invoice_detail_list section
#     if 'invoice_detail_list' in data:
#         for invoice in data['invoice_detail_list']:
#             for key, value in invoice.items():
#                 if 'InvoiceID' in value:
#                     value['InvoiceID'] = clean_value(value['InvoiceID'])
    
#     return data
def clean_invoice_ids(data):
    def clean_value(value):
        if isinstance(value, str):
            cleaned = re.sub(r'[^a-zA-Z0-9]', '', value)  # Remove special characters
            lower_cleaned = cleaned.lower()
            invoID_lw[lower_cleaned] = cleaned  # Store lowercase key → original cleaned value
            return lower_cleaned  # Store lowercase version in cleaned_data
        return value

    invoID_lw = {}  # Dictionary to store mappings (lowercase → original cleaned value)

    # Cleaning in invoiceDetails section
    if 'coding_details' in data and 'invoiceDetails' in data['coding_details']:
        if 'invoice#' in data['coding_details']['invoiceDetails']:
            invoice_value = data['coding_details']['invoiceDetails']['invoice#']
            if isinstance(invoice_value, list):
                cleaned_values = [clean_value(v) for v in invoice_value]
                data['coding_details']['invoiceDetails']['invoice#'] = cleaned_values
            else:
                data['coding_details']['invoiceDetails']['invoice#'] = clean_value(invoice_value)

    # Cleaning in invoice_detail_list section
    if 'invoice_detail_list' in data:
        for invoice in data['invoice_detail_list']:
            for key, value in invoice.items():
                if isinstance(value, dict) and 'InvoiceID' in value:
                    cleaned_value = clean_value(value['InvoiceID'])
                    value['InvoiceID'] = cleaned_value  # Store lowercase in cleaned data

    return data, invoID_lw  # Return cleaned data and mappings


def corp_postPro(op_unCl_1,mail_row_key,file_path,sender,mail_rw_dt,queue_task_id):
    update_FR_status = 0
    update_FR_status_msg = "Failed in postprocessing"
    db = next(get_db())
    
    try:
        # Cleaning invoice IDs
        op_1,invoID_lw = clean_invoice_ids(op_unCl_1)
        
    except Exception as e:
        op_1 = op_unCl_1
        logger.error(f"Error cleaning invoice IDs:input:{op_unCl_1} --error: {e},")
        logger.info(f"Error in unquote: {traceback.format_exc()}")
    try: 
        coding_approverName = op_1.get("coding_details", {}).get("approverDetails", {}).get("approverName", "")
        coding_approver_Designation = op_1.get("coding_details", {}).get("approverDetails", {}).get("title", "")
        invoice_ApproverName = op_1.get("approval_details", {}).get("Approver", "")
        invo_approver_email = op_1.get("approval_details", {}).get("from", "")
        invo_approver_Designation = op_1.get("approval_details", {}).get("Designation", "")
    except Exception:
        logger.info(f"Error in getting approver details: {traceback.format_exc()}")
        coding_approverName = ""
        invoice_ApproverName = ""
        invo_approver_email = ""
        invo_approver_Designation = ""
        coding_approver_Designation = ""
    try:
        if op_1['approval_details'].get("Approved keyword exists", "").lower() == "yes":
            approval_status = "Approved" if re.sub(r'[^a-zA-Z0-9]', '', str(op_1['approval_details'].get('Approved keyword', '')).lower()) == 'approved' else "Not approved"
        else:
            approval_status = "Not approved"
    except Exception:
            approval_status = "Not approved"
    
    try:
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

        #corp_metadata
        corp_metadata_query = db.query(model.corp_metadata)
        corp_metadata_rows = corp_metadata_query.all()

        # Convert list of ORM objects to a list of dictionaries
        corp_metadata_data = [row.__dict__ for row in corp_metadata_rows]


        # for row in corp_metadata_data:
        #     row.pop('_sa_instance_state', None)

        # Create DataFrame
        corp_metadata_df = pd.DataFrame(corp_metadata_data)
    except Exception as e:
        logger.error(f"Error in getting vendor mapping: {e}")
        logger.info(traceback.format_exc())
    try:
        creditLt = []
        credit_notes = {}
        for invoice in op_1["invoice_detail_list"]:
            for file_name, details in invoice.items():
                credit_notes[file_name] = details.get("CreditNote", "N/A")
                creditLt.append(details.get("CreditNote", "N/A"))
        # Print the extracted CreditNote values
        for file_name, credit_note in credit_notes.items():
            logger.info(f"File: {file_name}, CreditNote: {credit_note}")
        if len(creditLt) > 0:
            if str(creditLt[0]).lower() == "yes":
                credit_invo = 1
            elif str(creditLt[0]).lower() == "no":
                credit_invo = 0
            else:
                credit_invo = 2
        else:
            credit_invo = 2

    except Exception:
        credit_invo = 2
        
    corp_doc_id = ""
    
    timestmp =datetime.now(tz_region) 
    coding_tab_data = {}
    # credit_invo = 0
    coding_data = {}
    all_invo_coding = {}
    map_invo_att = {}
    userID = 1
    lt_corp_doc_id = []
    temp_found = 0
    # approval_status = ""

    try:
        template_type = op_1['template_type']
    except Exception:
        template_type = ""
    try:
        file_path = unquote(file_path)
        
        email_filepath_pdf = unquote(file_path[:-3]+'pdf')
    except Exception:
        file_path = ""
        logger.info(f"Error in unquote: {traceback.format_exc()}")
        email_filepath_pdf = ""

    
    if 'invoice#' in op_1['coding_details']['invoiceDetails']:
        # template 2
        # if type(op_1['coding_details']['invoiceDetails']["invoice#"])==list:
        if template_type == 'Template 2':
            # multi invoice template:
            temp_found = 1
            if 'approval_details' in op_1:
                if 'Approver' in  op_1['approval_details']:
                    approverName = op_1['approval_details']['Approver']
                else:
                    approverName = "" 

                if "from" in  op_1['approval_details']:
                    approver_email =  op_1['approval_details']['from']
                else:
                    approver_email = ""

                if "sent" in op_1['approval_details']:
                    approved_on = op_1['approval_details']['sent']
                else:
                    approved_on = ""

                if "Designation" in op_1['approval_details']:
                    approver_title = op_1['approval_details']['Designation']
                else:
                    approver_title = ""

                # if "Approved keyword exists" in op_1['approval_details']:
                #     if op_1['approval_details']["Approved keyword exists"] == "yes":
                #         if 'Approved keyword' in op_1['approval_details']:
                #             cln_approval_status = re.sub(r'[^a-zA-Z0-9]', '', str(op_1['approval_details']['Approved keyword']).lower())
                #             if cln_approval_status =='approved' :
                #                 approval_status = "Approved"
                #             else:
                #                 approval_status = "Not approved"
            
            missing_val = []
            if "coding_details" in op_1:
                if "email_metadata" in op_1['coding_details']:
                    if "from" in op_1['coding_details']['email_metadata']:
                        if len(op_1['coding_details']['email_metadata']['from'].split("<"))==2:
                            sender = op_1['coding_details']['email_metadata']['from'].split("<")[0]
                            sender_email = op_1['coding_details']['email_metadata']['from'].split("<")[1][:-1]
                        else:
                            sender = ""
                            sender_email = ""
                            missing_val.append('sender_email')
                            missing_val.append('sender')
                    else:
                        sender = ""
                        sender_email = ""
                        missing_val.append('sender_email')
                        missing_val.append('sender')
                    if 'sent' in op_1['coding_details']['email_metadata']:
                        sent_time = op_1['coding_details']['email_metadata']['sent']
                    else:
                        sent_time= ""
                        missing_val.append('sent_time')
                    if 'to' in op_1['coding_details']['email_metadata']:
                        sent_to = op_1['coding_details']['email_metadata']['to']
                    else:
                        sent_to = ""
                        missing_val.append("sent_to")
                else:
                    missing_val.append("email_metadata")
                    missing_val.append("sender")
                    missing_val.append("sender_email")
                    missing_val.append("sent_time")
                    missing_val.append("sent_to")
                if "TMID" in  op_1['coding_details']['approverDetails']:
                        TMID =  op_1['coding_details']['approverDetails']['TMID']
                else:
                    TMID = ""
            #- process multi invoice: 
            invoice_details = op_1['coding_details']['invoiceDetails']
            keys_to_check_invo = ['invoice#','store', 'dept', 'account', 'SL', 'project', 'activity','GST','invoicetotal']
            # try:
            #     if "Approved keyword exists" in op_1['approval_details']:
            #         if op_1['approval_details']["Approved keyword exists"] == "yes":
            #             if 'Approved keyword' in op_1['approval_details']:
            #                 cln_approval_status = re.sub(r'[^a-zA-Z0-9]', '', str(op_1['approval_details']['Approved keyword']).lower())
            #                 if cln_approval_status =='approved' :
            #                     approval_status = "Approved"
            #                 else:
            #                     approval_status = "Not approved"
            # except Exception:
            #     logger.info(f"Error in getting approval status: {traceback.format_exc()}")
            #     approval_status = "Not approved"
            if all(len(invoice_details[keys_to_check_invo[0]]) == len(invoice_details[key]) for key in keys_to_check_invo[1:]):
                for rw in range(len(invoice_details[keys_to_check_invo[0]])):
                    coding_data = {}
                    coding_tab_data = {}
                    
                    
                    if credit_invo == 1:   
                        coding_tab_data['document_type'] = "credit"
                    elif credit_invo == 0:
                        coding_tab_data['document_type'] = "invoice"
                    else:
                        coding_tab_data['document_type'] = "Unknown"
                    coding_tab_data['sender'] = sender
                    coding_tab_data['sender_email'] = sender_email
                    coding_tab_data['sent_to'] = sent_to
                    coding_tab_data['sent_time'] = sent_time
                    coding_tab_data["gst"] = cleanAmt_all(credit_invo, invoice_details['GST'][rw])
                    coding_tab_data["invoice_number"]  = invoice_details['invoice#'][rw]
                    coding_tab_data['approverName'] = coding_approverName
                    coding_tab_data["approver_email"] = invo_approver_email 
                    coding_tab_data["approved_on"] = approved_on
                    coding_tab_data["approver_title"] = coding_approver_Designation
                    coding_tab_data["approval_status"] = approval_status
                    coding_tab_data["TMID"] = TMID
                    coding_tab_data["invoicetotal"] =cleanAmt_all(credit_invo, invoice_details["invoicetotal"][rw])
                    coding_data[1] = {'store':invoice_details['store'][rw],
                                            'dept':invoice_details['dept'][rw],
                                            'account':invoice_details['account'][rw],
                                            'SL':invoice_details['SL'][rw],
                                            'project':invoice_details['project'][rw],
                                            'activity':invoice_details['activity'][rw],
                                            'amount':cleanAmt_all(credit_invo,float(cleanAmt_all(credit_invo,invoice_details['invoicetotal'][rw]))  - float(cleanAmt_all(credit_invo,invoice_details['GST'][rw])))
                    }
                    coding_tab_data['coding_data'] = coding_data
                    print(invoice_details['invoice#'][rw])
                
                    
                    all_invo_coding[invoice_details['invoice#'][rw]] = coding_tab_data
        elif template_type == 'Template 1' or template_type == 'Template 3' or template_type == 'Unknown Template':
            # template 1 & 3
            temp_found = 1
            missing_val = []
            #email_metadata
            if "coding_details" in op_1:
                if "email_metadata" in op_1['coding_details']:
                    if "from" in op_1['coding_details']['email_metadata']:
                        if len(op_1['coding_details']['email_metadata']['from'].split("<"))==2:
                            coding_tab_data['sender'] = op_1['coding_details']['email_metadata']['from'].split("<")[0]
                            coding_tab_data['sender_email'] = op_1['coding_details']['email_metadata']['from'].split("<")[1][:-1]
                        else:
                            missing_val.append('sender')
                            missing_val.append('sender_email')
                    else:
                        missing_val.append('sender')
                        missing_val.append('sender_email')
                    if 'sent' in op_1['coding_details']['email_metadata']:
                        coding_tab_data['sent_time'] = op_1['coding_details']['email_metadata']['sent']
                    else:
                        missing_val.append('sent_time')
                    if 'to' in op_1['coding_details']['email_metadata']:
                        coding_tab_data['sent_to'] = op_1['coding_details']['email_metadata']['to']
                    else:
                        missing_val.append("sent_to")
                else:
                    missing_val.append("email_metadata")
                    missing_val.append("sender")
                    missing_val.append("sender_email")
                    missing_val.append("sent_time")
                    missing_val.append("sent_to")


                if "invoiceDetails" in op_1['coding_details']:
                    if "invoicetotal" in op_1['coding_details']['invoiceDetails']:
                        if credit_invo ==1:
                            coding_tab_data['document_type'] = "credit"
                        elif credit_invo == 0:
                            coding_tab_data['document_type'] = "invoice"
                        else:
                            coding_tab_data['document_type'] = "Unknown"
                        c_invoTotal = cleanAmt_all(credit_invo, op_1['coding_details']['invoiceDetails']['invoicetotal'])
                    if c_invoTotal:
                        coding_tab_data['invoicetotal'] = c_invoTotal
                    else:
                        coding_tab_data['invoicetotal'] = None
                    if "GST" in op_1['coding_details']['invoiceDetails']:
                        c_gst = cleanAmt_all(credit_invo, op_1['coding_details']['invoiceDetails']['GST'])
                        coding_tab_data["gst"] = c_gst
                    if 'invoice#' in op_1['coding_details']['invoiceDetails']:
                        c_invoID = op_1['coding_details']['invoiceDetails']['invoice#']
                        coding_tab_data["invoice_number"] = c_invoID
                    invoice_details = op_1['coding_details']['invoiceDetails']
                    keys_to_check = ['store', 'dept', 'account', 'SL', 'project', 'activity', 'amount']
                    if (len(invoice_details[keys_to_check[0]]))==1:
                        keys_to_check =  ['store', 'dept', 'account', 'SL', 'project', 'activity']


                    # Check if all lengths are equal
                    if all(len(invoice_details[keys_to_check[0]]) == len(invoice_details[key]) for key in keys_to_check[1:]):
                        if (len(invoice_details[keys_to_check[0]]))==1:
                            coding_data[1] = {'store':invoice_details['store'][0],
                                                'dept':invoice_details['dept'][0],
                                                'account':invoice_details['account'][0],
                                                'SL':invoice_details['SL'][0],
                                                'project':invoice_details['project'][0],
                                                'activity':invoice_details['activity'][0],
                                                'amount':cleanAmt_all(credit_invo,invoice_details['amount'][0])
                                                }
                        else:
                            for rw in range(len(invoice_details[keys_to_check[0]])):
                                coding_data[rw+1] = {'store':invoice_details['store'][rw],
                                                    'dept':invoice_details['dept'][rw],
                                                    'account':invoice_details['account'][rw],
                                                    'SL':invoice_details['SL'][rw],
                                                    'project':invoice_details['project'][rw],
                                                    'activity':invoice_details['activity'][rw],
                                                    'amount':cleanAmt_all(credit_invo,invoice_details['amount'][rw])}
                        coding_tab_data["coding_data"] = coding_data
                    else:
                        coding_tab_data["coding_data"] = {}
                if 'approverDetails' in op_1['coding_details']:

                    if "TMID" in  op_1['coding_details']['approverDetails']:
                        coding_tab_data["TMID"] =  op_1['coding_details']['approverDetails']['TMID']
            if 'approval_details' in op_1:
                if 'Approver' in  op_1['approval_details']:
                    coding_tab_data['approverName'] = coding_approverName 
                else:
                    coding_tab_data['approverName'] = "" 

                if "from" in  op_1['approval_details']:
                    coding_tab_data["approver_email"] =  invo_approver_email 
                else:
                    coding_tab_data["approver_email"] = ""

                if "sent" in op_1['approval_details']:
                    coding_tab_data["approved_on"] = op_1['approval_details']['sent']
                else:
                    coding_tab_data["approved_on"] = ""

                if "Designation" in op_1['approval_details']:
                    coding_tab_data["approver_title"] = coding_approver_Designation
                else:
                    coding_tab_data["approver_title"] = ""

                # if "Approved keyword exists" in op_1['approval_details']:
                #     if str(op_1['approval_details']["Approved keyword exists"]).lower() == "yes":
                #         if 'Approved keyword' in op_1['approval_details']:
                #             cln_approval_status = re.sub(r'[^a-zA-Z0-9]', '', str(op_1['approval_details']['Approved keyword']).lower())
                #             if cln_approval_status =='approved' :
                #                 approval_status = "Approved"
                #             else:
                #                 approval_status = "Not approved"

                            
            all_invo_coding[c_invoID] = coding_tab_data
        else:
            temp_found = 0
            logger.info(f"No template found: data: {op_1}, mail_row_key: {mail_row_key}, file_path: {file_path}, sender: {sender}, mail_rw_dt : {mail_rw_dt} ")

    
    
    if temp_found==1:
        if "invoice_detail_list" in op_1:
            for invo_att in op_1['invoice_detail_list']:
                invo_att.keys()
                map_invo_att[list(invo_att.keys())[0]] = invo_att[list(invo_att.keys())[0]]['InvoiceID']

        # invo without coding:
        missing_coding = set(map_invo_att.values()) - set(all_invo_coding.keys())

        # invo without attachment:
        missing_attachment = set(all_invo_coding.keys()) - set(map_invo_att.values())

        # both coding + attachment present
        good_togo = set(all_invo_coding.keys()) & set(map_invo_att.values())
    
        #lower case all invoice IDs
        good_togo_lc = {}
        for inv_id in  good_togo:
            good_togo_lc[str(inv_id).lower()]= inv_id
        # processing invoice with coding and attachment: 
        for doc_dt_rw in op_1['invoice_detail_list']:
            try:
                if str(doc_dt_rw[list(doc_dt_rw.keys())[0]]['InvoiceID']).lower() in good_togo_lc.keys():
                    att_invoID_lw = str(doc_dt_rw[list(doc_dt_rw.keys())[0]]['InvoiceID']).lower()
                    att_invoID = good_togo_lc[att_invoID_lw]
                    if 'invoicetotal' in doc_dt_rw[list(doc_dt_rw.keys())[0]]:
                        invTotl = doc_dt_rw[list(doc_dt_rw.keys())[0]]['invoicetotal']
                    else:
                        invTotl = doc_dt_rw[list(doc_dt_rw.keys())[0]]['InvoiceTotal']
                    if 'NumberOfPages' in doc_dt_rw[list(doc_dt_rw.keys())[0]]:
                        att_invoPageCount = doc_dt_rw[list(doc_dt_rw.keys())[0]]['NumberOfPages']
                    else:
                        att_invoPageCount = ''
                    att_invoTotal = cleanAmt_all(credit_invo,invTotl)
                    if "GST" in doc_dt_rw[list(doc_dt_rw.keys())[0]]:
                        gst_amt = doc_dt_rw[list(doc_dt_rw.keys())[0]]['GST']
                    else:
                        gst_amt = 0
                    # elif "GST" in doc_dt_rw[list(doc_dt_rw.keys())[0]]:
                    #     gst_amt = doc_dt_rw[list(doc_dt_rw.keys())[0]]['GST']
                    gst = cleanAmt_all(credit_invo, gst_amt)
                    att_invoDate = doc_dt_rw[list(doc_dt_rw.keys())[0]]['InvoiceDate']
                    
                    if credit_invo==1:
                        document_type = "Credit"
                    elif credit_invo == 0:
                        document_type = "Invoice"   
                    else:
                        document_type = "Unknown"
                    if list(doc_dt_rw.keys())[0] in mail_rw_dt:
                        pdf_blobpath = mail_rw_dt[list(doc_dt_rw.keys())[0]]["pdf_blob_path"]
                        corp_trg_id = mail_rw_dt[list(doc_dt_rw.keys())[0]]["corp_trigger_id"]
                        mail_row_key = mail_rw_dt[list(doc_dt_rw.keys())[0]]["mail_row_key"]
                        
                    else:
                        pdf_blobpath = ""
                        mail_row_key = ""
                        corp_trg_id = ""


                    try:
                        file_path = unquote(file_path)
                        pdf_blobpath = unquote(pdf_blobpath)
                        email_filepath_pdf = unquote(file_path[:-3]+'pdf')
                    except Exception:
                        logger.info(f"Error in unquote: {traceback.format_exc()}")


                    # insert to db
                    try:
                        doc_data_invoID_ck = invoID_lw[att_invoID]
                        logger.info(f"invoID_lw: {invoID_lw}")
                    except Exception:
                        doc_data_invoID_ck = att_invoID
                    try:
                        pg_cnt = int(att_invoPageCount)
                    except Exception:
                        pg_cnt = 0
                    corp_doc_data = {"invoice_id":doc_data_invoID_ck,
                                    "invoice_date":att_invoDate,
                                    "invoicetotal":att_invoTotal,
                                    "gst":gst,
                                    "invo_page_count":pg_cnt,
                                    "document_type":document_type,
                                    "documentstatus":4,
                                    "documentsubstatus":7,
                                    "created_on":timestmp,
                                    "mail_row_key": mail_row_key,
                                    "email_filepath": file_path,
                                    "invo_filepath": pdf_blobpath,
                                    "email_filepath_pdf":email_filepath_pdf,
                                    "sender": sender,
                                    "approved_by":op_1['approval_details']['Approver'],
                                    "approver_title":op_1['approval_details']['Designation'],
                                }
                    
                    try:
                        corp_doc = model.corp_document_tab(**corp_doc_data)
                        db.add(corp_doc)
                        db.commit()
                        logger.info(f"Corp document added: {corp_doc}")
                        corp_doc_id = corp_doc.corp_doc_id
                        logger.info(f"Corp document added {timestmp}- postpro: {corp_doc} ,op_1: {op_1}")
                        lt_corp_doc_id.append(corp_doc_id)
                        update_FR_status = 1
                        update_FR_status_msg = "Processed"

                    except Exception as e:
                        update_FR_status = 0
                        update_FR_status_msg = update_FR_status_msg+": "+str(e)
                        logger.info(f"Corp document not added: {corp_doc} ,op_1: {op_1}")
                        logger.info(traceback.format_exc())
                    
                    #updating trigger tab:
                    try:
                        logger.info(f"update to corp_trigger_tab: doc_dt_rw:{doc_dt_rw}")
                        if list(doc_dt_rw.keys())[0] in mail_rw_dt:
                            pdf_blobpath = mail_rw_dt[list(doc_dt_rw.keys())[0]]["pdf_blob_path"]
                            corp_trg_id = mail_rw_dt[list(doc_dt_rw.keys())[0]]["corp_trigger_id"]
                            mail_row_key = mail_rw_dt[list(doc_dt_rw.keys())[0]]["mail_row_key"]
                            
                        else:
                            pdf_blobpath = ""
                            mail_row_key = ""
                            corp_trg_id = ""
                        if corp_trg_id !="":

                            corp_trigger = db.query(model.corp_trigger_tab).filter_by(corp_trigger_id=corp_trg_id).first()
                            if corp_trigger:
                                corp_trigger.status = update_FR_status_msg
                                corp_trigger.documentid = corp_doc_id
                                corp_trigger.updated_at = datetime.now(tz_region)  # Ensure it's a datetime object
                                db.commit()  # Save changes

                    except Exception as e:  
                        logger.error( f"Error updating corp_trigger_tab: {e}")
                        # db.rollback()
                    # vendor mapping:
                    # query = db.query(
                    #     model.Vendor.idVendor,
                    #     model.Vendor.VendorName,
                    #     model.Vendor.Synonyms,
                    #     model.Vendor.Address,
                    #     model.Vendor.VendorCode,
                    # ).filter(
                    #     func.jsonb_extract_path_text(
                    #         model.Vendor.miscellaneous, "VENDOR_STATUS"
                    #     )
                    #     == "A"
                    # )
                    # rows = query.all()
                    # columns = ["idVendor", "VendorName", "Synonyms", "Address", "VendorCode"]

                    # vendorName_df = pd.DataFrame(rows, columns=columns)

                    # #corp_metadata
                    # corp_metadata_query = db.query(model.corp_metadata)
                    # corp_metadata_rows = corp_metadata_query.all()

                    # # Convert list of ORM objects to a list of dictionaries
                    # corp_metadata_data = [row.__dict__ for row in corp_metadata_rows]


                    # # for row in corp_metadata_data:
                    # #     row.pop('_sa_instance_state', None)

                    # # Create DataFrame
                    # corp_metadata_df = pd.DataFrame(corp_metadata_data)

                    vendorname = doc_dt_rw[list(doc_dt_rw.keys())[0]]["VendorName"]
                    vendor_address = doc_dt_rw[list(doc_dt_rw.keys())[0]]["VendorAddress"]
                    matchVendorCorp(vendorname,vendor_address,corp_metadata_df,vendorName_df, userID,corp_doc_id,db)
                    try:
                        
                        app_status =  approval_status
                    except Exception:
                        app_status = "Not approved"
                    # update coding details 
                    coding_data_insert = {
                        'invoice_id': all_invo_coding[att_invoID].get('invoice_number', ""),
                        'corp_doc_id': corp_doc_id,
                        'coding_details': all_invo_coding[att_invoID].get('coding_data', ""),
                        'approver_name': all_invo_coding[att_invoID].get('approverName', ""),
                        'tmid': all_invo_coding[att_invoID].get('TMID', ""),
                        'approver_title': all_invo_coding[att_invoID].get('approver_title', ""),
                        'invoicetotal': cleanAmt_all(credit_invo, all_invo_coding[att_invoID].get('invoicetotal', 0)),
                        'gst': cleanAmt_all(credit_invo, all_invo_coding[att_invoID].get('gst', 0)),
                        'created_on': timestmp,
                        'sender_name': all_invo_coding[att_invoID].get('sender', ""),
                        'sender_email': all_invo_coding[att_invoID].get('sender_email', ""),
                        'sent_to': all_invo_coding[att_invoID].get('sent_to', ""),
                        'sent_time': all_invo_coding[att_invoID].get('sent_time', ""),
                        'approver_email': all_invo_coding[att_invoID].get('approver_email', ""),
                        'approved_on': all_invo_coding[att_invoID].get('approved_on', ""),
                        'approval_status': app_status,
                        'document_type': all_invo_coding[att_invoID].get('document_type', ""),
                        'template_type': template_type,
                        'mail_rw_key': mail_row_key,
                        'queue_task_id': queue_task_id,
                        'map_type': "System map",
                    }

                    

                    corp_coding_insert = model.corp_coding_tab(**coding_data_insert)
                    db.add(corp_coding_insert)
                    db.commit()
                    corp_code_id = corp_coding_insert.corp_coding_id
                    print("corp_code_id: ",corp_code_id)
                    
                    # insert doc data:
                    if "invoicetotal" in doc_dt_rw[list(doc_dt_rw.keys())[0]]:
                        cln_invoTotal = doc_dt_rw[list(doc_dt_rw.keys())[0]]["invoicetotal"]
                    else:
                        cln_invoTotal = doc_dt_rw[list(doc_dt_rw.keys())[0]]["InvoiceTotal"]
                    try:
                        crd_nt = doc_dt_rw[list(doc_dt_rw.keys())[0]]["CreditNote"]
                    
                        if crd_nt.lower() == "yes":
                            document_type = "credit"
                        else:
                            document_type = "invoice"
                    except Exception:
                        document_type = ""

                    pdf_invoTotal = cleanAmt_all(credit_invo,cln_invoTotal)
                    pdf_gst = cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]["GST"])
                    pdf_subTotal = cleanAmt_all(credit_invo, pdf_invoTotal-pdf_gst)

                    try:
                        docData_invoID_ck = invoID_lw[doc_dt_rw[list(doc_dt_rw.keys())[0]]["InvoiceID"]]
                        logger.info(f"invoID_lw: {invoID_lw}")
                    except Exception:
                        docData_invoID_ck = doc_dt_rw[list(doc_dt_rw.keys())[0]]["InvoiceID"]
                    try:
                        misc_amt = cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]["misc"])
                    except Exception:
                        misc_amt = 0    
                    corp_docdata_insert = {"invoice_id":docData_invoID_ck,
                                "invoice_date":doc_dt_rw[list(doc_dt_rw.keys())[0]]["InvoiceDate"],
                                    "vendor_name":doc_dt_rw[list(doc_dt_rw.keys())[0]]["VendorName"],
                                "vendoraddress":doc_dt_rw[list(doc_dt_rw.keys())[0]]["VendorAddress"],
                                "customername":"",
                                "customeraddress": "",
                                "currency":doc_dt_rw[list(doc_dt_rw.keys())[0]]["Currency"],
                                "document_type" : document_type,
                                "invoicetotal":pdf_invoTotal,
                                "subtotal":pdf_subTotal,
                                "corp_doc_id":corp_doc_id,
                                "bottledeposit":cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]['Bottle Deposit']),
                                "shippingcharges":cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]["Shipping Charges"]),
                                "litterdeposit":cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]["Litter Deposit"]),
                                "gst":pdf_gst,
                                "pst":cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]["PST"]),
                                "created_on":timestmp,
                                "pst_sk":cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]["PST-SK"]),
                                "pst_bc":cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]["PST-BC"]),
                                "ecology_fee":cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]["Ecology Fee"]),
                                "misc":misc_amt,
                                "approver": invoice_ApproverName ,
                                "approver_title": invo_approver_Designation
                                }
                    

                    corp_docdata_insert_data = model.corp_docdata(**corp_docdata_insert)
                    db.add(corp_docdata_insert_data)
                    db.commit()
                    corp_data_id = corp_docdata_insert_data.docdata_id
                    print("corp_data_id: ",corp_data_id)
            except Exception as er:
                logger.error(f"Error in inserting corp_docdata: {er} =>" + traceback.format_exc())
                

        # processing invoice without attachment:
        # document status & substatus:  4 , 130
        try:
            if list(doc_dt_rw.keys())[0] in mail_rw_dt:
                    pdf_blobpath = mail_rw_dt[list(doc_dt_rw.keys())[0]]["pdf_blob_path"]
                    corp_trg_id = mail_rw_dt[list(doc_dt_rw.keys())[0]]["corp_trigger_id"]
                    mail_row_key = mail_rw_dt[list(doc_dt_rw.keys())[0]]["mail_row_key"]
                    
            else:
                pdf_blobpath = ""
                mail_row_key = ""
                corp_trg_id = ""
        except Exception:
            logger.info(f"Error in mail_rw_dt: {traceback.format_exc()}")
            pdf_blobpath = ""
            mail_row_key = ""
            corp_trg_id = ""
       
        for miss_att in missing_attachment:
            try:
                docData_invoID_ck2 = invoID_lw[all_invo_coding[miss_att]["invoice_number"]]
                logger.info(f"invoID_lw: {invoID_lw}")
            except Exception:
                docData_invoID_ck2 = all_invo_coding[miss_att]["invoice_number"]

            # update coding details
            try:
                app_status = approval_status
            except Exception:   
                app_status = "Not approved"

            coding_data_insert = {
                'invoice_id': all_invo_coding[miss_att].get('invoice_number', ""),
                # 'corp_doc_id': corp_doc_id,
                'coding_details': all_invo_coding[miss_att].get('coding_data', ""),
                'approver_name': all_invo_coding[miss_att].get('approverName', ""),
                'tmid': all_invo_coding[miss_att].get('TMID', ""),
                'approver_title': all_invo_coding[miss_att].get('approver_title', ""),
                'invoicetotal': cleanAmt_all(credit_invo, all_invo_coding[miss_att].get('invoicetotal', 0)),
                'gst': cleanAmt_all(credit_invo, all_invo_coding[miss_att].get('gst', 0)),
                'created_on': timestmp,
                'sender_name': all_invo_coding[miss_att].get('sender', ""),
                'sender_email': all_invo_coding[miss_att].get('sender_email', ""),
                'sent_to': all_invo_coding[miss_att].get('sent_to', ""),
                'sent_time': all_invo_coding[miss_att].get('sent_time', ""),
                'approver_email': all_invo_coding[miss_att].get('approver_email', ""),
                'approved_on': all_invo_coding[miss_att].get('approved_on', ""),
                'approval_status': app_status,
                'document_type': all_invo_coding[miss_att].get('document_type', ""),
                'template_type': template_type,
                'mail_rw_key': mail_row_key,
                'queue_task_id': queue_task_id,
                'map_type': "Unmapped",
            }

            corp_coding_insert = model.corp_coding_tab(**coding_data_insert)
            db.add(corp_coding_insert)
            db.commit()
        
            
        # porcessing without coding details:
        # document status & substatus: 4 , 134
        for miss_code in op_1['invoice_detail_list']:
            logger.info(f"miss_code: {miss_code}")
            pdf_blobpath = mail_rw_dt[list(miss_code.keys())[0]]["pdf_blob_path"]
            corp_trg_id = mail_rw_dt[list(miss_code.keys())[0]]["corp_trigger_id"]
            mail_row_key = mail_rw_dt[list(miss_code.keys())[0]]["mail_row_key"]

            if miss_code[list(miss_code.keys())[0]]["InvoiceID"] in missing_coding:
                try:
                    docData_invoID_ck3 = invoID_lw[miss_code[list(miss_code.keys())[0]]['InvoiceID']]
                    logger.info(f"invoID_lw: {invoID_lw}")
                except Exception:
                    docData_invoID_ck3 = miss_code[list(miss_code.keys())[0]]['InvoiceID']
                try: 
                    pg_cnt = int(miss_code[list(miss_code.keys())[0]]["NumberOfPages"])
                except Exception:
                    pg_cnt = 0

                missing_code_docTab = {
                    "invoice_id":docData_invoID_ck3,
                    "invoicetotal":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["invoicetotal"]),
                    "email_filepath": file_path,
                    "invo_filepath": pdf_blobpath,
                    "mail_row_key": mail_row_key,
                    "email_filepath_pdf":email_filepath_pdf,
                    "gst":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["GST"]),
                    "invo_page_count":pg_cnt,
                    "created_on":timestmp,
                    "updated_on":timestmp,
                    "documentstatus":4,
                    "documentsubstatus":134,
                    "vendor_id":0,
                    
                }  
                try:
                    corp_doc = model.corp_document_tab(**missing_code_docTab)
                    db.add(corp_doc)
                    db.commit()
                    corp_doc_id = corp_doc.corp_doc_id
                    update_FR_status_msg = "Processed"
                except Exception as e:
                    update_FR_status = 0
                    update_FR_status_msg = update_FR_status_msg+": "+str(e)
                    logger.info(f"Corp document not added: {corp_doc} ,op_1: {op_1}")
                    logger.info(traceback.format_exc())
                    corp_doc_id = ""

                # update to trigger tab: 
                
                try:
                    logger.info(f"update to corp_trigger_tab: miss_code:{miss_code}")
                    if list(miss_code.keys())[0] in mail_rw_dt:
                        pdf_blobpath = mail_rw_dt[list(miss_code.keys())[0]]["pdf_blob_path"]
                        corp_trg_id = mail_rw_dt[list(miss_code.keys())[0]]["corp_trigger_id"]
                        mail_row_key = mail_rw_dt[list(miss_code.keys())[0]]["mail_row_key"]
                        
                    else:
                        pdf_blobpath = ""
                        mail_row_key = ""
                        corp_trg_id = ""
                    if corp_trg_id !="":

                        corp_trigger = db.query(model.corp_trigger_tab).filter_by(corp_trigger_id=corp_trg_id).first()
                        if corp_trigger:
                            corp_trigger.status = update_FR_status_msg
                            corp_trigger.documentid = corp_doc_id
                            corp_trigger.updated_at = datetime.now(tz_region)  # Ensure it's a datetime object
                            db.commit()  # Save changes

                except Exception as e:  
                    logger.error( f"Error updating corp_trigger_tab: {e}")

                #-----------------------

                #vendor mapping:
                try:
                    logger.info(f"vendor mapping: miss_code:{miss_code}")
                    vendorname = miss_code[list(miss_code.keys())[0]]["VendorName"]
                    vendor_address =miss_code[list(miss_code.keys())[0]]["VendorAddress"]
                    matchVendorCorp(vendorname,vendor_address,corp_metadata_df,vendorName_df, userID,corp_doc_id,db)
                except Exception as e:
                    logger.info(f"Error in vendor mapping: {e}")
                    logger.info(traceback.format_exc())

                #-------------------------
                
                lt_corp_doc_id.append(corp_doc_id)
                print("corp_doc_id: ",corp_doc_id)
                pdf_invoTotal = cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["invoicetotal"])
                pdf_gst = cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["GST"])
                pdf_subTotal = cleanAmt_all(credit_invo, pdf_invoTotal-pdf_gst)
                try:
                    crd_nt = miss_code[list(miss_code.keys())[0]]["CreditNote"]
                    if crd_nt.lower() == "yes":
                        document_type = "credit"
                    else:
                        document_type = "invoice"
                except Exception:
                    document_type = ""
                # update document data tab:
                # insert doc data:

                try:
                    docData_invoID_ck5 = invoID_lw[miss_code[list(miss_code.keys())[0]]["InvoiceID"]]
                    logger.info(f"invoID_lw: {invoID_lw}")
                except Exception:
                    docData_invoID_ck5 = miss_code[list(miss_code.keys())[0]]["InvoiceID"]
                try:
                    misc_amt = cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["misc"])
                except Exception:
                    misc_amt = 0 
                corp_docdata_insert = {"invoice_id":docData_invoID_ck5,
                            "invoice_date":miss_code[list(miss_code.keys())[0]]["InvoiceDate"],
                                "vendor_name":miss_code[list(miss_code.keys())[0]]["VendorName"],
                            "vendoraddress":miss_code[list(miss_code.keys())[0]]["VendorAddress"],
                            "customername":"",
                            "customeraddress": "",
                            "currency":miss_code[list(miss_code.keys())[0]]["Currency"],
                            "document_type" : document_type,
                            "invoicetotal":pdf_invoTotal,
                            "subtotal": pdf_subTotal,
                            "corp_doc_id":corp_doc_id,
                            "bottledeposit":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]['Bottle Deposit']),
                            "shippingcharges":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["Shipping Charges"]),
                            "litterdeposit":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["Litter Deposit"]),
                            "gst":pdf_gst,
                            "pst":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["PST"]),
                            "created_on":timestmp,
                            "pst_sk":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["PST-SK"]),
                            "pst_bc":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["PST-BC"]),
                            "ecology_fee":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["Ecology Fee"]),
                            "misc":misc_amt,
                            "approver": invoice_ApproverName ,
                            "approver_title":invo_approver_Designation
                            }
                corp_docdata_insert_data = model.corp_docdata(**corp_docdata_insert)
                db.add(corp_docdata_insert_data)
                db.commit()
                logger.info(f"Corp document data added: {corp_docdata_insert_data}")
                corp_data_id = corp_docdata_insert_data.docdata_id
                print("corp_data_id: ",corp_data_id)
        try:
            for doc_id_ in set(lt_corp_doc_id):
                if doc_id_ != "":
                    skipConf = 0
                    validate_corpdoc(doc_id_,userID,skipConf, db)
        except Exception:
            logger.error(traceback.format_exc())
    try:
        
        try:
            # # Check if the record exists in corp_coding_tab
            # existing_coding = db.query(model.corp_coding_tab).filter_by(corp_doc_id=corp_doc_id).first()

            # if not existing_coding:
            #     # Insert new record if it doesn't exist
            #     coding_data_insert = {
            #         "corp_doc_id": corp_doc_id,
            #         "created_on": timestmp,
            #         "template_type": template_type,
            #     }
            #     corp_coding_insert = model.corp_coding_tab(**coding_data_insert)
            #     db.add(corp_coding_insert)
            #     db.flush()  # Flush to get ID without committing
            #     corp_code_id = corp_coding_insert.corp_coding_id
            #     print("corp_code_id: ", corp_code_id)
            try:
                # Check if the record exists in corp_docdata:
                if not corp_doc_id or corp_doc_id == '':
                    existing_docdata = None
                else:
                    corp_doc_id = int(corp_doc_id)
                    existing_docdata = db.query(model.corp_docdata).filter_by(corp_doc_id=corp_doc_id).first()
            except: 
                existing_docdata = None

            if not existing_docdata:  # Only insert if the record does NOT exist
                corp_docdata_insert = {"corp_doc_id": corp_doc_id}
                corp_docdata_insert_data = model.corp_docdata(**corp_docdata_insert)
                db.add(corp_docdata_insert_data)
                db.flush()  # Flush to get ID without committing
                corp_data_id = corp_docdata_insert_data.docdata_id
                print("corp_data_id: ", corp_data_id)

            # Commit only if any new records were inserted
            db.commit()

        except Exception as e:
            db.rollback()  # Rollback transaction in case of error
            logger.info(f"Error: {str(e)}")
            logger.error(traceback.format_exc())
    except Exception as e:
            db.rollback()  # Rollback transaction in case of error
            logger.info(f"Error: {str(e)}")
            logger.error(traceback.format_exc())
      
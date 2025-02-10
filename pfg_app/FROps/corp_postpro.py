import re
import traceback
from datetime import datetime, timezone
from pfg_app.logger_module import logger
import pfg_app.model as model
from pfg_app.session.session import SQLALCHEMY_DATABASE_URL, get_db
from pfg_app.FROps.vendor_map import matchVendorCorp
from sqlalchemy import func
import pandas as pd
utc_timestamp = datetime.now(timezone.utc)
 

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
            cleaned_value = round(float("".join(cleaned_amount)), 2) * -1
            # Ensure -0.0 is converted to 0.0
            if cleaned_value == -0.0:
                return 0.0
            return cleaned_value
    except Exception:
        logger.info(traceback.format_exc())
        return None
    return 0.0

def cleanAmt_all(credit_invo, amount_str):
    if credit_invo ==1:
        rtn_amt = crd_clean_amount(amount_str)
    else:
        rtn_amt = clean_amount(amount_str)
    return rtn_amt

def corp_postPro(op_1):
    db = next(get_db())
    timestmp = utc_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    coding_tab_data = {}
    credit_invo = 0
    coding_data = {}
    all_invo_coding = {}
    map_invo_att = {}
    userID = 1

    if 'invoice#' in op_1['coding_details']['invoiceDetails']:
        if type(op_1['coding_details']['invoiceDetails']["invoice#"])==list:
            # multi invoice template:
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

                if "Approved keyword exists" in op_1['approval_details']:
                    if op_1['approval_details']["Approved keyword exists"] == "yes":
                        if 'Approved keyword' in op_1['approval_details']:
                            if str(op_1['approval_details']['Approved keyword']).lower() =='approved' :
                                approval_status = "Approved"
                            else:
                                approval_status = "Not approved"
            
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
                            missing_val.append('sender','sender_email')
                    else:
                        sender = ""
                        sender_email = ""
                        missing_val.append('sender','sender_email')
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
                    missing_val.appematchVendorCorpnd("email_metadata","sender","sender_email","sent_time","sent_to")
                if "TMID" in  op_1['coding_details']['approverDetails']:
                        TMID =  op_1['coding_details']['approverDetails']['TMID']
            #- process multi invoice: 
            invoice_details = op_1['coding_details']['invoiceDetails']
            keys_to_check_invo = ['invoice#','store', 'dept', 'account', 'SL', 'project', 'activity','GST','invoicetotal']
            if all(len(invoice_details[keys_to_check_invo[0]]) == len(invoice_details[key]) for key in keys_to_check_invo[1:]):
                for rw in range(len(invoice_details[keys_to_check_invo[0]])):
                    if '-' in invoice_details['invoicetotal'][rw]:
                        credit_invo = 1
                        coding_tab_data['document_type'] = "credit"
                    else:
                        coding_tab_data['document_type'] = "invoice"
                    coding_tab_data['sender'] = sender
                    coding_tab_data['sender_email'] = sender_email
                    coding_tab_data['sent_to'] = sent_to
                    coding_tab_data['sent_time'] = sent_time
                    coding_tab_data["gst"] = invoice_details['GST'][rw]
                    coding_tab_data["invoice_number"]  = invoice_details['invoice#'][rw]
                    coding_tab_data['approverName'] = approverName
                    coding_tab_data["approver_email"] = approver_email
                    coding_tab_data["approved_on"] = approved_on
                    coding_tab_data["approver_title"] = approver_title
                    coding_tab_data["approval_status"] = approval_status
                    coding_tab_data["TMID"] = TMID
                    coding_tab_data["invoicetotal"] = invoice_details["invoicetotal"][rw]
                    coding_data[1] = {'store':invoice_details['store'][rw],
                                            'dept':invoice_details['dept'][rw],
                                            'SL':invoice_details['SL'][rw],
                                            'project':invoice_details['project'][rw],
                                            'activity':invoice_details['activity'][rw],
                                            'amount':cleanAmt_all(credit_invo,invoice_details['invoicetotal'][rw])} 
                    coding_tab_data['coding_data'] = coding_data
                    print(invoice_details['invoice#'][rw])
                
            
                    all_invo_coding[invoice_details['invoice#'][rw]] = coding_tab_data
        else:
            missing_val = []
            #email_metadata
            if "coding_details" in op_1:
                if "email_metadata" in op_1['coding_details']:
                    if "from" in op_1['coding_details']['email_metadata']:
                        if len(op_1['coding_details']['email_metadata']['from'].split("<"))==2:
                            coding_tab_data['sender'] = op_1['coding_details']['email_metadata']['from'].split("<")[0]
                            coding_tab_data['sender_email'] = op_1['coding_details']['email_metadata']['from'].split("<")[1][:-1]
                        else:
                            missing_val.append('sender','sender_email')
                    else:
                        missing_val.append('sender','sender_email')
                    if 'sent' in op_1['coding_details']['email_metadata']:
                        coding_tab_data['sent_time'] = op_1['coding_details']['email_metadata']['sent']
                    else:
                        missing_val.append('sent_time')
                    if 'to' in op_1['coding_details']['email_metadata']:
                        coding_tab_data['sent_to'] = op_1['coding_details']['email_metadata']['to']
                    else:
                        missing_val.append("sent_to")
                else:
                    missing_val.append("email_metadata","sender","sender_email","sent_time","sent_to")

                if "invoiceDetails" in op_1['coding_details']:
                    if "invoicetotal" in op_1['coding_details']['invoiceDetails']:
                        if '-' in op_1['coding_details']['invoiceDetails']['invoicetotal']:
                            credit_invo = 1
                            coding_tab_data['document_type'] = "credit"
                            
                        else:
                            coding_tab_data['document_type'] = "invoice"
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
                                                'SL':invoice_details['SL'][0],
                                                'project':invoice_details['project'][0],
                                                'activity':invoice_details['activity'][0],
                                                'amount':c_invoTotal}
                        else:
                            for rw in range(len(invoice_details[keys_to_check[0]])):
                                coding_data[rw+1] = {'store':invoice_details['store'][rw],
                                                    'dept':invoice_details['dept'][rw],
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
                    coding_tab_data['approverName'] = op_1['approval_details']['Approver']
                else:
                    coding_tab_data['approverName'] = "" 

                if "from" in  op_1['approval_details']:
                    coding_tab_data["approver_email"] =  op_1['approval_details']['from']
                else:
                    coding_tab_data["approver_email"] = ""

                if "sent" in op_1['approval_details']:
                    coding_tab_data["approved_on"] = op_1['approval_details']['sent']
                else:
                    coding_tab_data["approved_on"] = ""

                if "Designation" in op_1['approval_details']:
                    coding_tab_data["approver_title"] = op_1['approval_details']['Designation']
                else:
                    coding_tab_data["approver_title"] = ""

                if "Approved keyword exists" in op_1['approval_details']:
                    if op_1['approval_details']["Approved keyword exists"] == "yes":
                        if 'Approved keyword' in op_1['approval_details']:
                            if str(op_1['approval_details']['Approved keyword']).lower() =='approved' :
                                coding_tab_data["approval_status"] = "Approved"
                            else:
                                coding_tab_data["approval_status"] = "Not approved"
            all_invo_coding[c_invoID] = coding_tab_data

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


    # processing invoice with coding and attachment: 
    for doc_dt_rw in op_1['invoice_detail_list']:
        if doc_dt_rw[list(doc_dt_rw.keys())[0]]['InvoiceID'] in good_togo:
            att_invoID = doc_dt_rw[list(doc_dt_rw.keys())[0]]['InvoiceID']
            if 'invoicetotal' in doc_dt_rw[list(doc_dt_rw.keys())[0]]:
                invTotl = doc_dt_rw[list(doc_dt_rw.keys())[0]]['invoicetotal']
            else:
                invTotl = doc_dt_rw[list(doc_dt_rw.keys())[0]]['InvoiceTotal']

            att_invoTotal = cleanAmt_all(credit_invo,invTotl)
            if "GST" in doc_dt_rw[list(doc_dt_rw.keys())[0]]:
                gst_amt = doc_dt_rw[list(doc_dt_rw.keys())[0]]['GST']
            # elif "GST" in doc_dt_rw[list(doc_dt_rw.keys())[0]]:
            #     gst_amt = doc_dt_rw[list(doc_dt_rw.keys())[0]]['GST']
            gst = cleanAmt_all(credit_invo, gst_amt)
            att_invoDate = doc_dt_rw[list(doc_dt_rw.keys())[0]]['InvoiceDate']
            
            # insert to db
            corp_doc_data = {"invoice_id":att_invoID,
                            "invoice_date":att_invoDate,
                            "invoicetotal":att_invoTotal,
                            "gst":gst,
                            # "documentstatus":4,
                            # "documentsubstatus":11,
                            "created_on":timestmp,
                        }
            corp_doc = model.corp_document_tab(**corp_doc_data)
            db.add(corp_doc)
            db.commit()
            logger.info(f"Corp document added: {corp_doc}")
            corp_doc_id = corp_doc.corp_doc_id
            print("corp_doc_id: ",corp_doc_id)
            
            # vendor mapping:
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



            vendorname = doc_dt_rw[list(doc_dt_rw.keys())[0]]["VendorName"]
            vendor_address =doc_dt_rw[list(doc_dt_rw.keys())[0]]["VendorAddress"]
            matchVendorCorp(vendorname,vendor_address,corp_metadata_df,vendorName_df, userID,corp_doc_id,db)
            
            # update coding details
            coding_data_insert = {'invoice_id':all_invo_coding[att_invoID]['invoice_number'],
                        'corp_doc_id':corp_doc_id,
                        'coding_details':all_invo_coding[att_invoID]['coding_data'],
                        'approver_name':all_invo_coding[att_invoID]['approverName'],
                        
                        'tmid':all_invo_coding[att_invoID]['TMID'],
                        'approver_title':all_invo_coding[att_invoID]['approver_title'],
                        'invoicetotal':all_invo_coding[att_invoID]['invoicetotal'],
                        'gst':all_invo_coding[att_invoID]['gst'],
                        'created_on': timestmp,
                        'sender_name': all_invo_coding[att_invoID]['sender'],
                        'sender_email':all_invo_coding[att_invoID]['sender_email'],
                        'sent_to':all_invo_coding[att_invoID]['sent_to'],
                        'sent_time':all_invo_coding[att_invoID]['sent_time'],
                        'approver_email':all_invo_coding[att_invoID]['approver_email'],
                        'approved_on':all_invo_coding[att_invoID]['approved_on'],
                        'approval_status':all_invo_coding[att_invoID]['approval_status'],
                        'document_type':all_invo_coding[att_invoID]['document_type']
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

            corp_docdata_insert = {"invoice_id":doc_dt_rw[list(doc_dt_rw.keys())[0]]["InvoiceID"],
                        "invoice_date":doc_dt_rw[list(doc_dt_rw.keys())[0]]["InvoiceDate"],
                            "vendor_name":doc_dt_rw[list(doc_dt_rw.keys())[0]]["VendorName"],
                        "vendoraddress":doc_dt_rw[list(doc_dt_rw.keys())[0]]["VendorAddress"],
                        "customername":"",
                        "customeraddress": "",
                        "currency":doc_dt_rw[list(doc_dt_rw.keys())[0]]["Currency"],
                        
                        "invoicetotal":cleanAmt_all(credit_invo,cln_invoTotal),
                        "subtotal":cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]["SubTotal"]),
                        
                        "corp_doc_id":corp_doc_id,
                        "bottledeposit":cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]['Bottle Deposit']),
                        "shippingcharges":cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]["Shipping Charges"]),
                        "litterdeposit":cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]["Litter Deposit"]),
                        "gst":cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]["GST"]),
                        "pst":cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]["PST"]),
                        "created_on":timestmp,
                        "pst_sk":cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]["PST-SK"]),
                        "pst_bc":cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]["PST-BC"]),
                        "ecology_fee":cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]["Ecology Fee"]),
                        "misc":cleanAmt_all(credit_invo,doc_dt_rw[list(doc_dt_rw.keys())[0]]["misc"]),
                        }
            

            corp_docdata_insert_data = model.corp_docdata(**corp_docdata_insert)
            db.add(corp_docdata_insert_data)
            db.commit()
            corp_data_id = corp_docdata_insert_data.docdata_id
            print("corp_data_id: ",corp_data_id)
    # processing invoice without attachment:
    # document status & substatus:  4 , 130
    for miss_att in missing_attachment:
        mssing_att_docData = {"invoice_id":all_invo_coding[miss_att]["invoice_number"],
                            "invoicetotal": all_invo_coding[miss_att]["invoicetotal"],
                            "gst": all_invo_coding[miss_att]["gst"],
                            "approved_by": all_invo_coding[miss_att]["approverName"],
                            "uploaded_date":timestmp ,
                            "approver_title":all_invo_coding[miss_att]["approver_title"],
                            "documentstatus": 4 ,  
                            "documentsubstatus": 130,
                            "created_on":timestmp,
                            "document_type":all_invo_coding[miss_att]["document_type"]}
        corp_doc = model.corp_document_tab(**mssing_att_docData)
        db.add(corp_doc)
        db.commit()
        corp_doc_id = corp_doc.corp_doc_id
        
        # update coding details: 
        # update coding details
        coding_data_insert = {'invoice_id':all_invo_coding[miss_att]['invoice_number'],
                        'corp_doc_id':corp_doc_id,
                        'coding_details':all_invo_coding[miss_att]['coding_data'],
                        'approver_name':all_invo_coding[miss_att]['approverName'],
                        
                        'tmid':all_invo_coding[miss_att]['TMID'],
                        'approver_title':all_invo_coding[miss_att]['approver_title'],
                        'invoicetotal':all_invo_coding[miss_att]['invoicetotal'],
                        'gst':all_invo_coding[miss_att]['gst'],
                        'created_on': timestmp,
                        'sender_name': all_invo_coding[miss_att]['sender'],
                        'sender_email':all_invo_coding[miss_att]['sender_email'],
                        'sent_to':all_invo_coding[miss_att]['sent_to'],
                        'sent_time':all_invo_coding[miss_att]['sent_time'],
                        'approver_email':all_invo_coding[miss_att]['approver_email'],
                        'approved_on':all_invo_coding[miss_att]['approved_on'],
                        'approval_status':all_invo_coding[miss_att]['approval_status'],
                        'document_type':all_invo_coding[miss_att]['document_type']
                        }
        corp_coding_insert = model.corp_coding_tab(**coding_data_insert)
        db.add(corp_coding_insert)
        db.commit()

    # porcessing without coding details:
    # document status & substatus: 4 , 134
    for miss_code in op_1['invoice_detail_list']:
        if miss_code[list(miss_code.keys())[0]]["InvoiceID"] in missing_coding:
            missing_code_docTab = {
                "invoice_id":miss_code[list(miss_code.keys())[0]]['InvoiceID'],
                "invoicetotal":miss_code[list(miss_code.keys())[0]]["invoicetotal"],
                "gst":miss_code[list(miss_code.keys())[0]]["GST"],
                "invo_page_count":miss_code[list(miss_code.keys())[0]]["NumberOfPages"],
                "created_on":timestmp,
                "updated_on":timestmp,
                "documentstatus":4,
                "documentsubstatus":134,
                
            }  
            corp_doc = model.corp_document_tab(**missing_code_docTab)
            db.add(corp_doc)
            db.commit()
            corp_doc_id = corp_doc.corp_doc_id
            print("corp_doc_id: ",corp_doc_id)
            
            # update document data tab:
            # insert doc data:
            corp_docdata_insert = {"invoice_id":miss_code[list(miss_code.keys())[0]]["InvoiceID"],
                        "invoice_date":miss_code[list(miss_code.keys())[0]]["InvoiceDate"],
                            "vendor_name":miss_code[list(miss_code.keys())[0]]["VendorName"],
                        "vendoraddress":miss_code[list(miss_code.keys())[0]]["VendorAddress"],
                        "customername":"",
                        "customeraddress": "",
                        "currency":miss_code[list(miss_code.keys())[0]]["Currency"],
                        
                        "invoicetotal":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["invoicetotal"]),
                        "subtotal":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["SubTotal"]),
                        
                        "corp_doc_id":corp_doc_id,
                        "bottledeposit":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]['Bottle Deposit']),
                        "shippingcharges":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["Shipping Charges"]),
                        "litterdeposit":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["Litter Deposit"]),
                        "gst":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["GST"]),
                        "pst":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["PST"]),
                        "created_on":timestmp,
                        "pst_sk":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["PST-SK"]),
                        "pst_bc":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["PST-BC"]),
                        "ecology_fee":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["Ecology Fee"]),
                        "misc":cleanAmt_all(credit_invo,miss_code[list(miss_code.keys())[0]]["misc"]),
                        }
            corp_docdata_insert_data = model.corp_docdata(**corp_docdata_insert)
            db.add(corp_docdata_insert_data)
            db.commit()
            logger.info(f"Corp document data added: {corp_docdata_insert_data}")
            corp_data_id = corp_docdata_insert_data.docdata_id
            print("corp_data_id: ",corp_data_id)

            
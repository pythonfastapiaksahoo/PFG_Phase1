from sqlalchemy.orm import aliased
import pandas as pd
from pfg_app import model
# from model import Session as db
# from model import get_db
from sqlalchemy import func
import pandas as pd
import pytz as tz
from datetime import datetime, timezone

from pfg_app.crud.CorpIntegrationCrud import processCorpInvoiceVoucher
from pfg_app.logger_module import logger
tz_region = tz.timezone("US/Pacific")


def payload_dbUpdate(doc_id,userID,db):
    timeStmp =datetime.now(tz_region) 
    return_status = {"Payload validation"  :{"status": 0,
                                                "StatusCode":0,
                                                "response": [
                                                                f"Payload data validation failed"
                                                            ],
                                                        }}
    # Aliases for tables
    CorpDocData = aliased(model.corp_docdata)
    CorpDocumentTab = aliased(model.corp_document_tab)
    CorpCodingTab = aliased(model.corp_coding_tab)

    # Query all required data in a single call
    result = (
        db.query(CorpDocData, CorpDocumentTab, CorpCodingTab)
        .join(CorpDocumentTab, CorpDocData.corp_doc_id == CorpDocumentTab.corp_doc_id)
        .join(CorpCodingTab, CorpDocData.corp_doc_id == CorpCodingTab.corp_doc_id)
        .filter(CorpDocData.corp_doc_id == doc_id)
        .all()
    )

    # Convert results to DataFrames
    df_corp_header_data = pd.DataFrame([{k: v for k, v in vars(row[0]).items() if k != "_sa_instance_state"} for row in result])
    df_corp_document = pd.DataFrame([{k: v for k, v in vars(row[1]).items() if k != "_sa_instance_state"} for row in result])
    df_corp_coding_tab = pd.DataFrame([{k: v for k, v in vars(row[2]).items() if k != "_sa_instance_state"} for row in result])

    gst_amt = list(df_corp_header_data['gst'])[0]
    VAT_APPLICABILITY = 'T' if gst_amt > 0 else 'O'

    data = {
        "DOCUMENT_ID": doc_id,
        "BUSINESS_UNIT": "NONPO",
        "INVOICE_ID": list(df_corp_header_data['invoice_id'])[0],
        "INVOICE_DT": list(df_corp_header_data['invoice_date'])[0],
        "VENDOR_SETID": "GLOBL",
        "VENDOR_ID": list(df_corp_document['vendor_code'])[0],
        "ORIGIN": "IDP",
        "GROSS_AMT": list(df_corp_header_data['invoicetotal'])[0],
        "TXN_CURRENCY_CD": list(df_corp_header_data['currency'])[0],
        "VAT_ENTRD_AMT": gst_amt,
        "OPRID": list(df_corp_coding_tab['tmid'])[0],
        "MERCHANDISE_AMT": list(df_corp_header_data['subtotal'])[0],
        "SHIPTO_ID": "8000",
        "VCHR_DIST_STG": list(df_corp_coding_tab['coding_details'])[0],
        "INVOICE_FILE_PATH": list(df_corp_document['invo_filepath'])[0],
        "EMAIL_PATH": str(list(df_corp_document['email_filepath_pdf'])[0]),
        "VAT_APPLICABILITY": VAT_APPLICABILITY,
        "VCHR_SRC":"CRP"
    }
    voucher_status = {}
    Failed_Code = {}
    status_ck = 1
    for i in data:
        if data[i] is None:
            voucher_status[i] = {'status':0,'StatusCode':0,
                                'status_msg':str(i)+" missing." }
            status_ck = status_ck * 0
            Failed_Code[i] = {'status':0,'StatusCode':0,
                                'status_msg':str(i)+" missing." }
        else:
            if i=='VCHR_DIST_STG':
                missing_cdFd = []
                for cd in data['VCHR_DIST_STG']:
                    set1 = set(data['VCHR_DIST_STG'][cd].keys())
                    # set2 = {'SL', 'activity', 'amount', 'dept', 'project', 'store',}
                    set2 = {'SL', 'activity', 'amount', 'dept', 'project', 'store','account'}

                    difference =  set2 - set1  # or set1.difference(set2)
                    if len(difference)>0:
                        missing_cdFd.append(difference)
                        voucher_status['Coding validation failed'] = {'status':0,'StatusCode':0,
                                    'status_msg':"Line"+str(cd)+": "+str(difference)+" missing." }
                        status_ck = status_ck * 0
                        Failed_Code['Coding validation failed'] = {'status':0,'StatusCode':0,
                                    'status_msg':"Line"+str(cd)+": "+str(difference)+" missing." }
                    else:
                        voucher_status[i] = {'status':1,'StatusCode':1,
                                    'status_msg':"success" }
                    
            else:
                voucher_status[i] = {'status':1,'StatusCode':1,
                                'status_msg':"success" }
    try:
        db.query(model.corp_coding_tab).filter( model.corp_coding_tab.corp_doc_id == doc_id
        ).update(
            {
                model.corp_coding_tab.voucher_status: voucher_status,  # noqa: E501
                

            }

        )
        db.commit()
        if status_ck == 1:
            return_status["Payload validation"] = {"status": 1,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    f"Success"
                                                                ],
                                                            }
    except Exception as e:
        logger.info(f"Error in updating coding tab: {e}")
        return_status["Payload validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    f"Error: {e}"
                                                                ],
                                                            }
        
        db.rollback()

    if status_ck == 1:

        # Check if a record exists
        existing_record = db.query(model.CorpVoucherData).filter_by(DOCUMENT_ID=doc_id).first()

        if existing_record:
            # Update existing record
            db.query(model.CorpVoucherData).filter_by(DOCUMENT_ID=doc_id).update(data)
        else:
            # Insert new record
            new_record = model.CorpVoucherData(**data)
            db.add(new_record)

        db.commit()
        docStatus = 2
        docSubStatus = 31
        db.query(model.corp_document_tab).filter( model.corp_document_tab.corp_doc_id == doc_id
            ).update(
                {
                    model.corp_document_tab.documentstatus: docStatus,  # noqa: E501
                    model.corp_document_tab.documentsubstatus: docSubStatus,  # noqa: E501
                    model.corp_document_tab.last_updated_by: userID,
                    # model.corp_document_tab.vendor_id: vendorID,
                    model.corp_document_tab.updated_on: timeStmp,

                }
            )
        db.commit()
        try:
            responsedata = processCorpInvoiceVoucher(doc_id, db)
            return_status["PeopleSoft response"] = {"status": 1,
                                                "StatusCode":0,
                                                "response": [
                                                                f" PeopleSoft:{str(responsedata)}"
                                                            ],
                                                        }
            
        except Exception as e:
            logger.error(f"Error in processing corp invoice voucher: {e}")
            return_status["PeopleSoft Failure"] = {"status": 0,
                                                "StatusCode":0,
                                                "response": [
                                                                f"Error: {e}"
                                                            ],
                                                        }
        return return_status
    else:
       
        docStatus = 4
        docSubStatus = 36
        db.query(model.corp_document_tab).filter( model.corp_document_tab.corp_doc_id == doc_id
            ).update(
                {
                    model.corp_document_tab.documentstatus: docStatus,  # noqa: E501
                    model.corp_document_tab.documentsubstatus: docSubStatus,  # noqa: E501
                    model.corp_document_tab.last_updated_by: userID,
                    # model.corp_document_tab.vendor_id: vendorID,
                    model.corp_document_tab.updated_on: timeStmp,

                }
            )
        db.commit()
    
        return Failed_Code
#corp_payloadValidations

import traceback
from sqlalchemy.orm import aliased
import pandas as pd
from pfg_app import model
# from model import Session as db
# from model import get_db
from sqlalchemy import func
import pandas as pd
import pytz as tz
from datetime import datetime, timezone
from sqlalchemy.sql import case
from pfg_app.crud.CorpIntegrationCrud import corp_update_docHistory, processCorpInvoiceVoucher
from pfg_app.logger_module import logger
from pfg_app.schemas.pfgtriggerSchema import InvoiceVoucherSchema
tz_region = tz.timezone("US/Pacific")


# def validate_voucher_distribution(db, vchr_dist_stg):
#     """
#     Validates each field (except amount) from VCHR_DIST_STG against the respective database tables.
#     Returns status_check (1 if valid, 0 if any issue), voucher_status, and Failed_Code dictionaries.
#     """
#     status_ck = 1
#     voucher_status = {}
#     Failed_Code = {}

#     for line, details in vchr_dist_stg.items():
#         store = details.get("store")
#         dept = details.get("dept")
#         account = details.get("account")
#         sl = details.get("SL")
#         project = details.get("project")
#         activity = details.get("activity")

#         # Validate Store
#         if store:
#             store_exists = db.query(model.PFGStore).filter_by(STORE=store).first()
#             if not store_exists:
#                 voucher_status[f"store_{line}"] = {"status": 0, "StatusCode": 0, "status_msg": f"Line {line}: Store {store} not found"}
#                 Failed_Code[f"store_{line}"] = {"status": 0, "StatusCode": 0, "status_msg": f"Line {line}: Store {store} not found"}
#                 status_ck = 0

#         # Validate Department
#         if dept:
#             dept_exists = db.query(model.PFGDepartment).filter_by(DEPTID=dept).first()
#             if not dept_exists:
#                 voucher_status[f"dept_{line}"] = {"status": 0, "StatusCode": 0, "status_msg": f"Line {line}: Department {dept} not found"}
#                 Failed_Code[f"dept_{line}"] = {"status": 0, "StatusCode": 0, "status_msg": f"Line {line}: Department {dept} not found"}
#                 status_ck = 0

#         # Validate Account
#         if account:
#             account_exists = db.query(model.PFGAccount).filter_by(ACCOUNT=account).first()
#             if not account_exists:
#                 voucher_status[f"account_{line}"] = {"status": 0, "StatusCode": 0, "status_msg": f"Line {line}: Account {account} not found"}
#                 Failed_Code[f"account_{line}"] = {"status": 0, "StatusCode": 0, "status_msg": f"Line {line}: Account {account} not found"}
#                 status_ck = 0

#         # Validate Strategic Ledger (SL)
#         if sl:
#             sl_exists = db.query(model.PFGStrategicLedger).filter_by(CHARTFIELD1=sl).first()
#             if not sl_exists:
#                 voucher_status[f"SL_{line}"] = {"status": 0, "StatusCode": 0, "status_msg": f"Line {line}: Strategic Ledger {sl} not found"}
#                 Failed_Code[f"SL_{line}"] = {"status": 0, "StatusCode": 0, "status_msg": f"Line {line}: Strategic Ledger {sl} not found"}
#                 status_ck = 0

#         # Validate Project
#         if project:
#             project_exists = db.query(model.PFGProject).filter_by(PROJECT_ID=project).first()
#             if not project_exists:
#                 voucher_status[f"project_{line}"] = {"status": 0, "StatusCode": 0, "status_msg": f"Line {line}: Project {project} not found"}
#                 Failed_Code[f"project_{line}"] = {"status": 0, "StatusCode": 0, "status_msg": f"Line {line}: Project {project} not found"}
#                 status_ck = 0

#         # Validate Project Activity
#         if activity and project:
#             activity_exists = db.query(model.PFGProjectActivity).filter_by(PROJECT_ID=project, ACTIVITY_ID=activity).first()
#             if not activity_exists:
#                 voucher_status[f"activity_{line}"] = {"status": 0, "StatusCode": 0, "status_msg": f"Line {line}: Project Activity {activity} not found"}
#                 Failed_Code[f"activity_{line}"] = {"status": 0, "StatusCode": 0, "status_msg": f"Line {line}: Project Activity {activity} not found"}
#                 status_ck = 0

#     return status_ck, voucher_status, Failed_Code

    
    
def validate_voucher_distribution(db, vchr_dist_stg):   
    # status_ck = 1
    # voucher_status = {}
    # coding_Failed_Code = {}
    store_msg = []
    dept_msg = []
    acc_msg = []
    sl_msg = []
    prj_msg = []
    act_msg = []
    misssing_PA = []
    
    for line, details in vchr_dist_stg.items():
        prj_fd = 0
        store = details.get("store")
        dept = details.get("dept")
        account = details.get("account")
        sl = details.get("SL")
        project = details.get("project")
        activity = details.get("activity")
        print(store,dept,account)
        # Validate Store
        if store:
            store_exists = db.query(model.PFGStore).filter_by(STORE=store).first()
            if store_exists:
                print("valid store")
            else:
                store_msg.append(f"Line:{line} -{store}")

        # Validate Department
        if dept:
           
            dept_exists = db.query(model.PFGDepartment).filter_by(DEPTID=dept).first()
            if dept_exists:
                print("valid dept")
            else:
                dept_msg.append(f"Line:{line} -{dept}") 
        # Validate Account
        if account:
            account_exists = db.query(model.PFGAccount).filter_by(ACCOUNT=account).first()
            if account_exists:
                print("valid account")
            else:
              
                acc_msg.append(f"Line:{line} -{account}") 

        # Validate Strategic Ledger (SL)
        if sl:
            sl_exists = db.query(model.PFGStrategicLedger).filter_by(CHARTFIELD1=sl).first()
            if sl_exists:
                print("valid SL")
            else:
                sl_msg.append(f"Line:{line} -{sl}") 

        # Validate Project
        if project:
            prj_fd = 1
            project_exists = db.query(model.PFGProject).filter_by(PROJECT_ID=project).first()
            if project_exists:
                print("Valid project")
                
            else:
               
                prj_msg.append(f"Line:{line} -{project}") 

        # Validate Project Activity
        if prj_fd == 1:
            if activity:
                print("activity exists")
            else:
                print("missing activity")
                misssing_PA.append(f"Line:{line} - Activity missing")
        else:
            if activity:
                print("misssing project")
                misssing_PA.append(f"Line:{line} - Project missing")

        if activity and project:
            activity_exists = db.query(model.PFGProjectActivity).filter_by(PROJECT_ID=project, ACTIVITY_ID=activity).first()
            if activity_exists:
                print("activity_exists valid")
            else:
                act_msg.append(f"Line:{line} -{project}/{activity}") 

    val_status_msg = ""   
    invl_status_cd = 1
    if len(store_msg)>0:
        val_status_msg = f"Invalid store:{store_msg}"
        invl_status_cd = invl_status_cd * 0

    if len(dept_msg)>0:
        if val_status_msg=="" :
            val_status_msg = f"Invalid department:{dept_msg}"
        else:
            val_status_msg = f"{val_status_msg} | Invalid department: {dept_msg}"
        invl_status_cd = invl_status_cd * 0

    if len(acc_msg)>0:
        if val_status_msg=="" :
            val_status_msg = f"Invalid account:{acc_msg}"
        else:
            val_status_msg = f"{val_status_msg} | Invalid account::{acc_msg}"
        invl_status_cd = invl_status_cd * 0

    if len(sl_msg)>0:
        if val_status_msg=="" :
            val_status_msg = f"Invalid SL:{sl_msg}"
        else:
            val_status_msg = f"{val_status_msg} | Invalid SL:{sl_msg}"
        invl_status_cd = invl_status_cd * 0     

    if len(prj_msg)>0:
        if val_status_msg=="" :
            val_status_msg = f"Invalid project:{prj_msg}"
        else:
            val_status_msg = f"{val_status_msg} | Invalid project:{prj_msg}"
        invl_status_cd = invl_status_cd * 0
    print("act_msg:",act_msg)
    if len(act_msg)>0:
        if val_status_msg=="" :
            val_status_msg = f"Proj & Activity mismatch(Proj/Act):{act_msg}"
        else:
            val_status_msg = f"{val_status_msg} | Proj & Activity mismatch(Proj/Act):{act_msg}"
        invl_status_cd = invl_status_cd * 0
    if len(misssing_PA)>0:
        if val_status_msg=="" :
            val_status_msg = f"Project/Activity missing:{misssing_PA}"
        else:
            val_status_msg = f"{val_status_msg} | Project/Activity missing:{misssing_PA}"
        invl_status_cd = invl_status_cd * 0
    logger.info(f"return fomr validate_voucher- invl_status_cd{invl_status_cd}, val_status_msg: {val_status_msg}")
    return invl_status_cd,val_status_msg


def payload_dbUpdate(doc_id,userID,db):
    timeStmp =datetime.now(tz_region) 
    SentToPeopleSoft = 0
    dmsg = ""
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

    if employee_id==None:
        return_status["Payload validation"] = {"status": 0,
                                                    "StatusCode":0,
                                                    "response": [
                                                                    f"TM ID missing."
                                                                ],
                                                            }
        logger.info(f"return line 275: {return_status}")
        return return_status

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
        "OPRID": employee_id,
        "MERCHANDISE_AMT": list(df_corp_header_data['subtotal'])[0],
        "SHIPTO_ID": "8000",
        "VCHR_DIST_STG": list(df_corp_coding_tab['coding_details'])[0],
        "INVOICE_FILE_PATH": list(df_corp_document['invo_filepath'])[0],
        "EMAIL_PATH": str(list(df_corp_document['email_filepath_pdf'])[0]),
        "VAT_APPLICABILITY": VAT_APPLICABILITY,
        "VCHR_SRC":"CRP"
    }
    logger.info(f"data: {data}")
    voucher_status = {}
    Failed_Code = {}
    status_ck = 1
    try:
        invl_status_cd,val_status_msg = validate_voucher_distribution(db, list(df_corp_coding_tab['coding_details'])[0])
        if invl_status_cd == 1:
            voucher_status['Voucher distribution validation'] = {'status':1,'StatusCode':0,
                                'status_msg':val_status_msg }
            status_ck = status_ck * 1
        else:
            Failed_Code['Voucher distribution validation'] = {'status':0,'StatusCode':0,
                                'status_msg':val_status_msg }
            status_ck = status_ck * 0
    except Exception as e:
        voucher_status['Failed data validation'] = {'status':0,'StatusCode':0,
                                'status_msg':str(e) }
        status_ck = status_ck * 0
        Failed_Code['Failed data validation'] = {'status':0,'StatusCode':0,
                                'status_msg':str(e) }
        
    if invl_status_cd==1:
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
                            voucher_status[i] = {'status':1,'StatusCode':0,
                                        'status_msg':"success" }
                        # else:
                        #     # Call the validation function when all required fields exist
                        #     status_ck, validation_status, validation_errors = validate_voucher_distribution(
                        #         db, data['VCHR_DIST_STG']
                        #     )
                            
                        #     # Update voucher_status and Failed_Code with validation results
                        #     voucher_status.update(validation_status)
                        #     Failed_Code.update(validation_errors)
                        
                else:
                    voucher_status[i] = {'status':1,'StatusCode':0,
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
            
        # db.rollback()
    else:
        status_ck = 0
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
            # responsedata = processCorpInvoiceVoucher(doc_id, db)
            # send to ppl soft:
            
            try:
                resp = processCorpInvoiceVoucher(doc_id, db)
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
                                logger.info(f"RespCodeInt {doc_id}: {RespCodeInt}")
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
                                
                                elif RespCodeInt == 410:
                                    dmsg = (
                                        InvoiceVoucherSchema.FILE_SIZE_EXCEEDED  # noqa: E501
                                    )
                                    docStatus = 4
                                    docSubStatus = 160
                                
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
                            logger.info(f"error docID: {doc_id} - No Http Response found")
                            dmsg = (
                                InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                            )
                            docStatus = 21
                            docSubStatus = 112
                            
                    else:
                        logger.info(f"error docID: {doc_id} - No data found ppl dft response")
                        dmsg = (
                            InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                        )
                        docStatus = 21
                        docSubStatus = 112
                except Exception as err:
                    logger.info(f"error docID: {doc_id} - No response")
                    logger.debug(
                        f"PopleSoftResponseError: {traceback.format_exc()}"  # noqa: E501
                    )
                    dmsg = InvoiceVoucherSchema.FAILURE_COMMON.format_message(  # noqa: E501
                        err
                    )
                    docStatus = 21
                    docSubStatus = 112

                try:
                    logger.info(f"Updating the document status for doc_id:{doc_id}")
                    db.query(model.corp_document_tab).filter(
                        model.corp_document_tab.corp_doc_id == doc_id
                    ).update(
                        {
                            model.corp_document_tab.documentstatus: docStatus,
                            model.corp_document_tab.documentsubstatus: docSubStatus,
                            model.corp_document_tab.retry_count: case(
                                (model.corp_document_tab.retry_count.is_(None), 1),  # If NULL, set to 1
                                else_=model.corp_document_tab.retry_count + 1        # Otherwise, increment
                            ) if docStatus == 21 and docSubStatus in [152, 143] else model.corp_document_tab.retry_count
                        }
                    )
                    db.commit()
                    logger.info(f"Updated docStatus {doc_id}: {docStatus}")
                except Exception:
                    logger.error(traceback.format_exc())
                try:

                    corp_update_docHistory(
                        doc_id, userID, docStatus, dmsg, db, docSubStatus
                    )

                except Exception:
                    logger.error(traceback.format_exc())
            except Exception as e:
                logger.info(f"error docID: {doc_id} - No response - failed")
                logger.debug(traceback.format_exc())
                dmsg = InvoiceVoucherSchema.FAILURE_COMMON.format_message(  # noqa: E501
                    e
                )
                docStatus = 21
                docSubStatus = 112

                # docStatusSync["Sent to PeopleSoft"] = {
                #     "status": SentToPeopleSoft,
                #     "StatusCode":0,
                #     "response": [dmsg],
                # }

                try:
                    db.query(model.corp_document_tab).filter(
                        model.corp_document_tab.corp_doc_id == doc_id
                    ).update(
                        {
                            model.corp_document_tab.documentstatus: docStatus,
                            model.corp_document_tab.documentsubstatus: docSubStatus,
                        }
                    )
                    db.commit()
                except Exception:
                    logger.debug(traceback.format_exc())

                try:
                    documentstatus = 21
                    corp_update_docHistory(
                        doc_id,
                        userID,
                        documentstatus,
                        dmsg,
                        db,docSubStatus,  # noqa: E501
                    )
                except Exception:
                    logger.debug(f"{traceback.format_exc()}")
            
            if docStatus == 4:
                return_status["Payload Data Error"] = { 
                    "status": SentToPeopleSoft, 
                    "StatusCode": 0,
                    "response": [dmsg],
                }
            else:
                return_status["Sent to PeopleSoft"] = {
                    "status": SentToPeopleSoft,
                    "StatusCode": 0,
                    "response": [dmsg],
    }

            # return_status["PeopleSoft response"] = {"status": 1,
            #                                     "StatusCode":0,
            #                                     "response": [
            #                                                     f" PeopleSoft:{str(resp)}"
            #                                                 ],
            #                                             }
            
        except Exception as e:
            logger.error(f"Error in processing corp invoice voucher: {e}")
            return_status["PeopleSoft Failure"] = {"status": 0,
                                                "StatusCode":0,
                                                "response": [
                                                                f"Error: {e}"
                                                            ],
                                                        }
        logger.info(f"return line 617: {return_status}")
        return return_status
    else:

        docStatus = 4
        docSubStatus = 4
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
        logger.info(f"return line 634: {Failed_Code}")
        return Failed_Code
from pfg_app import model
from pfg_app.FROps.corp_payloadValidation import payload_dbUpdate
from pfg_app.FROps.customCall import date_cnv
from pfg_app.session.session import SQLALCHEMY_DATABASE_URL, get_db
from sqlalchemy import func
import pandas as pd
from pfg_app.crud.CorpIntegrationCrud import corp_update_docHistory
from pfg_app.logger_module import logger
import traceback
# doc_id = 144
# userID = 1


from datetime import datetime

def check_date_format(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False
def validate_corpdoc(doc_id,userID,db):
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
    logger.info(f"doc_id: {doc_id}, vendor_id: {vendor_id}, document_type: {document_type}, invoice_id: {invoice_id}")
    if docStatus in (26,25):

        if vendor_id is not None:
            docStatus = 4
            docSubStatus = 11
            db.query(model.corp_document_tab).filter( model.corp_document_tab.corp_doc_id == doc_id
                ).update(
                    {
                        model.corp_document_tab.documentstatus: docStatus,  # noqa: E501
                        model.corp_document_tab.documentsubstatus: docSubStatus,  # noqa: E501
                        model.corp_document_tab.last_updated_by: userID,
                        # model.corp_document_tab.vendor_id: vendorID,

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
        
    if docStatus in (32,2,4):
        if vendor_id is None:
            docStatus = 26
            docSubStatus = 107
            db.query(model.corp_document_tab).filter( model.corp_document_tab.corp_doc_id == doc_id
                ).update(
                    {
                        model.corp_document_tab.documentstatus: docStatus,  # noqa: E501
                        model.corp_document_tab.documentsubstatus: docSubStatus,  # noqa: E501
                        model.corp_document_tab.last_updated_by: userID,
                        # model.corp_document_tab.vendor_id: vendorID,

                    }
                )
            db.commit()

        # duplicate check:
        if docSubStatus == 134:
                print("Coding - No Coding Lines Found")
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
            logger.info(f"return corp validations(ln 70): {return_status}")
            return return_status
        
        
        else:
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
                        # model.corp_document_tab.vendor_id: vendorID,

                    }
                )
                db.commit()
                logger.info(f"return corp validations(ln 111): {return_status}")
                return return_status
            else:
                try:
                # invoice total validation:
                    logger.info(f"ready for validations-docID: {doc_id}")
                    # Query corp_coding_tab:

                    docStatus = 4
                    docSubStatus = 11
                    db.query(model.corp_document_tab).filter( model.corp_document_tab.corp_doc_id == doc_id
                        ).update(
                            {
                                model.corp_document_tab.documentstatus: docStatus,  # noqa: E501
                                model.corp_document_tab.documentsubstatus: docSubStatus,  # noqa: E501
                                model.corp_document_tab.last_updated_by: userID,
                                # model.corp_document_tab.vendor_id: vendorID,

                            }
                        )
                    db.commit()
                    # return_status["Status overview"] = {"status": 0,
                    #                 "StatusCode":0,
                    #                     "response": [
                    #                                 "Validation pending"
                    #                             ],
                    #                         }
                    
                    # return return_status
                    


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
                    corp_metadata_qry = (
                        db.query(model.corp_metadata)
                        .filter(model.corp_metadata.vendorid == vendor_id)
                        .all()
                    )
                    

                    df_corp_metadata = pd.DataFrame([row.__dict__ for row in corp_metadata_qry])
                    date_format = list(df_corp_metadata['dateformat'])[0]
                    
                    
                    # Check for mandatory fields:
                    mand_invoTotal = list(df_corp_docdata['invoicetotal'])[0]
                    mand_gst = list(df_corp_docdata['gst'])[0]
                    mand_invDate = list(df_corp_docdata['invoice_date'])[0]
                    mand_subTotal = list(df_corp_docdata['subtotal'])[0]
                    mand_document_type = list(df_corp_docdata['document_type'])[0]

                    #date validation:
                    try:
                        if check_date_format(mand_invDate) == False:
                            req_date, date_status = date_cnv(mand_invDate, date_format)
                            if date_status == 1:
                                invDate_msg = "Valid Date Format"
                                invDate_status = 1
                                #update date to table:
                                db.query(model.corp_document_tab, model.corp_docdata).filter(
                                    model.corp_document_tab.corp_doc_id == doc_id,
                                    model.corp_docdata.corp_doc_id == doc_id
                                ).update(
                                    {
                                        model.corp_document_tab.invoice_date: req_date,
                                        model.corp_docdata.invoice_date: req_date
                                    }
                                )
                                db.commit()

                            else:
                                invDate_msg = "Invalid Date Format"
                                invDate_status = 0
                        else:
                            invDate_msg = "Valid Date Format"
                            invDate_status = 1
                    except Exception as e:
                        logger.error(f"Error in validate_corpdoc: {e}")
                        logger.info(traceback.format_exc())
                        invDate_msg = "Please review Date format"
                        invDate_status = 0      

                    
                    # total validation:
                    try:
                        logger.info(f"Validating invoice total- invoicetotal:{mand_invoTotal}, subtotal:{mand_subTotal}, gst:{mand_gst}")
                        if float(mand_invoTotal) - (float(mand_subTotal)+float(mand_gst))<0.9:
                            invoTotal_status = 1
                            gst_status = 1
                            subTotal_status = 1
                            subTotal_msg = "Subtotal match success"
                            gst_msg = "GST match success"
                            invoTotal_msg = "Invoice total match success"
                        else:
                            invoTotal_status = 0
                            invoTotal_msg = "Invoice total mismatch"
                    except Exception as e:
                        logger.error(f"Error in validate_corpdoc: {e}")
                        logger.info(traceback.format_exc())
                        invoTotal_status = 0
                        invoTotal_msg = "Please review Total"

                    
                    # document type validation:

                    try:
                        logger.info(f"Validating document type- document_type:{mand_document_type}")
                        if mand_document_type.lower() in ['invoice','credit']:
                            document_type_status = 1
                            document_type_msg = "Document type validation success"
                        else:
                            document_type_status = 0
                            document_type_msg = "Document type mismatch"
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
                    
                    else:
                        line_sum = 0
                        amt_threshold = 250000
                        cod_invoTotal =  df_corp_coding['invoicetotal']
                        cod_gst = df_corp_coding['gst']
                        
                        invoTotal_15 = (cod_invoTotal * 0.15)

                        pdf_invoTotal = list(df_corp_docdata['invoicetotal'])[0]
                        lt = []
                        for ln_amt in (list(df_corp_coding['coding_details'])[0]):
                            lt.append(list(df_corp_coding['coding_details'])[0][ln_amt]['amount'])
                            line_sum = line_sum + list(df_corp_coding['coding_details'])[0][ln_amt]['amount']

                        if abs(float(cod_invoTotal.values[0])- line_sum )> 0.09:
                            docStatus = 4
                            substatus = 136
                            documentdesc = "Coding - Line total mismatch"
                            return_status["Coding Line validation"] = {"status": 0,
                                                "StatusCode":0,
                                                "response": [
                                                                "Coding - Line total mismatch"
                                                            ],
                                                        }
                            # return return_status
                        else:
                            #line total match success
                            if abs(float(cod_invoTotal.values[0]) - pdf_invoTotal) >0.09:
                                docStatus = 4
                                substatus = 131
                                documentdesc = "Invoice - Total mismatch with coding total"
                            else:
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
                                    # return return_status
                                #total match pass:
                                elif pdf_invoTotal == 0:
                                    docStatus = 4
                                    substatus = 139
                                    documentdesc = "Zero $ invoice approval required"
                                    # Zero $ invoice - need approval'
                                    return_status["Coding validation"] = {"status": 0,
                                                "StatusCode":0,
                                                "response": [
                                                                f"Coding - GST exceeding 15% of invoice total"
                                                            ],
                                                        }
                                    # return return_status
                                    # print("Zero $ invoice approved")
                                elif pdf_invoTotal > amt_threshold:
                                    # need approval
                                    docStatus = 4
                                    substatus = 140
                                    documentdesc =  "Approval needed: Invoice ≥ threshold"
                                    print("need approval")
                                    return_status["Approval needed"] = {"status": 0,
                                                "StatusCode":0,
                                                "response": [
                                                                f"User approval required if invoice total meets threshold"
                                                            ],
                                                        }
                                    # return return_status
                            
                                else:
                                    
                                    approver_title =list(df_corp_document['approver_title'])[0] 
                                    if (('sr' in approver_title.lower()) or ('senior' in approver_title.lower())) or ('vp' in approver_title.lower()) or (('vice' in approver_title.lower()) and ('president' in approver_title.lower())):
                                        approvrd_ck = 1
                                    elif pdf_invoTotal <= 25000:
                                        if "assistant" in approver_title.lower():
                                            approvrd_ck = 0
                                        elif ("manager" in approver_title.lower()) or ("director" in approver_title.lower()) or ("Sr. Vice President" in approver_title):
                                            approvrd_ck = 1
                                    elif pdf_invoTotal<= 1000000:
                                        if ("director" in approver_title.lower()):
                                            approvrd_ck = 1
                                    if approvrd_ck==0:
                                        docStatus = 24
                                        substatus = 70
                                        documentdesc = "Invoice - Not Approved"
                                        return_status["Approval needed"] = {"status": 0,
                                                "StatusCode":0,
                                                "response": [
                                                                f"Invoice - Not Approved"
                                                            ],
                                                        }
                                        # return return_status
                                    elif approvrd_ck ==1:
                                    
                                        if list(df_corp_coding['approval_status'])[0].lower() == "approved":
                                            docStatus = 2
                                            substatus = 5
                                            documentdesc = "Ready for ERP"
                                            
                                            payload_dbUpdate(doc_id,userID,db)
                                            # return return_status
                                            return_status["Approval needed"] = {"status": 0,
                                                "StatusCode":0,
                                                "response": [
                                                                f"Payload data ready for PeopleSoft"
                                                            ],
                                                        }
                                        else: 
                                            docStatus = 24
                                            substatus = 137
                                            documentdesc = "Pending Approval"
                                            return_status["Approval needed"] = {"status": 0,
                                                "StatusCode":0,
                                                "response": [
                                                                f"Invoice - Pending Approval"
                                                            ],
                                                        }
                                            return return_status
                except Exception as e:
                    logger.error(f"Error in validate_corpdoc: {e}")
                    logger.info(traceback.format_exc())
    try:
        doc_updates_status = {'invoicetotal':{'status':invoTotal_status,'status_message':invoTotal_msg},
                'gst':{'status':gst_status,'status_message':gst_msg},
                'invoice_date':{'status':invDate_status,'status_message':invDate_msg},
                'subtotal':{'status':subTotal_status,'status_message':subTotal_msg},
                'document_type':{'status':document_type_status,'status_message':document_type_msg}}
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
        logger.error(f"Error in validate_corpdoc: {e}")
        logger.info(traceback.format_exc())

    try:
        db.query(model.corp_document_tab).filter( model.corp_document_tab.corp_doc_id == doc_id
            ).update(
                {
                    model.corp_document_tab.documentstatus: docStatus,  # noqa: E501
                    model.corp_document_tab.documentsubstatus: docSubStatus,  # noqa: E501
                    model.corp_document_tab.last_updated_by: userID,
                    # model.corp_document_tab.vendor_id: vendorID,

                }
            )
        db.commit()
    except Exception as e:
        logger.error(f"Error in validate_corpdoc: {e}")
        logger.info(traceback.format_exc())

    logger.info(f"return corp validations(ln 250): {return_status}")

    
    return return_status
                            

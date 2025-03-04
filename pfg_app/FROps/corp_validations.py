from pfg_app import model
from pfg_app.session.session import SQLALCHEMY_DATABASE_URL, get_db
from sqlalchemy import func
import pandas as pd
from pfg_app.crud.CorpIntegrationCrud import corp_update_docHistory
from pfg_app.logger_module import logger
import traceback
# doc_id = 144
# userID = 1
def validate_corpdoc(doc_id,userID,db):
    return_status = {}
    

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
        
    if docStatus in (32,4):

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
                    return_status["Status overview"] = {"status": 0,
                                    "StatusCode":0,
                                        "response": [
                                                    "Validation pending"
                                                ],
                                            }
                    
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
                    if (document_type is not None):
                        if document_type !="":
                            if str(document_type).lower() in ('invoice','credit'):
                                print(document_type)


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
                        docStatu = 4
                        substatus = 136
                        documentdesc = "Coding - Line total mismatch"
                        return_status["Coding validation"] = {"status": 0,
                                            "StatusCode":0,
                                            "response": [
                                                            "Coding - Line total mismatch"
                                                        ],
                                                    }
                    else:
                        #line total match success
                        if abs(float(cod_invoTotal.values[0]) - pdf_invoTotal) >0.09:
                            docStatu = 4
                            substatus = 131
                            documentdesc = "Invoice - Total mismatch with coding total"
                        else:
                            if (cod_gst > invoTotal_15).any():
                                docStatu = 4
                                substatus = 138
                                documentdesc = "Coding -GST exceeding 15%"
                                return_status["Coding validation"] = {"status": 0,
                                            "StatusCode":0,
                                            "response": [
                                                            f"Coding - GST exceeding 15% of invoice total"
                                                        ],
                                                    }
                            #total match pass:
                            elif pdf_invoTotal == 0:
                                docStatu = 4
                                substatus = 139
                                documentdesc = "Zero $ invoice approval required"
                                # Zero $ invoice - need approval'
                                return_status["Coding validation"] = {"status": 0,
                                            "StatusCode":0,
                                            "response": [
                                                            f"Coding - GST exceeding 15% of invoice total"
                                                        ],
                                                    }
                                print("Zero $ invoice approved")
                            elif pdf_invoTotal > amt_threshold:
                                # need approval
                                docStatu = 4
                                substatus = 140
                                documentdesc =  "Approval needed: Invoice ≥ threshold"
                                print("need approval")
                                return_status["Approval needed"] = {"status": 0,
                                            "StatusCode":0,
                                            "response": [
                                                            f"User approval required if invoice total meets threshold"
                                                        ],
                                                    }
                        
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
                                    docStatu = 24
                                    substatus = 70
                                    documentdesc = "Invoice - Not Approved"
                                    return_status["Approval needed"] = {"status": 0,
                                            "StatusCode":0,
                                            "response": [
                                                            f"Invoice - Not Approved"
                                                        ],
                                                    }
                                elif approvrd_ck ==1:
                                
                                    if list(df_corp_coding['approval_status'])[0].lower() == "approved":
                                        docStatu = 2
                                        substatus = 5
                                        documentdesc = "Ready for ERP"
                                        return_status["Approval needed"] = {"status": 0,
                                            "StatusCode":0,
                                            "response": [
                                                            f"Invoice - ready for PeopleSoft"
                                                        ],
                                                    }
                                    else: 
                                        docStatu = 24
                                        substatus = 137
                                        documentdesc = "Pending Approval"
                                        return_status["Approval needed"] = {"status": 0,
                                            "StatusCode":0,
                                            "response": [
                                                            f"Invoice - Pending Approval"
                                                        ],
                                                    }
                except Exception as e:
                    logger.error(f"Error in validate_corpdoc: {e}")
                    logger.info(traceback.format_exc())

    logger.info(f"return corp validations(ln 250): {return_status}")
    
    return return_status
                            

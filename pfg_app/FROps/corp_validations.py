from pfg_app import model
from pfg_app.session.session import SQLALCHEMY_DATABASE_URL, get_db
from sqlalchemy import func
import pandas as pd
from pfg_app.crud.CorpIntegrationCrud import corp_update_docHistory


# doc_id = 144
# userID = 1
def validate_corpdoc(doc_id,userID,db):

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


    if docStatus in (26,25):
        print("Vendor mapping required")
        
    elif docStatus in (10,):
        print("Document rejected")
        
    elif docStatus in (32,4):
        # duplicate check:
        dupCk_document_data = (
        db.query(model.corp_document_tab)
        .filter(
            model.corp_document_tab.corp_doc_id != doc_id,
            model.corp_document_tab.vendor_id == vendor_id,
            model.corp_document_tab.documentstatus != 10
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
            corp_update_docHistory(doc_id, userID, docStatus, substatus, documentdesc, db)
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
        else:
            # invoice total validation:
            print("ready for validations")
            # Query corp_coding_tab:
            if docSubStatus == 134:
                print("Coding - No Coding Lines Found")
                
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

            line_sum = 0
            amt_threshold = 500000
            cod_invoTotal =  df_corp_coding['invoicetotal']
            cod_gst = df_corp_coding['gst']
            

            pdf_invoTotal = list(df_corp_docdata['invoicetotal'])[0]
            lt = []
            for ln_amt in (list(df_corp_coding['coding_details'])[0]):
                lt.append(list(df_corp_coding['coding_details'])[0][ln_amt]['amount'])
                line_sum = line_sum + list(df_corp_coding['coding_details'])[0][ln_amt]['amount']

            if abs(cod_invoTotal- line_sum )> 0.09:
                docStatu = 4
                substatus = 136
                documentdesc = "Coding - Line total mismatch"
            else:
                #line total match success
                if abs(cod_invoTotal - pdf_invoTotal) >0.09:
                    docStatu = 4
                    substatus = 131
                    documentdesc = "Invoice - Total mismatch with coding total"
                else:
                    #total match pass:
                    if pdf_invoTotal == 0:
                        # Zero $ invoice - need approval
                        print("Zero $ invoice approved")
                    # elif pdf_invoTotal >amt_threshold:
                    #     # need approval
                    #     print("need approval")
                
                    else:
                        
                        approver_title =list(df_corp_document['approver_title'])[0] 
                        if (('sr' in approver_title.lower()) or ('senior' in approver_title.lower())) or ('vp' in approver_title.lower()) or (('vice' in approver_title.lower()) and ('president' in approver_title.lower())):
                            approvrd_ck = 1
                        elif pdf_invoTotal <= 25000:
                            if ("manager" in approver_title.lower()) or ("director" in approver_title.lower()) or ("Sr. Vice President" in approver_title):
                                approvrd_ck = 1
                        elif pdf_invoTotal<= 1000000:
                            if ("director" in approver_title.lower()):
                                approvrd_ck = 1
                        if approvrd_ck==0:
                            docStatu = 24
                            substatus = 70
                            documentdesc = "Invoice - Not Approved"
                        elif approvrd_ck ==1:
                        
                            if list(df_corp_coding['approval_status'])[0].lower() == "approved":
                                docStatu = 2
                                substatus = 5
                                documentdesc = "Ready for ERP"
                            else: 
                                docStatu = 24
                                substatus = 137
                                documentdesc = "Pending Approval"

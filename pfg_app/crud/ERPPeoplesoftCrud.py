import traceback

import requests
from fastapi import HTTPException, Response

import pfg_app.model as model
from pfg_app import settings
from pfg_app.crud.ERPIntegrationCrud import processInvoiceVoucher
from pfg_app.crud.InvoiceCrud import update_docHistory
from pfg_app.logger_module import logger
from pfg_app.schemas.pfgtriggerSchema import InvoiceVoucherSchema
from pfg_app.session.session import get_db


def bulk_update_voucher_status():
    try:
        db = next(get_db())
        # Batch size for processing
        batch_size = 100  # Define a reasonable batch size
        # Fetch all document IDs with status id 7 (Sent to Peoplesoft) in batches
        doc_query = db.query(model.Document.idDocument).filter(
            model.Document.documentStatusID == 7
        )

        total_docs = doc_query.count()  # Total number of documents to process
        logger.info(f"Total documents to process: {total_docs}")
        # Process in batches
        for start in range(0, total_docs, batch_size):
            doc_ids = doc_query.offset(start).limit(batch_size).all()
        for doc_id in doc_ids:
            resp = processInvoiceVoucher(doc_id, db)
            if "data" in resp:
                if "Http Response" in resp["data"]:
                    RespCode = resp["data"]["Http Response"]
                    if resp["data"]["Http Response"].isdigit():
                        RespCodeInt = int(RespCode)
                        if RespCodeInt == 201:
                            dmsg = InvoiceVoucherSchema.SUCCESS_STAGED  # noqa: E501
                            docStatus = 7
                            docSubStatus = 43

                        elif RespCodeInt == 400:
                            dmsg = InvoiceVoucherSchema.FAILURE_IICS  # noqa: E501
                            docStatus = 21
                            docSubStatus = 108

                        elif RespCodeInt == 406:
                            dmsg = InvoiceVoucherSchema.FAILURE_INVOICE  # noqa: E501
                            docStatus = 21
                            docSubStatus = 109

                        elif RespCodeInt == 422:
                            dmsg = InvoiceVoucherSchema.FAILURE_PEOPLESOFT  # noqa: E501
                            docStatus = 21
                            docSubStatus = 110

                        elif RespCodeInt == 424:
                            dmsg = (
                                InvoiceVoucherSchema.FAILURE_FILE_ATTACHMENT  # noqa: E501
                            )
                            docStatus = 21
                            docSubStatus = 111

                        elif RespCodeInt == 500:
                            dmsg = (
                                InvoiceVoucherSchema.INTERNAL_SERVER_ERROR  # noqa: E501
                            )
                            docStatus = 21
                            docSubStatus = 53
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
                    dmsg = InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                    docStatus = 21
                    docSubStatus = 112
            else:
                dmsg = InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                docStatus = 21
                docSubStatus = 112
    except Exception as err:
        logger.info(f"PopleSoftResponseError: {err}")
        dmsg = InvoiceVoucherSchema.FAILURE_COMMON.format_message(err)  # noqa: E501
        docStatus = 21
        docSubStatus = 112

    try:
        db.query(model.Document).filter(model.Document.idDocument == doc_id).update(
            {
                model.Document.documentStatusID: docStatus,
                model.Document.documentsubstatusID: docSubStatus,  # noqa: E501
            }
        )
        db.commit()
    except Exception as err:
        logger.info(f"ErrorUpdatingPostingData: {err}")

    try:

        update_docHistory(doc_id, 1, docStatus, dmsg, db)
    except Exception as e:
        logger.error(f"ERPPeopleSoft 139: {str(e)}")
    except Exception as e:
        print("Error in ProcessInvoiceVoucher fun(): ", traceback.format_exc())
        logger.info(f"PopleSoftResponseError: {e}")
        dmsg = InvoiceVoucherSchema.FAILURE_COMMON.format_message(e)
        docStatus = 21
        docSubStatus = 112


def newbulkupdateInvoiceStatus():
    try:
        db = next(get_db())
        # Batch size for processing
        batch_size = 100  # Define a reasonable batch size

        # Fetch all document IDs with status id 7 (Sent to Peoplesoft) in batches
        doc_query = db.query(model.Document.idDocument).filter(
            model.Document.documentStatusID == 7
        )

        total_docs = doc_query.count()  # Total number of documents to process
        logger.info(f"Total documents to process: {total_docs}")

        # API credentials
        api_url = settings.erp_invoice_status_endpoint
        headers = {"Content-Type": "application/json"}
        auth = (settings.erp_user, settings.erp_password)

        # Process in batches
        for start in range(0, total_docs, batch_size):
            doc_ids = doc_query.offset(start).limit(batch_size).all()

            # Fetch voucher data for each document in the batch
            voucher_data_list = (
                db.query(model.VoucherData)
                .filter(
                    model.VoucherData.documentID.in_([doc_id[0] for doc_id in doc_ids])
                )
                .all()
            )

            # Prepare payloads and make API requests
            updates = []
            for voucherdata, doc_id in zip(voucher_data_list, doc_ids):
                # Prepare the payload for the API request
                invoice_status_payload = {
                    "RequestBody": {
                        "INV_STAT_RQST": {
                            "BUSINESS_UNIT": voucherdata.Business_unit,
                            "INVOICE_ID": voucherdata.Invoice_Id,
                            "INVOICE_DT": voucherdata.Invoice_Dt,
                            "VENDOR_SETID": voucherdata.Vendor_Setid,
                            "VENDOR_ID": voucherdata.Vendor_ID,
                        }
                    }
                }

                try:
                    # Make a POST request to the external API
                    response = requests.post(
                        api_url,
                        json=invoice_status_payload,
                        headers=headers,
                        auth=auth,
                        timeout=60,  # Set a timeout of 60 seconds
                    )
                    response.raise_for_status()  # Raise an exception for HTTP errors

                    # Process the response if the status code is 200
                    if response.status_code == 200:
                        invoice_data = response.json()
                        entry_status = invoice_data.get("ENTRY_STATUS")

                        # Determine the new document status based on ENTRY_STATUS
                        documentstatusid = None
                        if entry_status == "STG":
                            # Skip updating if entry_status is "STG"
                            # because the status is already 7
                            continue
                        elif entry_status == "QCK":
                            documentstatusid = 27
                        elif entry_status == "R":
                            documentstatusid = 28
                        elif entry_status == "P":
                            documentstatusid = 29
                        elif entry_status == "NF":
                            documentstatusid = 30

                        # If there's a valid document status update,
                        # add it to the bulk update list
                        if documentstatusid:
                            updates.append(
                                {
                                    "idDocument": doc_id[0],
                                    "documentStatusID": documentstatusid,
                                }
                            )

                except requests.exceptions.RequestException as e:
                    # Log the error and skip this document,
                    # but don't interrupt the batch
                    logger.error(f"Error for doc_id {doc_id[0]}: {str(e)}")

            # Perform bulk database update for the batch
            if updates:
                db.bulk_update_mappings(model.Document, updates)
                db.commit()  # Commit the changes for this batch

            logger.info(f"Processed batch {start} to {start + batch_size}")

        return {"message": "Bulk update completed successfully"}

    except Exception as e:
        logger.error(f"Error: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500, detail=f"Error processing invoice voucher: {str(e)}"
        )

import traceback

import pfg_app.model as model
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

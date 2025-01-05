# from sqlalchemy.orm import
import base64
import os
import re
import traceback
from datetime import datetime, timedelta

from azure.storage.blob import BlobServiceClient
from fastapi.responses import Response
from sqlalchemy import String, and_, case, cast, desc, exists, func, or_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Load, aliased, load_only

import pfg_app.model as model
from pfg_app import settings
from pfg_app.core.utils import get_credential
from pfg_app.logger_module import logger

status = [
    "System Check In - Progress",
    "Processing Document",
    "Finance Approval Completed",
    "Need To Review",
    "Edit in Progress",
    "Awaiting Edit Approval",
    "Sent to PeopleSoft",
    "Payment Cleared",
    "Payment Partially Paid",
    "Invoice Rejected",
    "Payment Rejected",
    "PO Open",
    "PO Closed",
    "Posted In ERP",
]

substatus = [
    (32, "ERP Exception"),
    (35, "Waiting for GRN creation"),
    (37, "GRN successfully created in ERP"),
    (39, "GRN Created in Serina"),
    (78, "GRN Approval Pending"),
]


async def read_paginate_doc_inv_list_with_ln_items(
    u_id,
    ven_id,
    inv_type,
    stat,
    off_limit,
    db,
    uni_api_filter,
    ven_status,
    date_range,
    sort_column,
    sort_order,
):
    """Function to read the paginated document invoice list.

    Parameters:
    ----------
    ven_id : int
        The ID of the vendor to filter the invoice documents.
    inv_type : str
        The type of invoice to filter the results.
    stat : Optional[str]
        The status of the invoice for filtering purposes.
    off_limit : tuple
        A tuple containing offset and limit for pagination.
    db : Session
        Database session object used to interact with the backend database.
    uni_api_filter : Optional[str]
        A universal filter for API queries.
    ven_status : Optional[str]
        Status of the vendor to filter the results.

    Returns:
    -------
    list
        A list containing the filtered document invoice data.
    """
    try:
        # Mapping document statuses to IDs
        all_status = {
            "posted": 14,
            "rejected": 10,
            "exception": 4,
            "VendorNotOnboarded": 25,
            "VendorUnidentified": 26,
            "Duplicate Invoice": 32,
        }

        # Dictionary to handle different types of invoices (ServiceProvider or Vendor)
        inv_choice = {
            "ser": (
                model.ServiceProvider,
                model.ServiceAccount,
                Load(model.ServiceProvider).load_only("ServiceProviderName"),
                Load(model.ServiceAccount).load_only("Account"),
            ),
            "ven": (
                model.Vendor,
                model.VendorAccount,
                Load(model.Vendor).load_only("VendorName", "Address", "VendorCode"),
                Load(model.VendorAccount).load_only("Account"),
            ),
        }

        # Create subquery for latest history logs
        latest_history_log = (
            db.query(
                model.DocumentHistoryLogs.documentID,
                func.max(model.DocumentHistoryLogs.CreatedOn).label("latest_created_on"),
            )
            .group_by(model.DocumentHistoryLogs.documentID)
            .subquery()
        )


        # Initial query setup for fetching document, status, and related entities
        data_query = (
            db.query(
                model.Document,
                model.DocumentStatus,
                model.DocumentSubStatus,
                inv_choice[inv_type][0],
                inv_choice[inv_type][1],
                model.User.firstName.label("last_updated_by"),
            )
            .options(
                Load(model.Document).load_only(
                    "docheaderID",
                    "totalAmount",
                    "documentStatusID",
                    "CreatedOn",
                    "documentsubstatusID",
                    "sender",
                    "JournalNumber",
                    "UploadDocType",
                    "store",
                    "dept",
                    "documentDate",
                    "voucher_id",
                    "mail_row_key",
                ),
                Load(model.DocumentSubStatus).load_only("status"),
                Load(model.DocumentStatus).load_only("status", "description"),
                inv_choice[inv_type][2],
                inv_choice[inv_type][3],
                # Load(model.User).load_only("firstName"),
            )
            .join(
                model.DocumentSubStatus,
                model.DocumentSubStatus.idDocumentSubstatus
                == model.Document.documentsubstatusID,
                isouter=True,
            )
            .join(
                model.VendorAccount,
                model.VendorAccount.idVendorAccount == model.Document.vendorAccountID,
                isouter=True,
            )
            .join(
                model.Vendor,
                model.Vendor.idVendor == model.VendorAccount.vendorID,
                isouter=True,
            )
            .join(
                model.DocumentStatus,
                model.DocumentStatus.idDocumentstatus
                == model.Document.documentStatusID,
                isouter=True,
            )
            .join(
                latest_history_log,
                latest_history_log.c.documentID == model.Document.idDocument,
                isouter=True,
            )
            .join(
                model.DocumentHistoryLogs,
                and_(
                    model.DocumentHistoryLogs.documentID == model.Document.idDocument,
                    model.DocumentHistoryLogs.CreatedOn == latest_history_log.c.latest_created_on,
                ),
                isouter=True,
            )
            .join(
                model.User,
                model.User.idUser == model.DocumentHistoryLogs.userID,
                isouter=True,
            )
            .filter(
                model.Document.idDocumentType == 3,
                model.Document.vendorAccountID.isnot(None),
            )
        )

        # Apply vendor ID filter if provided
        if ven_id:
            sub_query = db.query(model.VendorAccount.idVendorAccount).filter_by(
                vendorID=ven_id
            )
            data_query = data_query.filter(
                model.Document.vendorAccountID.in_(sub_query)
            )

        status_list = []
        if stat:
            # Split the status string by ':' to get a list of statuses
            status_list = stat.split(":")

            # Map status names to IDs
            status_ids = [all_status[s] for s in status_list if s in all_status]
            if status_ids:
                data_query = data_query.filter(
                    model.Document.documentStatusID.in_(status_ids)
                )
        # Apply vendor status filter if provided
        if ven_status:
            if ven_status == "A":
                data_query = data_query.filter(
                    func.jsonb_extract_path_text(
                        model.Vendor.miscellaneous, "VENDOR_STATUS"
                    )
                    == "A"
                )
            elif ven_status == "I":
                data_query = data_query.filter(
                    func.jsonb_extract_path_text(
                        model.Vendor.miscellaneous, "VENDOR_STATUS"
                    )
                    == "I"
                )

        # Apply date range filter for documentDate
        if date_range:
            frdate, todate = date_range.lower().split("to")
            frdate = datetime.strptime(frdate.strip(), "%Y-%m-%d")
            todate = datetime.strptime(
                todate.strip(), "%Y-%m-%d"
            )  # Remove timedelta adjustments
            frdate_str = frdate.strftime("%Y-%m-%d")
            todate_str = todate.strftime("%Y-%m-%d")
            data_query = data_query.filter(
                or_(
                    model.Document.documentDate.between(frdate_str, todate_str),
                    model.Document.CreatedOn.between(frdate, todate),
                )
            )

        # Function to normalize strings by removing non-alphanumeric
        # characters and converting to lowercase
        def normalize_string(input_str):
            return func.lower(func.regexp_replace(input_str, r"[^a-zA-Z0-9]", "", "g"))

        # Apply universal API filter if provided, including line items
        if uni_api_filter:
            uni_search_param_list = uni_api_filter.split(":")
            for param in uni_search_param_list:
                # Normalize the user input filter
                normalized_filter = re.sub(r"[^a-zA-Z0-9]", "", param.lower())

                # Create a pattern for the search with wildcards
                pattern = f"%{normalized_filter}%"

                filter_condition = or_(
                    normalize_string(model.Document.docheaderID).ilike(pattern),
                    normalize_string(model.Document.documentDate).ilike(pattern),
                    normalize_string(model.Document.sender).ilike(pattern),
                    cast(model.Document.totalAmount, String).ilike(
                        f"%{uni_api_filter}%"
                    ),
                    func.to_char(model.Document.CreatedOn, "YYYY-MM-DD").ilike(
                        f"%{uni_api_filter}%"
                    ),  # noqa: E501
                    normalize_string(model.Document.JournalNumber).ilike(pattern),
                    normalize_string(model.Document.UploadDocType).ilike(pattern),
                    normalize_string(model.Document.store).ilike(pattern),
                    normalize_string(model.Document.dept).ilike(pattern),
                    normalize_string(model.Document.voucher_id).ilike(pattern),
                    normalize_string(model.Document.mail_row_key).ilike(pattern),
                    normalize_string(model.Vendor.VendorName).ilike(pattern),
                    normalize_string(model.Vendor.Address).ilike(pattern),
                    normalize_string(model.DocumentSubStatus.status).ilike(pattern),
                    normalize_string(model.DocumentStatus.status).ilike(pattern),
                    normalize_string(model.DocumentStatus.description).ilike(pattern),
                    normalize_string(inv_choice[inv_type][1].Account).ilike(pattern),
                    # Check if any related DocumentLineItems.Value matches the filter
                    exists().where(
                        (
                            model.DocumentLineItems.documentID
                            == model.Document.idDocument
                        )
                        & normalize_string(model.DocumentLineItems.Value).ilike(pattern)
                    ),
                )
                data_query = data_query.filter(filter_condition)

        # Get the total count of records before applying limit and offset
        total_count = data_query.distinct(model.Document.idDocument).count()
        
        # Pagination
        offset, limit = off_limit
        off_val = (offset - 1) * limit
        if off_val < 0:
            return Response(
                status_code=403,
                headers={"ClientError": "Please provide a valid offset value."},
            )
        
        # Apply sorting
        sort_columns_map = {
            "Invoice Number": model.Document.docheaderID,
            "Vendor Code": model.Vendor.VendorCode,
            "Vendor Name": model.Vendor.VendorName,
            "Confirmation Number": model.Document.JournalNumber,
            "Store": model.Document.store,
            "Department": model.Document.dept,
            "Status": model.DocumentStatus.status,
            "Sub Status": model.DocumentSubStatus.status,
            "Amount": model.Document.totalAmount,
            "Upload Date": model.Document.CreatedOn,
        }

        if sort_column in sort_columns_map:
            # sort_field = sort_columns_map.get(sort_column, model.Document.idDocument)
            sort_field = sort_columns_map[sort_column]
            
            if sort_order.lower() == "desc":
                # Apply descending order to sort_field
                data_query = data_query.order_by(sort_field.desc())
            else:
                # Apply ascending order to sort_field
                data_query = data_query.order_by(sort_field.asc())

            Documentdata = (data_query.limit(limit).offset(off_val).all())
            
        else:
            data_query = data_query.order_by(model.Document.idDocument.desc())
            # Apply pagination
            Documentdata = (
            data_query.distinct(model.Document.idDocument)
            .limit(limit)
            .offset(off_val)
            .all()
        )

        # Return paginated document data with line items
        return {"ok": {"Documentdata": Documentdata, "TotalCount": total_count}}

    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()




async def read_invoice_data(u_id, inv_id, db):
    """
    This function reads the invoice list and contains the following parameters:

    Parameters:
    -----------
    u_id : int
        The user ID provided as a function parameter.
    inv_id : int
        The invoice ID provided as a function parameter.
    db : Session
        A session object that interacts with the backend database.

    Returns:
    --------
    dict
        A dictionary containing the result of the vendordata, invoice header,
        line items and upload time .
    """
    try:
        vendordata = ""
        # Fetching invoice data along with DocumentStatus using correct join
        invdat = (
            db.query(model.Document, model.DocumentStatus.status)
            .join(
                model.DocumentStatus,
                model.Document.documentStatusID
                == model.DocumentStatus.idDocumentstatus,
                isouter=True,
            )
            .filter(model.Document.idDocument == inv_id)  # Use correct field in filter
            .one()
        )

        # provide vendor details
        if invdat.Document.vendorAccountID:
            vendordata = (
                db.query(model.Vendor, model.VendorAccount)
                .options(
                    Load(model.Vendor).load_only(
                        "VendorName",
                        "VendorCode",
                        "vendorType",
                        "Address",
                        "City",
                        "miscellaneous",
                    ),
                    Load(model.VendorAccount).load_only("Account"),
                )
                .filter(
                    model.VendorAccount.idVendorAccount
                    == invdat.Document.vendorAccountID
                )
                .join(
                    model.VendorAccount,
                    model.VendorAccount.vendorID == model.Vendor.idVendor,
                    isouter=True,
                )
                .all()
            )
        # provide header deatils of invoce
        headerdata = (
            db.query(model.DocumentData, model.DocumentTagDef, model.DocumentUpdates)
            .options(
                Load(model.DocumentData).load_only(
                    "Value",
                    "isError",
                    "ErrorDesc",
                    "IsUpdated",
                    "Xcord",
                    "Ycord",
                    "Width",
                    "Height",
                ),
                Load(model.DocumentTagDef).load_only("TagLabel"),
                Load(model.DocumentUpdates).load_only("OldValue", "UpdatedOn"),
            )
            .filter(
                model.DocumentData.documentTagDefID
                == model.DocumentTagDef.idDocumentTagDef,
                model.DocumentData.documentID == inv_id,
            )
            .outerjoin(
                model.DocumentUpdates,
                (
                    model.DocumentUpdates.documentDataID
                    == model.DocumentData.idDocumentData
                )
                & (model.DocumentUpdates.IsActive == 1),
            )
            # .join(
            #     model.DocumentUpdates,
            #     model.DocumentUpdates.documentDataID
            #     == model.DocumentData.idDocumentData,
            #     isouter=True,
            # )
            # .filter(
            #     or_(
            #         model.DocumentData.IsUpdated == 0,
            #         model.DocumentUpdates.IsActive == 1,
            #     )
            # )
        )
        headerdata = headerdata.all()
        # provide linedetails of invoice, add this later , "isError", "IsUpdated"
        # to get the unique line item tags
        subquery = (
            db.query(model.DocumentLineItems.lineItemtagID)
            .filter_by(documentID=inv_id)
            .distinct()
        )
        # get all the related line tags description
        doclinetags = (
            db.query(model.DocumentLineItemTags)
            .options(load_only("TagName"))
            .filter(model.DocumentLineItemTags.idDocumentLineItemTags.in_(subquery))
            .all()
        )

        for row in doclinetags:
            # Build the initial query for line data
            query = (
                db.query(model.DocumentLineItems, model.DocumentUpdates)
                .options(
                    Load(model.DocumentLineItems).load_only(
                        "Value",
                        "IsUpdated",
                        "isError",
                        "ErrorDesc",
                        "Xcord",
                        "Ycord",
                        "Width",
                        "Height",
                        "itemCode",
                    ),
                    Load(model.DocumentUpdates).load_only("OldValue", "UpdatedOn"),
                )
                .filter(
                    model.DocumentLineItems.lineItemtagID == row.idDocumentLineItemTags,
                    model.DocumentLineItems.documentID == inv_id,
                )
                .join(
                    model.DocumentUpdates,
                    model.DocumentUpdates.documentLineItemID
                    == model.DocumentLineItems.idDocumentLineItems,
                    isouter=True,
                )
                .filter(
                    or_(
                        model.DocumentLineItems.IsUpdated == 0,
                        model.DocumentUpdates.IsActive == 1,
                    )
                )
            )
            # Execute the query to retrieve the data
            linedata = query.all()

            # Attach the linedata to the row
            row.linedata = linedata

        return {
            "ok": {
                "vendordata": vendordata,
                "headerdata": headerdata,
                "linedata": doclinetags,
                "uploadtime": invdat.Document.uploadtime,
                "documentstatusid": invdat.Document.documentStatusID,
                "documentstatus": invdat.status,  # Return status
                "documentsubstatusid": invdat.Document.documentsubstatusID,
            }
        }

    except Exception:
        logger.error(f"Error in line item :{traceback.format_exc()}")
        return Response(status_code=500, headers={"codeError": "Server Error"})
    finally:
        db.close()


async def read_invoice_file(u_id, inv_id, db):
    """Function to read the invoice file and return its base64 encoded content
    along with the content type.

    Parameters:
    ----------
    u_id : int
        User ID of the requester.
    inv_id : int
        Invoice ID for which the file is to be retrieved.
    db : Session
        Database session object used to interact with the backend database.

    Returns:
    -------
    dict
        A dictionary containing the file path in base64 format and its content type.
    """
    try:
        content_type = "application/pdf"
        # getting invoice data for later operation
        invdat = (
            db.query(model.Document)
            .options(load_only("docPath", "supplierAccountID", "vendorAccountID"))
            .filter_by(idDocument=inv_id)
            .one()
        )
        # check if file path is present and give base64 coded image url
        if invdat.docPath:
            try:
                fr_data = (
                    db.query(model.FRConfiguration)
                    .options(
                        load_only(
                            "ConnectionString", "ContainerName", "ServiceContainerName"
                        )
                    )
                    .filter_by(idCustomer=1)
                    .one()
                )
                account_url = (
                    f"https://{settings.storage_account_name}.blob.core.windows.net"
                )
                blob_service_client = BlobServiceClient(
                    account_url=account_url, credential=get_credential()
                )
                if invdat.supplierAccountID is not None:
                    blob_client = blob_service_client.get_blob_client(
                        container=fr_data.ContainerName, blob=invdat.docPath
                    )
                if invdat.vendorAccountID is not None:
                    blob_client = blob_service_client.get_blob_client(
                        container=fr_data.ContainerName, blob=invdat.docPath
                    )

                # invdat.docPath = str(list(blob_client.download_blob().readall()))
                try:
                    filetype = os.path.splitext(invdat.docPath)[1].lower()
                    if filetype == ".png":
                        content_type = "image/png"
                    elif filetype == ".jpg" or filetype == ".jpeg":
                        content_type = "image/jpg"
                    else:
                        content_type = "application/pdf"
                except Exception:
                    print(f"Error in file type : {traceback.format_exc()}")
                invdat.docPath = base64.b64encode(blob_client.download_blob().readall())
            except Exception:
                logger.error(traceback.format_exc())
                invdat.docPath = ""

        return {"result": {"filepath": invdat.docPath, "content_type": content_type}}

    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500, headers={"codeError": "Server Error"})
    finally:
        db.close()


# async def update_invoice_data(u_id, inv_id, inv_data, db):
#     """Function to update the invoice line item data.

#     Parameters:
#     ----------
#     u_id : int
#         User ID of the requester.
#     inv_id : int
#         Invoice ID for the invoice line item to be updated.
#     inv_data : PydanticModel
#         Pydantic model object containing the member data for updating the invoice line
#         item.
#     db : Session
#         Database session object, used to interact with the backend database.

#     Returns:
#     -------
#     dict
#         A dictionary containing the result of the update operation.
#     """
#     try:
#         # avoid data updates by other users if in lock
#         dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#         for row in inv_data:
#             try:
#                 # check to see if the document id and document data are related
#                 if row.documentDataID:
#                     db.query(model.DocumentData).filter_by(
#                         documentID=inv_id, idDocumentData=row.documentDataID
#                     ).scalar()
#                 else:
#                     db.query(model.DocumentLineItems).filter_by(
#                         documentID=inv_id, idDocumentLineItems=row.documentLineItemID
#                     ).scalar()
#             except Exception:
#                 logger.error(traceback.format_exc())
#                 return Response(
#                     status_code=403,
#                     headers={"ClientError": "invoice and value mismatch"},
#                 )
#             # to check if the document update table, already has rows present in it
#             inv_up_data_id = (
#                 db.query(model.DocumentUpdates.idDocumentUpdates)
#                 .filter_by(documentDataID=row.documentDataID)
#                 .all()
#             )
#             inv_up_line_id = (
#                 db.query(model.DocumentUpdates.idDocumentUpdates)
#                 .filter_by(documentLineItemID=row.documentLineItemID)
#                 .all()
#             )
#             if len(inv_up_data_id) > 0 or len(inv_up_line_id) > 0:
#                 # if present set active status to false for old row
#                 if row.documentDataID:
#                     db.query(model.DocumentUpdates).filter_by(
#                         documentDataID=row.documentDataID, IsActive=1
#                     ).update({"IsActive": 0})
#                 else:
#                     db.query(model.DocumentUpdates).filter_by(
#                         documentLineItemID=row.documentLineItemID, IsActive=1
#                     ).update({"IsActive": 0})
#                 db.flush()
#             data = dict(row)
#             data["IsActive"] = 1
#             # data["updatedBy"] = u_id
#             data["UpdatedOn"] = dt
#             data = model.DocumentUpdates(**data)
#             db.add(data)
#             db.flush()
#             if row.documentDataID:
#                 doc_table_match = {
#                     "InvoiceTotal": "totalAmount",
#                     "InvoiceDate": "documentDate",
#                     "InvoiceId": "docheaderID",
#                     "PurchaseOrder": "PODocumentID",
#                 }
#                 ser_doc_table_match = {
#                     "Issue Date": "documentDate",
#                     "Total Due Inc VAT": "totalAmount",
#                     "Invoice ID": "docheaderID",
#                 }
#                 tag_def_inv_id = (
#                     db.query(
#                         model.DocumentData.documentTagDefID,
#                         model.DocumentData.documentID,
#                     )
#                     .filter_by(idDocumentData=row.documentDataID)
#                     .one()
#                 )
#                 label = (
#                     db.query(model.DocumentTagDef.TagLabel)
#                     .filter_by(idDocumentTagDef=tag_def_inv_id.documentTagDefID)
#                     .scalar()
#                 )
#                 # to update the document if header data is updated
#                 if label in doc_table_match.keys():
#                     db.query(model.Document).filter_by(
#                         idDocument=tag_def_inv_id.documentID
#                     ).update({doc_table_match[label]: data.NewValue})

#                 # update the document if header data is updated for service provider
#                 if label in ser_doc_table_match.keys():
#                     value = data.NewValue
#                     if label == "Total Due Inc VAT":
#                         value = float(re.sub(r"[^0-9.]", "", value))
#                     db.query(model.Document).filter_by(
#                         idDocument=tag_def_inv_id.documentID
#                     ).update({ser_doc_table_match[label]: value})
#                 db.query(model.DocumentData).filter_by(
#                     idDocumentData=row.documentDataID
#                 ).update({"IsUpdated": 1, "isError": 0, "Value": data.NewValue})


#             else:
#                 db.query(model.DocumentLineItems).filter_by(
#                     idDocumentLineItems=row.documentLineItemID
#                 ).update({"IsUpdated": 1, "isError": 0, "Value": data.NewValue})
#             db.flush()
#         # last updated time stamp
#         db.query(model.Document).filter_by(idDocument=inv_id).update(
#             {"UpdatedOn": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}
#         )
#         db.commit()
#         return {"result": "success"}
#     except Exception:
#         logger.error(traceback.format_exc())
#         db.rollback()
#         return Response(status_code=500, headers={"Error": "Server error"})
#     finally:
#         db.close()


# ------------------------------New Change ---------------------------------------


async def update_invoice_data(u_id, inv_id, inv_data, db):
    """Function to update the invoice line item data.

    Parameters:
    ----------
    u_id : int
        User ID of the requester.
    inv_id : int
        Invoice ID for the invoice line item to be updated.
    inv_data : PydanticModel
        Pydantic model object containing the member data for updating the invoice line
        item.
    db : Session
        Database session object, used to interact with the backend database.

    Returns:
    -------
    dict
        A dictionary containing the result of the update operation.
    """
    try:
        # avoid data updates by other users if in lock
        dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        docStatus_id = (
            db.query(model.Document.documentStatusID)
            .filter(model.Document.idDocument == inv_id)
            .scalar()
        )
        consolidated_updates = []  # Store updates for consolidated log
        for row in inv_data:
            try:
                # Check if the document id and document data are related
                if row.documentDataID:
                    db.query(model.DocumentData).filter_by(
                        documentID=inv_id, idDocumentData=row.documentDataID
                    ).scalar()
                else:
                    db.query(model.DocumentLineItems).filter_by(
                        documentID=inv_id, idDocumentLineItems=row.documentLineItemID
                    ).scalar()
            except Exception:
                logger.error(traceback.format_exc())
                return Response(
                    status_code=403,
                    headers={"ClientError": "invoice and value mismatch"},
                )

            # Check if the document update table already has rows present in it
            inv_up_data_id = (
                db.query(model.DocumentUpdates.idDocumentUpdates)
                .filter_by(documentDataID=row.documentDataID)
                .all()
            )
            inv_up_line_id = (
                db.query(model.DocumentUpdates.idDocumentUpdates)
                .filter_by(documentLineItemID=row.documentLineItemID)
                .all()
            )
            if len(inv_up_data_id) > 0 or len(inv_up_line_id) > 0:
                # If present, set the active status to false for the old row
                if row.documentDataID:
                    db.query(model.DocumentUpdates).filter_by(
                        documentDataID=row.documentDataID, IsActive=1
                    ).update({"IsActive": 0})
                else:
                    db.query(model.DocumentUpdates).filter_by(
                        documentLineItemID=row.documentLineItemID, IsActive=1
                    ).update({"IsActive": 0})
                db.flush()

            # Add the new update to DocumentUpdates
            data = dict(row)
            data["IsActive"] = 1
            data["UpdatedOn"] = dt
            data = model.DocumentUpdates(**data)
            db.add(data)
            db.flush()

            # If documentDataID is present, check for VendorName updates
            if row.documentDataID:
                doc_table_match = {
                    "InvoiceTotal": "totalAmount",
                    "InvoiceDate": "documentDate",
                    "InvoiceId": "docheaderID",
                }

                # Get the documentTagDefID associated with the documentDataID
                tag_def_inv_id = (
                    db.query(
                        model.DocumentData.documentTagDefID,
                        model.DocumentData.documentID,
                    )
                    .filter_by(idDocumentData=row.documentDataID)
                    .one()
                )

                # Get the TagLabel from the DocumentTagDef table
                label = (
                    db.query(model.DocumentTagDef.TagLabel)
                    .filter_by(idDocumentTagDef=tag_def_inv_id.documentTagDefID)
                    .scalar()
                )
                # to update the document if header data is updated
                if label in doc_table_match.keys():
                    db.query(model.Document).filter_by(
                        idDocument=tag_def_inv_id.documentID
                    ).update({doc_table_match[label]: data.NewValue})

                # If the TagLabel is "VendorName", proceed with fetching VendorCode
                if label == "VendorName":
                    # Fetch VendorCode using the NewValue from the Vendor table
                    vendor = (
                        db.query(model.Vendor)
                        .filter_by(VendorName=row.NewValue)
                        .first()
                    )

                    if vendor and vendor.VendorCode:
                        # Get idVendorAccount using the VendorCode
                        vendor_account = (
                            db.query(model.VendorAccount)
                            .filter_by(Account=vendor.VendorCode)
                            .first()
                        )

                        if vendor_account:
                            # Get the count of active DocumentModel for the vendorAccountID
                            active_models_query = db.query(
                                model.DocumentModel
                            ).filter_by(
                                idVendorAccount=vendor_account.idVendorAccount,
                                is_active=1,
                            )
                            active_model_count = active_models_query.count()

                            if active_model_count == 0:
                                # No active DocumentModel found
                                return {"message": "Vendor Onboarding is not done"}
                            elif active_model_count > 1:
                                # More than one active DocumentModel found
                                return {"message": "Multiple active models exist"}
                            else:
                                # Exactly one active DocumentModel found
                                document_model = active_models_query.first()

                                # Update Document's vendorAccountID and idDocumentModel
                                db.query(model.Document).filter_by(
                                    idDocument=inv_id
                                ).update(
                                    {
                                        "vendorAccountID": vendor_account.idVendorAccount,  # noqa: E501
                                        "documentModelID": document_model.idDocumentModel,  # noqa: E501
                                    }
                                )
                                db.flush()

                                # Fetch all documentTagDefs for the updated
                                # documentModel
                                all_tag_defs = (
                                    db.query(model.DocumentTagDef)
                                    .filter_by(
                                        idDocumentModel=document_model.idDocumentModel
                                    )
                                    .all()
                                )

                                # Fetch all  DocumentData based on inv_id
                                doc_data = (
                                    db.query(model.DocumentData)
                                    .filter_by(documentID=inv_id)
                                    .all()
                                )

                                # Loop through DocumentData entries
                                for doc_entry in doc_data:
                                    # Get the current TagLabel for the documentTagDefID
                                    # in DocumentData
                                    current_tag_label = (
                                        db.query(model.DocumentTagDef.TagLabel)
                                        .filter_by(
                                            idDocumentTagDef=doc_entry.documentTagDefID
                                        )
                                        .scalar()
                                    )

                                    # Check if the TagLabel matches with any
                                    # from all_tag_defs
                                    for tag_def in all_tag_defs:
                                        if current_tag_label == tag_def.TagLabel:
                                            # Swap the documentTagDefID in DocumentData
                                            db.query(model.DocumentData).filter_by(
                                                idDocumentData=doc_entry.idDocumentData
                                            ).update(
                                                {
                                                    "documentTagDefID": tag_def.idDocumentTagDef  # noqa: E501
                                                }
                                            )  # noqa: E501
                                            db.flush()
                                            break  # TagLabel match found, stop the loo

                # Update document data as per original logic
                db.query(model.DocumentData).filter_by(
                    idDocumentData=row.documentDataID
                ).update({"IsUpdated": 1, "isError": 0, "Value": data.NewValue})

                consolidated_updates.append(
                    f"Field {label} updated to {data.NewValue} from {data.OldValue}"
                )

            else:
                # Update DocumentLineItems for line item updates
                db.query(model.DocumentLineItems).filter_by(
                    idDocumentLineItems=row.documentLineItemID
                ).update({"IsUpdated": 1, "isError": 0, "Value": data.NewValue})

            db.flush()
        # Updating the consolidated history log for updated fields
        if consolidated_updates:
            try:
                update_docHistory(
                    inv_id,
                    u_id,
                    docStatus_id,
                    "; ".join(consolidated_updates),
                    db,
                )
            except Exception:
                logger.error(
                    f"Error updating document history: {traceback.format_exc()}"
                )
        # Update the last updated timestamp for the document
        db.query(model.Document).filter_by(idDocument=inv_id).update(
            {"UpdatedOn": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}
        )
        db.commit()

        return {"result": "success"}

    except Exception:
        logger.error(traceback.format_exc())
        db.rollback()
        return Response(status_code=500, headers={"Error": "Server error"})
    finally:
        db.close()


async def update_column_pos(u_id, tabtype, col_data, bg_task, db):
    """Function to update the column position of a specified tab.

    Parameters:
    ----------
    u_id : int
        User ID provided as a function parameter.
    tabtype : str
        Tab type used to identify which tab's column position to update.
    col_data : PydanticModel
        Pydantic model containing the column data for updating the column position.
    bg_task : BackgroundTasks
        Background task manager for handling asynchronous tasks.
    db : Session
        Database session object, used to interact with the backend database.

    Returns:
    -------
    dict
        A dictionary containing the result of the update operation.
    """
    try:
        UpdatedOn = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for items in col_data:
            items = dict(items)
            items["UpdatedOn"] = UpdatedOn
            items["documentColumnPos"] = items.pop("ColumnPos")

            db.query(model.DocumentColumnPos).filter_by(
                idDocumentColumn=items.pop("idtabColumn")
            ).update(items)

        db.commit()
        return {"result": "updated"}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(
            status_code=403,
            headers={f"{traceback.format_exc()}clientError": "update failed"},
        )
    finally:
        db.close()


async def read_column_pos(userID, tabtype, db):
    """Function to retrieve the column position based on the tab type.

    Parameters:
    ----------
    u_id : int
        User ID provided as a function parameter.
    tabtype : str
        Tab type used to filter and retrieve the column positions.
    db : Session
        Database session object, used to interact with the backend database.

    Returns:
    -------
    dict
        A dictionary containing the column positions for the specified tab type.
    """
    try:
        # Query to retrieve column data based on userID and tab type
        column_data = (
            db.query(model.DocumentColumnPos, model.ColumnPosDef)
            .filter_by()
            .options(
                Load(model.DocumentColumnPos).load_only(
                    "documentColumnPos", "isActive"
                ),
                Load(model.ColumnPosDef).load_only(
                    "columnName", "columnDescription", "dbColumnname"
                ),
            )
            .filter(
                model.DocumentColumnPos.columnNameDefID == model.ColumnPosDef.idColumn,
                model.DocumentColumnPos.userID == userID,
                model.DocumentColumnPos.tabtype == tabtype,
            )
            .all()
        )
        # If no column data is found, copy default settings from the admin (userID=1)
        if len(column_data) == 0:
            allcolumns = (
                db.query(model.DocumentColumnPos)
                .filter(model.DocumentColumnPos.userID == 1)
                .all()
            )
            # Insert default column positions for the current user
            for ac in allcolumns:
                to_insert = {
                    "columnNameDefID": ac.columnNameDefID,
                    "documentColumnPos": ac.documentColumnPos,
                    "isActive": ac.isActive,
                    "tabtype": ac.tabtype,
                    "userID": userID,
                }
                db.add(model.DocumentColumnPos(**to_insert))
                db.commit()
            # Fetch column data again after inserting defaults
            column_data = (
                db.query(model.DocumentColumnPos, model.ColumnPosDef)
                .filter_by()
                .options(
                    Load(model.DocumentColumnPos).load_only(
                        "documentColumnPos", "isActive"
                    ),
                    Load(model.ColumnPosDef).load_only(
                        "columnName", "columnDescription", "dbColumnname"
                    ),
                )
                .filter(
                    model.DocumentColumnPos.columnNameDefID
                    == model.ColumnPosDef.idColumn,
                    model.DocumentColumnPos.userID == userID,
                    model.DocumentColumnPos.tabtype == tabtype,
                )
                .all()
            )
        # Convert the query result (a tuple of two models) to a list of dictionaries
        column_data_list = []
        for row in column_data:
            row_dict = {}
            for idx, col in enumerate(row):
                if isinstance(col, model.DocumentColumnPos):
                    row_dict["DocumentColumnPos"] = {
                        "documentColumnPos": col.documentColumnPos,
                        "isActive": col.isActive,
                        "idDocumentColumn": col.idDocumentColumn,
                    }
                elif isinstance(col, model.ColumnPosDef):
                    row_dict["ColumnPosDef"] = {
                        "columnName": col.columnName,
                        "columnDescription": col.columnDescription,
                        "dbColumnname": col.dbColumnname,
                        "idColumn": col.idColumn,
                    }
            column_data_list.append(row_dict)
        return {"col_data": column_data_list}
    except Exception:
        # Log any exceptions and return a 500 response
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        # Ensure the database session is closed
        db.close()


async def get_role_priority(u_id, db):
    """This function retrieves the role priority for a given user based on
    their access permissions.

    Parameters:
    ----------
    u_id : int
        The ID of the user whose role priority is being retrieved.
    db : Session
        Database session object to interact with the backend.

    Returns:
    -------
    int
        The priority value of the user's role. If an error occurs, it returns
        0 as a default.
    """
    try:
        # Subquery to get the permission definition ID for the user
        sub_query = db.query(model.AccessPermission.permissionDefID).filter_by(
            userID=u_id
        )
        # Query to get the priority value for the permission definition
        # from AccessPermissionDef table
        return (
            db.query(model.AccessPermissionDef.Priority)
            .filter_by(idAccessPermissionDef=sub_query.scalar_subquery())
            .scalar()
        )
    except Exception:
        # Log the error and return a default priority of 0 in case of failure
        logger.error(traceback.format_exc())
        return 0
    finally:
        # Ensure the database session is closed properly
        db.close()


async def read_invoice_status_history(u_id, inv_id, db):
    """Function to read the invoice status history logs.

    Parameters:
    ----------
    u_id : int
        The ID of the user requesting the invoice status history.
    inv_id : int
        The ID of the invoice whose status history is being retrieved.
    db : Session
        Database session object to interact with the backend.

    Returns:
    -------
    list
        A list of document history logs with associated user information,
        status descriptions, and financial status.
    """
    try:
        # Define a case statement to translate the status and sub-status IDs into
        # meaningful status descriptions
        doc_status_hist = case(
            [
                (
                    and_(
                        model.DocumentHistoryLogs.documentStatusID == 4,
                        model.DocumentHistoryLogs.documentSubStatusID == 29,
                    ),
                    model.DocumentHistoryLogs.documentdescription,
                ),
                (model.DocumentHistoryLogs.documentStatusID == 0, "Invoice Uploaded"),
                (
                    model.DocumentHistoryLogs.documentStatusID == 2,
                    "Document Processed Successfully",
                ),
                (model.DocumentHistoryLogs.documentStatusID == 3, "Approval Completed"),
                (model.DocumentHistoryLogs.documentSubStatusID == 8, "PO Item Check"),
                (
                    model.DocumentHistoryLogs.documentSubStatusID == 16,
                    "Unitprice MisMatched",
                ),
                (
                    model.DocumentHistoryLogs.documentSubStatusID == 21,
                    "PO Quantity Check",
                ),
                (
                    model.DocumentHistoryLogs.documentSubStatusID == 32,
                    "Invoice ERP Error",
                ),
                (model.DocumentHistoryLogs.documentSubStatusID == 34, "PO Line Issue"),
                (
                    model.DocumentHistoryLogs.documentSubStatusID == 35,
                    "Waiting for GRN creation",
                ),
                (
                    model.DocumentHistoryLogs.documentSubStatusID == 39,
                    "GRN Created in Serina",
                ),
                (
                    model.DocumentHistoryLogs.documentSubStatusID == 37,
                    "GRN Created in ERP",
                ),
                (model.DocumentHistoryLogs.documentSubStatusID == 40, "GRN ERP Error"),
                (
                    and_(
                        model.DocumentHistoryLogs.documentSubStatusID == 3,
                        model.DocumentHistoryLogs.documentStatusID is None,
                    ),
                    "OCR Error Corrected",
                ),
                (
                    and_(
                        model.DocumentHistoryLogs.documentSubStatusID == 3,
                        model.DocumentHistoryLogs.documentStatusID == 1,
                    ),
                    "Invoice Submitted for Batch",
                ),
                (model.DocumentHistoryLogs.documentStatusID == 5, "Edit in Progress"),
                (
                    model.DocumentHistoryLogs.documentStatusID == 6,
                    "Awaiting Edit Approval",
                ),
                (model.DocumentHistoryLogs.documentStatusID == 7, "Sent to PeopleSoft"),
                (model.DocumentHistoryLogs.documentStatusID == 8, "Payment Cleared"),
                (
                    model.DocumentHistoryLogs.documentStatusID == 9,
                    "Payment Partially Paid",
                ),
                (model.DocumentHistoryLogs.documentStatusID == 10, "Invoice Rejected"),
                (model.DocumentHistoryLogs.documentStatusID == 11, "Payment Rejected"),
                (model.DocumentHistoryLogs.documentStatusID == 12, "PO Open"),
                (model.DocumentHistoryLogs.documentStatusID == 13, "PO Closed"),
                (model.DocumentHistoryLogs.documentStatusID == 14, "Posted In ERP"),
                (model.DocumentHistoryLogs.documentStatusID == 16, "Invoice Upload"),
                (model.DocumentHistoryLogs.documentStatusID == 17, "Edit Rule"),
                (model.DocumentHistoryLogs.documentStatusID == 18, "Approved Rule"),
                (model.DocumentHistoryLogs.documentStatusID == 19, "ERP Updated"),
                (
                    model.DocumentHistoryLogs.documentStatusID == 21,
                    "GRN Invoice Rejected",
                ),
            ]
        ).label("dochistorystatus")
        # Define another case statement to translate the financial status
        # into meaningful labels
        doc_fin_status = case(
            [
                (
                    model.DocumentHistoryLogs.documentfinstatus == 0,
                    "Partially Approved",
                ),
                (
                    model.DocumentHistoryLogs.documentfinstatus == 1,
                    "Completely Approved",
                ),
            ],
            else_="UNKNOWN",
        ).label("documentFinancialStatus")
        # Query to fetch document history logs with relevant fields
        return (
            db.query(
                model.DocumentHistoryLogs,
                model.User.firstName,
                model.User.lastName,
                model.Document.docheaderID,
                doc_status_hist,
                doc_fin_status,
            )
            .options(load_only("documentdescription", "userAmount", "CreatedOn"))
            .filter(model.DocumentHistoryLogs.documentID == model.Document.idDocument)
            .join(
                model.User,
                model.DocumentHistoryLogs.userID == model.User.idUser,
                isouter=True,
            )
            .filter(model.Document.idDocument == inv_id)
            .order_by(model.DocumentHistoryLogs.CreatedOn)
            .all()
        )
    except Exception:
        # In case of any error, log it and return a server error response
        logger.error(traceback.format_exc())
        return Response(status_code=500, headers={"Error": "Server Error"})
    finally:
        # Ensure the database session is closed properly
        db.close()


async def read_doc_history(inv_id, download, db):
    """Function to read invoice history logs.

    Parameters:
    ----------
    inv_id : int
        The ID of the invoice whose history is being retrieved.
    download : bool
        A flag to indicate if the request is for a downloadable version of
        the history logs.
    db : Session
        Database session object to interact with the backend.

    Returns:
    -------
    list
        A list of document history logs with associated details such as
        user and vendor info.
    """
    try:
        # If download is requested, fetch detailed information including vendor
        # and document info
        if download:
            return (
                db.query(
                    model.DocumentHistoryLogs,
                    model.Document.docheaderID,
                    model.Document.documentDate,
                    model.Document.JournalNumber,
                    model.Document.UploadDocType,
                    model.Vendor.VendorName,
                    model.User.firstName,
                )
                .options(
                    load_only("documentdescription", "documentStatusID", "CreatedOn")
                )
                .filter(
                    model.DocumentHistoryLogs.documentID == model.Document.idDocument
                )
                .filter(model.DocumentHistoryLogs.userID == model.User.idUser)
                .join(
                    model.VendorAccount,
                    model.Document.vendorAccountID
                    == model.VendorAccount.idVendorAccount,
                    isouter=True,
                )
                .join(
                    model.Vendor,
                    model.VendorAccount.vendorID == model.Vendor.idVendor,
                    isouter=True,
                )
                .filter(model.Document.idDocument == inv_id)
                .order_by(model.DocumentHistoryLogs.CreatedOn)
                .all()
            )
        else:
            # If download is not requested, fetch only the essential history log details
            return (
                db.query(model.DocumentHistoryLogs, model.User.firstName)
                .options(
                    load_only("documentdescription", "documentStatusID", "CreatedOn")
                )
                .filter(
                    model.DocumentHistoryLogs.documentID == model.Document.idDocument
                )
                .filter(model.Document.idDocument == inv_id)
                .join(model.User, model.DocumentHistoryLogs.userID == model.User.idUser)
                .order_by(model.DocumentHistoryLogs.CreatedOn)
                .all()
            )
    except Exception:
        # Log the error and return a server error response
        logger.error(traceback.format_exc())
        return Response(status_code=500, headers={"Error": "Server Error"})
    finally:
        # Ensure that the database session is closed after execution
        db.close()


async def read_document_lock_status(u_id, inv_id, client_ip, db):
    """Function to check the current lock status of a document.

    Parameters:
    ----------
    u_id : int
        The ID of the user requesting the document lock status.
    inv_id : int
        The ID of the document whose lock status is being checked.
    client_ip : object
        The client IP address of the user to compare with the lock's host client.
    db : Session
        Database session object to interact with the backend.

    Returns:
    -------
    dict
        A dictionary with the current lock status, including user and
        host details, or an error message.
    """
    try:
        # get current date time
        current_datetime = datetime.utcnow()
        # get lock info on document
        lock_info = (
            db.query(model.Document.lock_info, model.User)
            .options(Load(model.User).load_only("firstName", "lastName"))
            .filter(model.Document.idDocument == inv_id)
            .join(
                model.User,
                model.User.idUser == model.Document.lock_user_id,
                isouter=True,
            )
            .one()
        )
        # check if lock info is null
        if lock_info[0]:
            lock_datetime = datetime.strptime(
                lock_info[0]["lock_date_time"], "%Y-%m-%d %H:%M:%S"
            )
            # if lock info not null, compare the lock
            # session time, if lesser than current time reset info
            if lock_datetime < current_datetime:
                db.query(model.Document).filter_by(idDocument=inv_id).update(
                    {"lock_info": None, "lock_user_id": None}
                )
                db.commit()
                lock_info[0]["lock_status"] = 0
            # check if the user is the current host
            if lock_info[0]["host_client"] == client_ip.client_address:
                lock_info[0]["lock_status"] = 0
        else:
            lock_info = {
                "lock_info": {
                    "host_client": None,
                    "lock_status": 0,
                    "lock_date_time": "",
                },
                "User": {"firstName": "", "idUser": None, "lastName": ""},
            }
        return {"result": lock_info}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500, headers={"Error": "Server error"})
    finally:
        db.close()


async def update_document_lock_status(u_id, inv_id, session_datetime, db):
    try:
        # Retrieve the current user who has locked the document (if any)
        lock_uid = (
            db.query(model.Document.lock_user_id)
            .filter(model.Document.idDocument == inv_id)
            .scalar()
        )
        # Extract session status and other details from the input dictionary
        session_datetime = dict(session_datetime)
        # If the document is locked by another user, return a forbidden error response
        if lock_uid and lock_uid != u_id:
            return Response(
                status_code=403,
                headers={"ClientError": "check lock status before updating"},
            )
        else:
            # If session is active, update the lock info and set a lock timeout
            if session_datetime["session_status"]:
                session_time = datetime.utcnow()
                session_time = session_time + timedelta(minutes=5)
                session_time = session_time.strftime("%Y-%m-%d %H:%M:%S")
                # Update the document lock information in the database
                db.query(model.Document).filter_by(idDocument=inv_id).update(
                    {
                        "lock_info": {
                            "lock_status": 1,
                            "lock_date_time": session_time,
                            "host_client": session_datetime["client_address"],
                        },
                        "lock_user_id": u_id,
                    }
                )
            else:
                # If session is not active, remove the lock information
                session_time = ""
                db.query(model.Document).filter_by(idDocument=inv_id).update(
                    {"lock_info": None, "lock_user_id": None}
                )
            # Commit the changes to the database
            db.commit()
            # Return the result along with the session's end time
            return {"result": "updated", "session_end_time": session_time}
    except Exception:
        # Log any error that occurs and return a server error response
        logger.error(traceback.format_exc())
        return Response(status_code=500, headers={"Error": "Server error"})
    finally:
        # Ensure that the database session is closed after operation
        db.close()


async def new_get_stamp_data_by_document_id(u_id, inv_id, db):
    """Function to retrieve stamp data fields based on the document ID.

    Parameters:
    ----------
    u_id : int
        User ID of the requestor.
    inv_id : int
        Document ID used to filter the stamp data.
    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    dict or model.StampData
        Returns a dictionary with a message if no data is found, otherwise returns
        the stamp data containing selected fields:
        - DEPTNAME
        - RECEIVING_DATE
        - CONFIRMATION_NUMBER
        - RECEIVER
        - SELECTED_DEPT
        - storenumber
    """
    try:
        # Query to filter records based on the document ID
        stamp_data_records = (
            db.query(model.StampDataValidation)
            .filter(model.StampDataValidation.documentid == inv_id)
            .all()
        )

        # Check if records exists or not
        if not stamp_data_records or (
            len(stamp_data_records) == 1
            and stamp_data_records[0].stamptagname == "Credit Identifier"
        ):
            static_stamp_data_records = [
                {
                    "stamptagname": "SelectedDept",
                    "stampvalue": "",
                    "is_error": 0,
                },
                {
                    "stamptagname": "ReceivingDate",
                    "stampvalue": "",
                    "is_error": 0,
                },
                {
                    "stamptagname": "Receiver",
                    "stampvalue": "",
                    "is_error": 0,
                },
                {
                    "stamptagname": "Department",
                    "stampvalue": "",
                    "is_error": 0,
                },
                {
                    "stamptagname": "StoreType",
                    "stampvalue": "",
                    "is_error": 0,
                },
                {
                    "stamptagname": "StoreNumber",
                    "stampvalue": "",
                    "is_error": 0,
                },
                {
                    "stamptagname": "ConfirmationNumber",
                    "stampvalue": "",
                    "is_error": 0,
                },
            ]
            return {"StampNotFound": {"stamp_data_records": static_stamp_data_records}}

        return {"StampFound": {"stamp_data_records": stamp_data_records}}

    except SQLAlchemyError:
        # Handle any SQLAlchemy-specific error and rollback
        logger.error(traceback.format_exc())
        db.rollback()
        return {"error": f"Database error occurred: {str(traceback.format_exc())}"}

    except Exception:
        # Handle any other general exceptions
        logger.error(traceback.format_exc())
        return Response(status_code=500, headers={"Error": "Internal Server error"})


async def new_update_stamp_data_fields(u_id, inv_id, update_data_list, db):
    """Function to update stamp data fields based on document ID.

    If a record does not exist for a given `stamptagname` and `inv_id`,
    a new record will be inserted.

    :param u_id: User ID of the requestor.
    :param inv_id: Document ID to filter the stamp data for updating or inserting.
    :param update_data_list: List of data to update or insert specific fields.
    :param db: Session to interact with the database.
    :return: List of updated or newly inserted StampData objects.
    """
    dt = datetime.now().strftime("%m-%d-%Y %H:%M:%S")
    updated_records = []
    consolidated_updates = []
    docStatus_id = (
        db.query(model.Document.documentStatusID)
        .filter(model.Document.idDocument == inv_id)
        .scalar()
    )
    try:
        for update_data in update_data_list:
            try:
                # Extract data from the current update dictionary
                stamptagname = update_data.stamptagname
                new_value = update_data.NewValue
                old_value = update_data.OldValue
                skipconfig_ck = update_data.skipconfig_ck
                # Query the database to find the record
                stamp_data = (
                    db.query(model.StampDataValidation)
                    .filter(
                        model.StampDataValidation.documentid == inv_id,
                        model.StampDataValidation.stamptagname == stamptagname,
                    )
                    .first()
                )

                # If no record is found, create a new record (insert operation)
                if not stamp_data:
                    # Create a new StampDataValidation object
                    new_stamp_data = model.StampDataValidation(
                        documentid=inv_id,
                        stamptagname=stamptagname,
                        stampvalue=new_value,
                        is_error=0,
                        skipconfig_ck=skipconfig_ck,
                        IsUpdated=1,
                        OldValue=old_value,
                        UpdatedOn=dt,
                    )

                    # Add the new object to the session for insertion
                    db.add(new_stamp_data)

                    # Track this record as newly inserted
                    updated_records.append(new_stamp_data)

                else:
                    # If record exists, update the values
                    stamp_data.OldValue = old_value
                    stamp_data.stampvalue = new_value
                    stamp_data.is_error = 0
                    stamp_data.skipconfig_ck = skipconfig_ck
                    stamp_data.IsUpdated = 1
                    stamp_data.UpdatedOn = dt

                    # Track this record as updated
                    updated_records.append(stamp_data)

                # Query the Document table for the corresponding document
                document_record = (
                    db.query(model.Document)
                    .filter(model.Document.idDocument == inv_id)
                    .first()
                )

                if document_record:
                    # Update the JournalNumber field if stamptagname
                    #  is 'ConfirmationNumber'
                    if stamptagname == "ConfirmationNumber":
                        document_record.JournalNumber = new_value

                    # Update the StoreNumber field if stamptagname is 'StoreNumber'
                    elif stamptagname == "StoreNumber":
                        document_record.store = new_value

                    # Update the Department field if stamptagname is 'Department'
                    elif stamptagname == "Department":
                        document_record.dept = new_value

                    # Add the updated document to the session for commit
                    db.add(document_record)

                # Consolidate updates for the history log
                consolidated_updates.append(
                    f"Field '{stamptagname}' updated to '{new_value}' from '{old_value}'"
                )
            except SQLAlchemyError:
                logger.error(traceback.format_exc())
                # Catch any SQLAlchemy-specific error during the
                # update/insert of a single record
                updated_records.append(
                    {
                        "stamptagname": stamptagname,
                        "error": "Database error occurred: "
                        + f"{str(traceback.format_exc())}",
                    }
                )
                # Continue to next record without breaking the loop

        # Commit the changes to the database (insert and update)
        db.commit()

        # Log the consolidated updates in document history
        if consolidated_updates:
            try:
                update_docHistory(
                    inv_id,
                    u_id,
                    docStatus_id,
                    "; ".join(consolidated_updates),
                    db,
                )
            except Exception:
                logger.error(traceback.format_exc())

        # Refresh and return the updated or newly inserted records
        for stamp_data in updated_records:
            if isinstance(stamp_data, model.StampDataValidation):
                db.refresh(stamp_data)

    except SQLAlchemyError:
        logger.error(traceback.format_exc())
        # Handle any general SQLAlchemy error and rollback the transaction
        db.rollback()
        return {
            "error": "Transaction failed due to a "
            + f"database error: {str(traceback.format_exc())}"
        }

    except Exception:
        logger.error(traceback.format_exc())
        # Handle any other unexpected errors and rollback
        db.rollback()
        print(traceback.format_exc())
        return Response(status_code=500, headers={"Error": "Internal Server error"})

    return updated_records


def update_docHistory(documentID, userID, documentstatus, documentdesc, db):
    """Function to update the document history by inserting a new record into
    the DocumentHistoryLogs table.

    Parameters:
    ----------
    documentID : int
        The ID of the document for which history is being updated.
    userID : int
        The ID of the user who is making the update.
    documentstatus : int
        The current status of the document being recorded in the history.
    documentdesc : str
        A description or reason for the status change.
    db : Session
        Database session object to interact with the backend.

    Returns:
    -------
    None or dict
        Returns None on success or an error message on failure.
    """
    try:
        # Creating a dictionary to hold the document history data
        docHistory = {}
        docHistory["documentID"] = documentID
        docHistory["userID"] = userID
        docHistory["documentStatusID"] = documentstatus
        docHistory["documentdescription"] = documentdesc
        docHistory["CreatedOn"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        # Insert the new document history log into the database
        db.add(model.DocumentHistoryLogs(**docHistory))
        # Commit the transaction to save the changes
        db.commit()
    except Exception:
        # Log the exception details for debugging
        logger.error(traceback.format_exc())
        # Rollback the transaction in case of an error
        db.rollback()
        # Return a descriptive error message to the caller
        return {"DB error": "Error while inserting document history"}


async def reject_invoice(userID, invoiceID, reason, db):
    """Function to reject an invoice by updating its status and logging the
    change.

    Parameters:
    ----------
    userID : int
        The ID of the user rejecting the invoice.
    invoiceID : int
        The ID of the invoice being rejected.
    reason : str
        The reason provided for rejecting the invoice.
    db : Session
        The database session object used for interacting with the backend.

    Returns:
    -------
    str or dict
        Returns a success message or a dictionary with an error message
        if the operation fails.
    """
    try:
        # Fetching the first name of the user performing the rejection
        first_name = (
            db.query(model.User.firstName).filter(model.User.idUser == userID).scalar()
        )
        # Updating the document's status to rejected
        db.query(model.Document).filter(model.Document.idDocument == invoiceID).update(
            {
                "documentStatusID": 10,
                "documentsubstatusID": 13,
                "documentDescription": reason + " by " + first_name,
            }
        )
        # Commit the changes to the database
        db.commit()
        # Update document history with the new status change
        update_docHistory(invoiceID, userID, 10, reason, db)

        return "success: document status changed to rejected!"

    except Exception:
        # Logging the error and rolling back any changes in case of failure
        logger.error(traceback.format_exc())
        db.rollback()
        return {"DB error": "Error while updating document status"}


async def read_all_doc_inv_list(
    u_id, ven_id, inv_type, stat, db, uni_api_filter, ven_status
):
    """Function to read the full document invoice list without pagination.

    Parameters:
    ----------
    ven_id : int
        The ID of the vendor to filter the invoice documents.
    inv_type : str
        The type of invoice to filter the results.
    stat : Optional[str]
        The status of the invoice for filtering purposes.
    db : Session
        Database session object used to interact with the backend database.
    uni_api_filter : Optional[str]
        A universal filter for API queries.
    ven_status : Optional[str]
        Status of the vendor to filter the results.

    Returns:
    -------
    list
        A list containing the filtered document invoice data.
    """
    try:
        # Mapping document statuses to IDs
        all_status = {
            "posted": 14,
            "rejected": 10,
            "exception": 4,
            "VendorNotOnboarded": 25,
            "VendorUnidentified": 26,
            "Duplicate Invoice": 32,
        }

        # Dictionary to handle different types of invoices (ServiceProvider or Vendor)
        inv_choice = {
            "ser": (
                model.ServiceProvider,
                model.ServiceAccount,
                Load(model.ServiceProvider).load_only("ServiceProviderName"),
                Load(model.ServiceAccount).load_only("Account"),
            ),
            "ven": (
                model.Vendor,
                model.VendorAccount,
                Load(model.Vendor).load_only("VendorName", "Address", "VendorCode"),
                Load(model.VendorAccount).load_only("Account"),
            ),
        }
        # Initial query setup for fetching document, status, and related entities
        data_query = (
            db.query(
                model.Document,
                model.DocumentStatus,
                model.DocumentSubStatus,
                inv_choice[inv_type][0],
                inv_choice[inv_type][1],
            )
            .options(
                Load(model.Document).load_only(
                    "docheaderID",
                    "totalAmount",
                    "documentStatusID",
                    "CreatedOn",
                    "documentsubstatusID",
                    "sender",
                    "JournalNumber",
                    "UploadDocType",
                    "store",
                    "dept",
                    "documentDate",
                    "voucher_id",
                    "mail_row_key",
                ),
                Load(model.DocumentSubStatus).load_only("status"),
                Load(model.DocumentStatus).load_only("status", "description"),
                inv_choice[inv_type][2],
                inv_choice[inv_type][3],
            )
            .join(
                model.DocumentSubStatus,
                model.DocumentSubStatus.idDocumentSubstatus
                == model.Document.documentsubstatusID,
                isouter=True,
            )
            .join(
                model.VendorAccount,
                model.VendorAccount.idVendorAccount == model.Document.vendorAccountID,
                isouter=True,
            )
            .join(
                model.Vendor,
                model.Vendor.idVendor == model.VendorAccount.vendorID,
                isouter=True,
            )
            .join(
                model.DocumentStatus,
                model.DocumentStatus.idDocumentstatus
                == model.Document.documentStatusID,
                isouter=True,
            )
            .filter(
                model.Document.idDocumentType == 3,
                model.Document.vendorAccountID.isnot(None),
            )
        )

        # Filter by vendor ID if provided
        if ven_id:
            sub_query = db.query(model.VendorAccount.idVendorAccount).filter_by(
                vendorID=ven_id
            )
            data_query = data_query.filter(
                model.Document.vendorAccountID.in_(sub_query)
            )

        status_list = []
        if stat:
            # Split the status string by ':' to get a list of statuses
            status_list = stat.split(":")

            # Map status names to IDs
            status_ids = [all_status[s] for s in status_list if s in all_status]
            if status_ids:
                data_query = data_query.filter(
                    model.Document.documentStatusID.in_(status_ids)
                )
        # Filter by vendor status (active or inactive) if provided
        if ven_status:
            if ven_status == "A":
                data_query = data_query.filter(
                    func.jsonb_extract_path_text(
                        model.Vendor.miscellaneous, "VENDOR_STATUS"
                    )
                    == "A"
                )

            elif ven_status == "I":
                data_query = data_query.filter(
                    func.jsonb_extract_path_text(
                        model.Vendor.miscellaneous, "VENDOR_STATUS"
                    )
                    == "I"
                )

        # Function to normalize strings
        # (removes non-alphanumeric characters and converts to lowercase)
        def normalize_string(input_str):
            return func.lower(func.regexp_replace(input_str, r"[^a-zA-Z0-9]", "", "g"))

        # Apply universal API filter across multiple columns if provided
        if uni_api_filter:
            uni_search_param_list = uni_api_filter.split(":")
            for param in uni_search_param_list:
                # Normalize the user input filter
                normalized_filter = re.sub(r"[^a-zA-Z0-9]", "", param.lower())

                # Create a pattern for the search with wildcards
                pattern = f"%{normalized_filter}%"
                # Apply the filter to several columns using OR conditions
                filter_condition = or_(
                    normalize_string(model.Document.docheaderID).ilike(pattern),
                    normalize_string(model.Document.documentDate).ilike(pattern),
                    normalize_string(model.Document.sender).ilike(pattern),
                    cast(model.Document.totalAmount, String).ilike(
                        f"%{uni_api_filter}%"
                    ),
                    func.to_char(model.Document.CreatedOn, "YYYY-MM-DD").ilike(
                        f"%{uni_api_filter}%"
                    ),  # noqa: E501
                    normalize_string(model.Document.JournalNumber).ilike(pattern),
                    normalize_string(model.Document.UploadDocType).ilike(pattern),
                    normalize_string(model.Document.store).ilike(pattern),
                    normalize_string(model.Document.dept).ilike(pattern),
                    normalize_string(model.Document.voucher_id).ilike(pattern),
                    normalize_string(model.Document.mail_row_key).ilike(pattern),
                    normalize_string(model.Vendor.VendorName).ilike(pattern),
                    normalize_string(model.Vendor.Address).ilike(pattern),
                    normalize_string(model.DocumentSubStatus.status).ilike(pattern),
                    normalize_string(model.DocumentStatus.status).ilike(pattern),
                    normalize_string(model.DocumentStatus.description).ilike(pattern),
                    normalize_string(inv_choice[inv_type][1].Account).ilike(pattern),
                    # Check if any related DocumentLineItems.Value matches the filter
                    exists().where(
                        (
                            model.DocumentLineItems.documentID
                            == model.Document.idDocument
                        )
                        & normalize_string(model.DocumentLineItems.Value).ilike(pattern)
                    ),
                )
                data_query = data_query.filter(filter_condition)

        # Get the total count of records
        total_count = data_query.distinct(model.Document.idDocument).count()
        # Retrieve all records sorted by creation date, without pagination
        Documentdata = data_query.order_by(model.Document.CreatedOn.desc()).all()

        return {"ok": {"Documentdata": Documentdata, "TotalCount": total_count}}

    except Exception:
        # Log the error and return a 500 response in case of failure
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def get_get_email_row_associated_files_new(
    u_id, off_limit, uni_api_filter, column_filter, db, sort_column, sort_order
):
    try:
        data_query = db.query(model.QueueTask)
        split_doc_table_alias = aliased(model.SplitDocTab)

        # Count distinct mail_row_key
        total_items = (
            data_query.with_entities(
                func.count(
                    func.distinct(model.QueueTask.request_data["mail_row_key"].astext)
                )
            )
            .filter(
                model.QueueTask.request_data["mail_row_key"].isnot(None)
            )  # Ensure not null
            .scalar()  # Get the scalar result
        )

        # Extract offset and limit for pagination
        try:
            offset, limit = off_limit
            off_val = (offset - 1) * limit
        except (TypeError, ValueError):
            logger.error(
                f"Invalid pagination parameters: {str(traceback.format_exc())}"
            )
            off_val = 0
            limit = 10
        data = []
        # Query to get the latest 10 unique mail_row_keys
        latest_mail_row_keys = (
            db.query(
                model.QueueTask.request_data["mail_row_key"].astext.label(
                    "mail_row_key"
                ),
                func.max(model.QueueTask.created_at).label("latest_created_at"),
                func.count(model.QueueTask.id).label("attachment_count"),
            )
            .filter(
                model.QueueTask.request_data["mail_row_key"].isnot(None)
            )  # Exclude NULL values
            .group_by(
                model.QueueTask.request_data["mail_row_key"].astext
            )  # Group by mail_row_key
            .order_by(
                desc(func.max(model.QueueTask.created_at))
            )  # Order by the latest created_at
            .offset(off_val)
            .limit(limit)
            .all()
        )

        for row in latest_mail_row_keys:
            data_to_insert = {
                "mail_number": row.mail_row_key,
                "attachment_count": row.attachment_count,
                "created_at": row.latest_created_at,
                "attachment": [],
            }
            # Query to get the related attachments for each mail_row_key
            related_attachments = (
                db.query(model.SplitDocTab)
                .filter(
                    model.SplitDocTab.mail_row_key == row.mail_row_key,
                )
                .all()
            )
            for attachment in related_attachments:
                attachment_dict = attachment.__dict__
                attachment_dict.pop("_sa_instance_state")
                attachment_dict["file_path"] = attachment_dict["invoice_path"]
                attachment_dict.pop("invoice_path")
                attachment_dict["type"] = attachment_dict["file_path"].split(".")[-1]
                attachment_dict["total_page_count"] = attachment_dict["totalpagecount"]
                attachment_dict.pop("totalpagecount")

                associated_invoices = (
                    db.query(model.frtrigger_tab)
                    .filter(
                        model.frtrigger_tab.splitdoc_id == attachment.splitdoc_id,
                    )
                    .all()
                )

                # remove unnecessary fields
                attachment_dict.pop("splitdoc_id")
                attachment_dict.pop("vendortype")
                # attachment_dict.pop('email_subject')
                attachment_dict.pop("emailbody_path")
                # attachment_dict.pop('sender')
                attachment_dict.pop("mail_row_key")

                attachment_dict["associated_invoice_file"] = []
                for invoice in associated_invoices:
                    invoice_dict = invoice.__dict__
                    invoice_dict.pop("_sa_instance_state")
                    invoice_dict["filepath"] = invoice_dict["blobpath"]
                    invoice_dict.pop("blobpath")
                    invoice_dict["file_size"] = invoice_dict["filesize"]
                    invoice_dict.pop("filesize")
                    invoice_dict["type"] = invoice_dict["filepath"].split(".")[-1]
                    invoice_dict["vendor_id"] = invoice_dict["vendorID"]
                    invoice_dict.pop("vendorID")
                    invoice_dict["document_id"] = invoice_dict["documentid"]
                    invoice_dict.pop("documentid")

                    # remove unnecessary fields
                    invoice_dict.pop("splitdoc_id")
                    invoice_dict.pop("prebuilt_linedata")
                    invoice_dict.pop("pagecount")
                    invoice_dict.pop("frtrigger_id")
                    invoice_dict.pop("prebuilt_headerdata")
                    invoice_dict.pop("sender")

                    attachment_dict["associated_invoice_file"].append(invoice_dict)
                data_to_insert["attachment"].append(attachment_dict)
            
            queue_task = (
                db.query(model.QueueTask)
                .filter(
                    model.QueueTask.request_data["mail_row_key"]
                    == data_to_insert["mail_number"]
                )
                .first()
            )
            data_to_insert["email_path"] = queue_task.request_data["email_path"]
            data_to_insert["sender"] = queue_task.request_data["sender"]
            data_to_insert["email_subject"] = queue_task.request_data["subject"]
            # if len(data_to_insert["attachment"]):
                # data_to_insert["email_path"] = (
                #     "/".join(data_to_insert["attachment"][0]["file_path"].split("/")[:8])
                #     + ".eml"
                # )
                # data_to_insert["sender"] = data_to_insert["attachment"][0]["sender"]
                # data_to_insert["email_subject"] = data_to_insert["attachment"][0][
                #     "email_subject"
                # ]
            data_to_insert["overall_page_count"] = sum(
                [
                    attachment["total_page_count"] or 0
                    for attachment in data_to_insert["attachment"]
                ]
            )
            # else:
                
            #     data_to_insert["overall_page_count"] = 0
            # if related_attachments is zero then queued ,if the status of any of the attachment is not queued then it is in progress , if all the attachment's status is completed then it is completed and if the status of any of associated invoice is Error then it is in error

            if any(
                [
                    invoice["status"] == "Error"
                    for attachment in data_to_insert["attachment"]
                    for invoice in attachment["associated_invoice_file"]
                ]
            ):
                data_to_insert["status"] = "Error"
            elif all(
                [
                    attachment["status"] == "Processed-completed"
                    for attachment in data_to_insert["attachment"]
                ]
            ):
                data_to_insert["status"] = "Completed"
            elif len(data_to_insert["attachment"]):
                data_to_insert["status"] = "In Progress"
            elif len(data_to_insert["attachment"]) == 0:
                data_to_insert["status"] = "Queued"
            data.append(data_to_insert)
        return {"data": data, "total_items": total_items}

    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)


async def get_email_row_associated_files(
    u_id, off_limit, uni_api_filter, column_filter, db, sort_column, sort_order
):
    """Function to retrieve SplitDocTab data for a specific splitdoc_id with
    exception handling, grouping by mail_number if multiple entries exist.

    Args:
        u_id: User ID
        db: SQLAlchemy session
        off_limit: Tuple of offset and limit values for pagination.

    Returns:
        result: A dictionary containing grouped data from SplitDocTab rows.
    """
    try:
        base_query = db.query(model.SplitDocTab).filter(
            model.SplitDocTab.mail_row_key != "NULL"
        )
        data_query = base_query

        # Helper function for case-insensitive, alphanumeric normalization
        def normalize_string(input_str):
            return func.lower(func.regexp_replace(input_str, r"[^a-zA-Z0-9]", "", "g"))

        # Apply universal API filter if provided
        if uni_api_filter:
            try:
                # Split terms in the filter
                uni_search_param_list = [
                    param.strip() for param in uni_api_filter.split(":")
                ]

                # Define separate lists for date and non-date terms
                date_filters = []
                text_filters = []

                for term in uni_search_param_list:
                    # Clean term by removing unwanted characters
                    term = re.sub(r"[^a-zA-Z0-9 ,]", "", term)

                    # Attempt to parse the term as a date (e.g., "Oct 30, 2024")
                    try:
                        date_obj = datetime.strptime(term, "%b %d, %Y")
                        # Create date filter to match any time within that day
                        start_date = date_obj.strftime("%Y-%m-%d 00:00:00")
                        end_date = date_obj.strftime("%Y-%m-%d 23:59:59")
                        date_filters.append(
                            model.SplitDocTab.updated_on.between(start_date, end_date)
                        )
                    except ValueError:
                        # If not a date, treat it as a general search term
                        pattern = f"%{term}%"
                        text_filter = or_(
                            normalize_string(model.SplitDocTab.emailbody_path).ilike(
                                pattern
                            ),
                            normalize_string(model.SplitDocTab.sender).ilike(pattern),
                            cast(model.SplitDocTab.totalpagecount, String).ilike(
                                pattern
                            ),
                            normalize_string(model.SplitDocTab.mail_row_key).ilike(
                                pattern
                            ),
                            normalize_string(model.SplitDocTab.status).ilike(pattern),
                            normalize_string(model.SplitDocTab.email_subject).ilike(
                                pattern
                            ),
                        )
                        text_filters.append(text_filter)

                # Combine text and date filters with AND condition
                if date_filters or text_filters:
                    data_query = data_query.filter(and_(*date_filters, *text_filters))

            except (AttributeError, TypeError, ValueError):
                logger.error(
                    f"Error processing universal API filter: {str(traceback.format_exc())}"  # noqa: E501
                )
        # Apply column-specific filter if provided
        if column_filter:
            try:
                if ":" in column_filter:
                    column_name, search_value = column_filter.split(":", 1)
                    column_name = column_name.strip().lower()
                    search_value = search_value.strip()

                    # Prepare normalized search value for use in ilike
                    normalized_search_value = re.sub(
                        r"[^a-zA-Z0-9]", "", search_value.lower()
                    )

                    if column_name in ["total page count", "totalpagecount"]:
                        data_query = data_query.filter(
                            cast(model.SplitDocTab.totalpagecount, String).ilike(
                                f"%{search_value}%"
                            )
                        )
                    elif column_name == "sender":
                        data_query = data_query.filter(
                            normalize_string(model.SplitDocTab.sender).ilike(
                                f"%{search_value}%"
                            )
                        )
                    elif column_name == "created on":
                        # Convert input date to the appropriate format
                        try:
                            # Parse the input date
                            parsed_date = datetime.strptime(search_value, "%b %d, %Y")
                            # Format the parsed date to match the DB format
                            formatted_date = parsed_date.strftime("%Y-%m-%d")

                            data_query = data_query.filter(
                                func.to_char(
                                    model.SplitDocTab.updated_on, "YYYY-MM-DD"
                                ).ilike(f"%{formatted_date}%")
                            )
                        except ValueError as ve:
                            logger.error(f"Date parsing error: {str(ve)}")
                    elif column_name == "mail row key":
                        # Adjust filter to match with or without hyphens
                        data_query = data_query.filter(
                            or_(
                                normalize_string(model.SplitDocTab.mail_row_key).ilike(
                                    f"%{normalized_search_value}%"
                                ),
                                model.SplitDocTab.mail_row_key.ilike(
                                    f"%{search_value}%"
                                ),
                            )
                        )
                    elif column_name == "status":
                        data_query = data_query.filter(
                            normalize_string(model.SplitDocTab.status).ilike(
                                f"%{search_value}%"
                            )
                        )
                    elif column_name == "email subject":
                        # Normalize and use ilike for email_subject filtering
                        data_query = data_query.filter(
                            model.SplitDocTab.email_subject.ilike(f"%{search_value}%")
                        )
            except (AttributeError, TypeError, ValueError):
                logger.error(
                    f"Error processing column filter: {str(traceback.format_exc())}"
                )

        # # Alias for the SplitDocTab model
        SplitDocTabAlias = aliased(model.SplitDocTab)
        # Sorting logic
        sort_columns_map = {
            "Sender": SplitDocTabAlias.sender,
            "Mail Row Key": SplitDocTabAlias.mail_row_key,
            "Created On": SplitDocTabAlias.updated_on,
            "Email Subject": SplitDocTabAlias.email_subject,
        }
        if sort_column and sort_column in sort_columns_map:
            column_to_sort = sort_columns_map[sort_column]
            order_by_clause = (
                column_to_sort.desc()
                if sort_order.lower() == "desc"
                else column_to_sort.asc()
            )
        else:
            order_by_clause = SplitDocTabAlias.splitdoc_id.desc()  # Default sorting

        # Extract offset and limit for pagination
        try:
            offset, limit = off_limit
            off_val = (offset - 1) * limit
        except (TypeError, ValueError):
            logger.error(
                f"Invalid pagination parameters: {str(traceback.format_exc())}"
            )
            off_val = 0
            limit = 10

        # Main query to retrieve all records with the applied filters and pagination
        # Count total unique mail_row_keys
        total_items = (
            data_query.with_entities(model.SplitDocTab.mail_row_key).distinct().count()
        )

        # Subquery to get the latest splitdoc_id per mail_row_key, with filters applied
        latest_splitdoc_subquery = (
            data_query.with_entities(  # Start with the filtered base query
                model.SplitDocTab.mail_row_key,
                func.max(model.SplitDocTab.splitdoc_id).label("latest_splitdoc_id"),
            )
            .group_by(model.SplitDocTab.mail_row_key)
            .subquery()
        )

        # # Main query to get latest unique mail_row_keys with pagination
        # unique_mail_keys_query = (
        #     db.query(SplitDocTabAlias.mail_row_key)
        #     .join(
        #         latest_splitdoc_subquery,
        #         latest_splitdoc_subquery.c.latest_splitdoc_id
        #         == SplitDocTabAlias.splitdoc_id,
        #     )
        #     .order_by(SplitDocTabAlias.splitdoc_id.desc())
        #     .offset(off_val)
        #     .limit(limit)
        # )

        # Main query to get latest unique mail_row_keys with sorting and pagination
        unique_mail_keys_query = (
            db.query(SplitDocTabAlias.mail_row_key)
            .join(
                latest_splitdoc_subquery,
                latest_splitdoc_subquery.c.latest_splitdoc_id
                == SplitDocTabAlias.splitdoc_id,
            )
            .order_by(order_by_clause)
            .offset(off_val)
            .limit(limit)
        )

        unique_mail_keys = unique_mail_keys_query.all()
        # Step 2: Retrieve all splitdoc_ids for the selected unique mail_row_keys
        if unique_mail_keys:
            unique_mail_keys_list = [key[0] for key in unique_mail_keys]

            all_split_docs_query = data_query.filter(
                model.SplitDocTab.mail_row_key.in_(unique_mail_keys_list)
            ).order_by(model.SplitDocTab.splitdoc_id.desc())

            unique_split_docs = all_split_docs_query.all()

        else:
            unique_split_docs = []

        grouped_mail_data = {}

        # Process each split_doc entry
        for split_doc in unique_split_docs:
            base_eml_path = split_doc.invoice_path.rsplit("/", 1)[0] + ".eml"
            mail_number = split_doc.mail_row_key

            # Define a new mail data structure if mail_number is
            # not in grouped_mail_data
            if mail_number not in grouped_mail_data:
                mail_data = {
                    "mail_number": mail_number,
                    "email_path": base_eml_path,
                    "sender": split_doc.sender,
                    "email_subject": split_doc.email_subject,
                    "attachment_count": 0,
                    "overall_page_count": 0,
                    "attachment": [],
                }
                grouped_mail_data[mail_number] = mail_data
            else:
                mail_data = grouped_mail_data[mail_number]

            # Retrieve fr_trigger_tab entries for each split_doc
            try:
                fr_trigger_tab = (
                    db.query(model.frtrigger_tab)
                    .filter(model.frtrigger_tab.splitdoc_id == split_doc.splitdoc_id)
                    .all()
                )
            except SQLAlchemyError:
                logger.error(
                    f"Database error in fr_trigger_tab query: {str(traceback.format_exc())}"  # noqa: E501
                )
                fr_trigger_tab = []

            mail_data["attachment_count"] += 1
            mail_data["overall_page_count"] += split_doc.totalpagecount or 0

            # Build attachment data
            file_extension = split_doc.invoice_path.split(".")[-1].lower()
            file_type = (
                file_extension
                if file_extension in ["pdf", "jpg", "png", "eml"]
                else "unknown"
            )
            child = {
                "file_path": split_doc.invoice_path,
                "type": file_type,
                "total_page_count": split_doc.totalpagecount or 0,
                "pages_processed": split_doc.pages_processed,
                "created_on": split_doc.updated_on,
                "status": split_doc.status,
                "associated_invoice_file": [],
            }

            for fr in fr_trigger_tab:
                associated_invoice_files = {
                    "filepath": fr.blobpath,
                    "type": file_extension,
                    "document_id": fr.documentid,
                    "status": fr.status,
                    "file_size": fr.filesize,
                    "vendor_id": fr.vendorID,
                    "page_number": fr.page_number,
                }
                child["associated_invoice_file"].append(associated_invoice_files)

            # Append each distinct attachment as a separate entry
            mail_data["attachment"].append(child)

        return {"total_items": total_items, "data": list(grouped_mail_data.values())}

    except Exception:
        logger.error(f"An unexpected error occurred: {str(traceback.format_exc())}")
        return {"total_items": 0, "data": []}  # Default response on unexpected error


async def readdeptname(db):
    """This function read list of Department name from database.

    It contains 2 parameter.
    :param u_id: The user ID for which to fetch department data.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        # Query to get vendor names and filter by VENDOR_STATUS
        # query = db.query(model.PFGDepartment.DESCRSHORT)
        query = db.query(model.PFGDepartment.DESCRSHORT, model.PFGDepartment.DEPTID)
        data = query.all()
        return data

    except Exception:
        logger.error(traceback.format_exc())
        return Response(
            status_code=500, headers={"Error": "Server error", "Desc": "Invalid result"}
        )
    finally:
        db.close()


async def upsert_line_items(u_id, inv_id, inv_data, db):
    """Upserts (updates or inserts) line items for a given document ID.

    Args:
        db (Session): SQLAlchemy database session.
        document_id (int): ID of the document.
        line_data (list): List of dictionaries containing line item data.
            Each item should have keys:
                - "documentLineItemID" (int)
                - "line_item_tag_id" (int)
                - "item_code" (str)
                - "NewValue" (str)

    Returns:
        dict: Result containing 'inserted' and 'updated' counts.
    """
    inserted_count = 0
    updated_count = 0

    try:
        # avoid data updates by other users if in lock
        dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        docStatus_id = (
            db.query(model.Document.documentStatusID)
            .filter(model.Document.idDocument == inv_id)
            .scalar()
        )
        consolidated_updates = []
        for row in inv_data:
            if row.documentLineItemID:
                try:
                    # Check if the line item exists for this tag ID and document ID
                    db.query(model.DocumentLineItems).filter_by(
                        idDocumentLineItems=row.documentLineItemID,
                        documentID=inv_id,
                    ).scalar()
                except Exception:
                    logger.error(traceback.format_exc())
                    return Response(
                        status_code=403,
                        headers={"ClientError": "invoice and line value mismatch"},
                    )
                inv_up_line_id = (
                    db.query(model.DocumentUpdates.idDocumentUpdates)
                    .filter_by(documentLineItemID=row.documentLineItemID)
                    .all()
                )
                if len(inv_up_line_id) > 0:
                    db.query(model.DocumentUpdates).filter_by(
                        documentLineItemID=row.documentLineItemID, IsActive=1
                    ).update({"IsActive": 0})
                    db.flush()
                # Prepare data for new update
                update_data = {
                    "documentLineItemID": row.documentLineItemID,
                    "NewValue": row.NewValue,
                    "IsActive": 1,
                    "UpdatedOn": dt,
                }
                new_update = model.DocumentUpdates(**update_data)
                db.add(new_update)
                db.flush()

                # Add to consolidated history log
                consolidated_updates.append(
                    f"Line Item ID {row.documentLineItemID}: Updated from {row.OldValue} to {row.NewValue}"
                )

                # Update DocumentLineItems for line item updates
                db.query(model.DocumentLineItems).filter_by(
                    idDocumentLineItems=row.documentLineItemID
                ).update({"IsUpdated": 1, "isError": 0, "Value": row.NewValue})

                updated_count += 1
            else:
                # Insert a new line item
                new_line = model.DocumentLineItems(
                    documentID=inv_id,
                    lineItemtagID=row.lineItemTagID,
                    Value=row.NewValue,
                    isError=0,
                    itemCode=row.itemCode,
                    invoice_itemcode=row.itemCode,
                    IsUpdated=0,
                    CreatedOn=dt,
                )
                db.add(new_line)
                inserted_count += 1

        # Add consolidated history log
        if consolidated_updates:
            try:
                update_docHistory(
                    inv_id,
                    u_id,
                    docStatus_id,
                    "; ".join(consolidated_updates),
                    db,
                )
            except Exception:
                logger.error(traceback.format_exc())
        # Commit the changes
        db.commit()
        return {
            "inserted": inserted_count,
            "updated": updated_count,
        }

    except Exception as e:
        logger.error(traceback.format_exc())
        db.rollback()
        return Response(status_code=500, headers={"Error": "Server error"})

    finally:
        db.close()


async def delete_line_items(u_id, inv_id, line_item_objects, db):
    """Deletes one or more line items for a given invoice ID.

    Args:
        inv_id (int): ID of the invoice.
        line_item_objects (list): List of objects containing line item IDs to delete.
        Each object should have an attribute `documentLineItemID`.
        db (Session): SQLAlchemy database session.

    Returns:
        dict: Result containing 'deleted_count' or an error message.
    """
    deleted_count = 0
    history_log = []
    try:
        docStatus_id = (
            db.query(model.Document.documentStatusID)
            .filter(model.Document.idDocument == inv_id)
            .scalar()
        )
        # Extract IDs from the objects
        line_item_ids = [obj.documentLineItemID for obj in line_item_objects]

        if not line_item_ids:
            return {"error": "No valid line item IDs provided."}

        # Delete related records in documentupdates first
        db.query(model.DocumentUpdates).filter(
            model.DocumentUpdates.documentLineItemID.in_(line_item_ids)
        ).delete(synchronize_session=False)

        # Fetch and delete line items
        line_items_to_delete = (
            db.query(model.DocumentLineItems)
            .filter(
                model.DocumentLineItems.idDocumentLineItems.in_(line_item_ids),
                model.DocumentLineItems.documentID == inv_id,
            )
            .all()
        )

        if not line_items_to_delete:
            return {
                "error": "No matching line items found for the provided invoice ID."
            }
        # Collect details for history logging
        for line_item in line_items_to_delete:
            history_log.append(
                f"Deleted line item with ID: {line_item.idDocumentLineItems}, Value: {line_item.Value}"
            )
        deleted_count = len(line_items_to_delete)
        for line_item in line_items_to_delete:
            db.delete(line_item)

        # Log the consolidated history
        try:
            consolidated_message = "; ".join(history_log)
            update_docHistory(
                inv_id,
                u_id,
                docStatus_id,
                "; ".join(consolidated_message),
                db,
            )
        except Exception:
            logger.error("Failed to update history log.")

        # Commit the deletion
        db.commit()

        return {"deleted_count": deleted_count}

    except Exception as e:
        logger.error(traceback.format_exc())
        db.rollback()
        return {"error": "An error occurred while deleting line items."}

    finally:
        db.close()


async def update_credit_identifier_to_stamp_data(u_id, inv_id, update_data, db):
    """Function to update or insert a single stamp data record based on
    document ID.

    :param u_id: User ID of the requestor.
    :param inv_id: Document ID to filter the stamp data for updating or inserting.
    :param update_data: Data object containing `stamptagname`, `NewValue`, and `OldValue`.
    :param db: Session to interact with the database.
    :return: Updated or newly inserted StampDataValidation object or error details.
    """
    dt = datetime.now().strftime("%m-%d-%Y %H:%M:%S")
    docStatus_id = (
        db.query(model.Document.documentStatusID)
        .filter(model.Document.idDocument == inv_id)
        .scalar()
    )
    try:
        # Extract data from the update_data object
        stamptagname = update_data.stamptagname
        new_value = update_data.NewValue
        old_value = update_data.OldValue
        skipconfig_ck = update_data.skipconfig_ck
        # Query the database for an existing record
        stamp_data = (
            db.query(model.StampDataValidation)
            .filter(
                model.StampDataValidation.documentid == inv_id,
                model.StampDataValidation.stamptagname == stamptagname,
            )
            .first()
        )

        if not stamp_data:
            # If no record exists, insert a new record
            new_stamp_data = model.StampDataValidation(
                documentid=inv_id,
                stamptagname=stamptagname,
                stampvalue=new_value,
                is_error=0,
                skipconfig_ck=skipconfig_ck,
                IsUpdated=1,
                OldValue=old_value,
                UpdatedOn=dt,
            )
            db.add(new_stamp_data)  # Add to session for insertion
            db.commit()  # Commit the changes
            db.refresh(new_stamp_data)  # Refresh to get updated instance
            dmsg = f"Field '{stamptagname}' added with value '{new_value}'"
            # Log the consolidated updates in document history
            try:
                update_docHistory(
                    inv_id,
                    u_id,
                    docStatus_id,
                    dmsg,
                    db,
                )
            except Exception:
                logger.error(traceback.format_exc())
            return new_stamp_data
        else:
            # If the record exists, update it
            stamp_data.OldValue = old_value
            stamp_data.stampvalue = new_value
            stamp_data.is_error = 0
            stamp_data.skipconfig_ck = skipconfig_ck
            stamp_data.IsUpdated = 1
            stamp_data.UpdatedOn = dt

            db.commit()  # Commit the changes
            db.refresh(stamp_data)  # Refresh to get updated instance
            dmsg = f"Field '{stamptagname}' updated to '{new_value}' from '{old_value}'"
            try:
                update_docHistory(
                    inv_id,
                    u_id,
                    docStatus_id,
                    dmsg,
                    db,
                )
            except Exception:
                logger.error(traceback.format_exc())
            return stamp_data

    except SQLAlchemyError as e:
        logger.error(traceback.format_exc())
        return {
            "stamptagname": stamptagname,
            "error": f"Database error occurred: {str(e)}",
        }


async def get_voucher_data_by_document_id(u_id, document_id, db):
    """Retrieve voucher data filtered by documentID.

    Parameters:
    -----------
    user_id : int
        ID of the user requesting the data.
    document_id : int
        Document ID to filter the voucher data.
    db : Session
        Database session object.

    Returns:
    --------
    List[dict]
        List of voucher data rows matching the given document ID.
    """
    try:
        # Check if the document ID exists
        exists = (
            db.query(model.VoucherData)
            .filter(model.VoucherData.documentID == document_id)
            .first()
        )

        if not exists:
            # Return message if documentID does not exist
            return {
                "error": f"Document ID: {document_id} does not exist in the voucherdata table."
            }

        # Query to retrieve all data matching the documentID
        results = (
            db.query(model.VoucherData)
            .filter(model.VoucherData.documentID == document_id)
            .all()
        )

        # Format and return the result
        return [
            {
                "voucherdataID": row.voucherdataID,
                "documentID": row.documentID,
                "Business_unit": row.Business_unit,
                "Invoice_Id": row.Invoice_Id,
                "Invoice_Dt": row.Invoice_Dt,
                "Vendor_Setid": row.Vendor_Setid,
                "Vendor_ID": row.Vendor_ID,
                "Origin": row.Origin,
                "Gross_Amt": row.Gross_Amt,
                "Voucher_Line_num": row.Voucher_Line_num,
                "Merchandise_Amt": row.Merchandise_Amt,
                "Distrib_Line_num": row.Distrib_Line_num,
                "Account": row.Account,
                "Deptid": row.Deptid,
                "Image_Nbr": row.Image_Nbr,
                "File_Name": row.File_Name,
                "storenumber": row.storenumber,
                "storetype": row.storetype,
                "receiver_id": row.receiver_id,
                "status": row.status,
                "recv_ln_nbr": row.recv_ln_nbr,
                "gst_amt": row.gst_amt,
                "currency_code": row.currency_code,
                "freight_amt": row.freight_amt,
                "misc_amt": row.misc_amt,
            }
            for row in results
        ]
    except Exception as e:
        # Log and handle exceptions
        print(f"Error while fetching voucher data: {e}")
        return {"error": "An error occurred while fetching the data."}

# from sqlalchemy.orm import
import base64
import os
import traceback
from datetime import datetime, timedelta

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from fastapi.responses import Response
from sqlalchemy import String, and_, case, cast, func, or_
from sqlalchemy.orm import Load, load_only

credential = DefaultAzureCredential()
import re

from sqlalchemy.exc import SQLAlchemyError

import pfg_app.model as model
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

# Sql case statement for re placing number with string status


doc_status = case(
    *(
        (model.Document.documentsubstatusID == value[0], value[1])
        for value in substatus
    ),
    *(
        (model.Document.documentStatusID == value[0] + 1, value[1])
        for value in enumerate(status)
    ),
    else_="",
).label("docstatus")


async def read_doc_inv_list(u_id, ven_id, inv_type, stat, db):
    """This function reads document invoice list, contains following parameters
    :param u_id: It is a function parameters that is of integer type, it
    provides the user Id.

    :param sp_id: It is a function parameters that is of integer type,
        it provides the service provider Id.
    :param ven_id: It is a function parameters that is of integer type,
        it provides the vendor Id.
    :param usertype: It is a function parameters that is of integer
        type, it provides the user type.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of list type.
    """
    try:
        all_status = {"posted": 14, "rejected": 10, "exception": 4}
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
                Load(model.Vendor).load_only("VendorName", "Address"),
                Load(model.VendorAccount).load_only("Account"),
            ),
        }
        data = db.query(
            model.Document,
            doc_status,
            model.DocumentSubStatus,
            inv_choice[inv_type][0],
            inv_choice[inv_type][1],
        ).filter(model.Document.idDocumentType == 3)
        # filters for query parameters
        if ven_id:
            sub_query = db.query(model.VendorAccount.idVendorAccount).filter_by(
                vendorID=ven_id
            )
            data = data.filter(model.Document.vendorAccountID.in_(sub_query))
        # filter by status
        if stat:
            data = data.filter(model.Document.documentStatusID == all_status[stat])
        # Fetching Document data
        Documentdata = (
            data.options(
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
                ),
                Load(model.DocumentSubStatus).load_only("status"),
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
                model.DocumentHistoryLogs,
                model.DocumentHistoryLogs.documentID == model.Document.idDocument,
                isouter=True,
            )
        )

        Documentdata = (
            Documentdata.join(
                model.VendorAccount,
                model.VendorAccount.idVendorAccount == model.Document.vendorAccountID,
                isouter=True,
            )
            .join(
                model.Vendor,
                model.Vendor.idVendor == model.VendorAccount.vendorID,
                isouter=True,
            )
            .filter(model.Document.vendorAccountID.isnot(None))
        )
        return {
            "ok": {
                "Documentdata": Documentdata.order_by(
                    model.Document.CreatedOn.desc()
                ).all(),
                "Count": len(Documentdata.all()),
            }
        }

    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def read_paginate_doc_inv_list(
    u_id, ven_id, inv_type, stat, off_limit, db, uni_api_filter, ven_status
):
    """This function reads document invoice list, contains following parameters
    :param u_id: It is a function parameters that is of integer type, it
    provides the user Id.

    :param sp_id: It is a function parameters that is of integer type,
        it provides the service provider Id.
    :param ven_id: It is a function parameters that is of integer type,
        it provides the vendor Id.
    :param usertype: It is a function parameters that is of integer
        type, it provides the user type.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of list type.
    """
    try:
        all_status = {
            "posted": 14,
            "rejected": 10,
            "exception": 4,
            "VendorNotOnboarded": 25,
            "VendorUnidentified": 26,
        }

        doc_status = case(
            [
                (model.Document.documentsubstatusID == value[0], value[1])
                for value in substatus
            ]
            + [
                (model.Document.documentStatusID == value[0] + 1, value[1])
                for value in enumerate(status)
            ]
            + [
                (
                    model.Document.documentStatusID == all_status["VendorUnidentified"],
                    "VendorUnidentified",
                ),
                (
                    model.Document.documentStatusID == all_status["VendorNotOnboarded"],
                    "VendorNotOnboarded",
                ),
            ],
            else_="",
        ).label("docstatus")

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
                Load(model.Vendor).load_only("VendorName", "Address"),
                Load(model.VendorAccount).load_only("Account"),
            ),
        }
        # Initial query setup
        data_query = db.query(
            model.Document,
            doc_status,
            model.DocumentSubStatus,
            inv_choice[inv_type][0],
            inv_choice[inv_type][1],
        ).filter(model.Document.idDocumentType == 3)

        # filters for query parameters
        if ven_id:
            sub_query = db.query(model.VendorAccount.idVendorAccount).filter_by(
                vendorID=ven_id
            )
            data_query = data_query.filter(
                model.Document.vendorAccountID.in_(sub_query)
            )
        # filter by status
        if stat:
            data_query = data_query.filter(
                model.Document.documentStatusID == all_status[stat]
            )

        # Apply necessary joins and options
        data_query = (
            data_query.options(
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
                ),
                Load(model.DocumentSubStatus).load_only("status"),
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
                model.DocumentHistoryLogs,
                model.DocumentHistoryLogs.documentID == model.Document.idDocument,
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
            .filter(model.Document.vendorAccountID.isnot(None))
        )

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

        # Pagination
        offset, limit = off_limit
        off_val = (offset - 1) * limit
        if off_val < 0:
            return Response(
                status_code=403,
                headers={"ClientError": "Please provide a valid offset value."},
            )

        if uni_api_filter:
            data_query = data_query.filter(
                (model.Document.docheaderID.ilike(f"%{uni_api_filter}%"))
                | (model.Document.documentDate.ilike(f"%{uni_api_filter}%"))
                | (model.Document.sender.ilike(f"%{uni_api_filter}%"))
                | (
                    cast(model.Document.totalAmount, String).ilike(
                        f"%{uni_api_filter}%"
                    )
                )  # Use a numeric comparison for `totalAmount` instead of `LIKE`
                | (
                    func.to_char(
                        model.Document.CreatedOn, "YYYY-MM-DD HH24:MI:SS"
                    ).ilike(f"%{uni_api_filter}%")
                )
                | (model.Document.JournalNumber.ilike(f"%{uni_api_filter}%"))
                | (model.Document.UploadDocType.ilike(f"%{uni_api_filter}%"))
                | (model.Document.store.ilike(f"%{uni_api_filter}%"))
                | (model.Document.dept.ilike(f"%{uni_api_filter}%"))
                | (model.Vendor.VendorName.ilike(f"%{uni_api_filter}%"))
                | (model.Vendor.Address.ilike(f"%{uni_api_filter}%"))
                | (model.DocumentSubStatus.status.ilike(f"%{uni_api_filter}%"))
                | (inv_choice[inv_type][1].Account.ilike(f"%{uni_api_filter}%"))
            )

        # Get the total count of records before applying limit and offset
        total_count = data_query.distinct(model.Document.idDocument).count()
        # Apply limit and offset for pagination
        Documentdata = (
            data_query.order_by(model.Document.CreatedOn.desc())
            .limit(limit)
            .offset(off_val)
            .all()
        )

        return {"ok": {"Documentdata": Documentdata, "TotalCount": total_count}}

    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def read_paginate_doc_inv_list_with_ln_items(
    ven_id, inv_type, stat, off_limit, db, uni_api_filter, ven_status
):
    """This function reads document invoice list with pagination and
    filtering."""
    try:
        all_status = {
            "posted": 14,
            "rejected": 10,
            "exception": 4,
            "VendorNotOnboarded": 25,
            "VendorUnidentified": 26,
        }

        # Build doc_status case statement for labeling
        doc_status = case(
            [
                (model.Document.documentsubstatusID == value[0], value[1])
                for value in substatus
            ]
            + [
                (model.Document.documentStatusID == value[0] + 1, value[1])
                for value in enumerate(status)
            ]
            + [
                (
                    model.Document.documentStatusID == all_status["VendorUnidentified"],
                    "VendorUnidentified",
                ),
                (
                    model.Document.documentStatusID == all_status["VendorNotOnboarded"],
                    "VendorNotOnboarded",
                ),
            ],
            else_="",
        ).label("docstatus")

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
                Load(model.Vendor).load_only("VendorName", "Address"),
                Load(model.VendorAccount).load_only("Account"),
            ),
        }

        # Initial query setup for documents
        data_query = db.query(
            model.Document,
            doc_status,
            model.DocumentSubStatus,
            inv_choice[inv_type][0],
            inv_choice[inv_type][1],
        ).filter(model.Document.idDocumentType == 3)

        # Apply vendor ID filter if provided
        if ven_id:
            sub_query = db.query(model.VendorAccount.idVendorAccount).filter_by(
                vendorID=ven_id
            )
            data_query = data_query.filter(
                model.Document.vendorAccountID.in_(sub_query)
            )

        # Filter by status if provided
        if stat:
            data_query = data_query.filter(
                model.Document.documentStatusID == all_status[stat]
            )

        # Apply necessary joins and options for fetching document data
        data_query = (
            data_query.options(
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
                ),
                Load(model.DocumentSubStatus).load_only("status"),
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
                model.DocumentHistoryLogs,
                model.DocumentHistoryLogs.documentID == model.Document.idDocument,
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
            .filter(model.Document.vendorAccountID.isnot(None))
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

        # Apply universal API filter if provided, including line items
        if uni_api_filter:
            filter_condition = or_(
                model.Document.docheaderID.ilike(f"%{uni_api_filter}%"),
                model.Document.documentDate.ilike(f"%{uni_api_filter}%"),
                model.Document.sender.ilike(f"%{uni_api_filter}%"),
                cast(model.Document.totalAmount, String).ilike(f"%{uni_api_filter}%"),
                func.to_char(model.Document.CreatedOn, "YYYY-MM-DD HH24:MI:SS").ilike(
                    f"%{uni_api_filter}%"
                ),
                model.Document.JournalNumber.ilike(f"%{uni_api_filter}%"),
                model.Document.UploadDocType.ilike(f"%{uni_api_filter}%"),
                model.Document.store.ilike(f"%{uni_api_filter}%"),
                model.Document.dept.ilike(f"%{uni_api_filter}%"),
                model.Vendor.VendorName.ilike(f"%{uni_api_filter}%"),
                model.Vendor.Address.ilike(f"%{uni_api_filter}%"),
                model.DocumentSubStatus.status.ilike(f"%{uni_api_filter}%"),
                inv_choice[inv_type][1].Account.ilike(f"%{uni_api_filter}%"),
                # New condition: Check if any related DocumentLineItems.Value matches the filter
                # exists().where(
                #     (model.DocumentLineItems.documentID == model.Document.idDocument) &
                #     (model.DocumentLineItems.Value.ilike(f"%{uni_api_filter}%"))
                # )
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

        # Apply pagination
        Documentdata = (
            data_query.order_by(model.Document.CreatedOn.desc())
            .limit(limit)
            .offset(off_val)
            .all()
        )

        # If uni_api_filter exists, fetch line items
        result = []
        if uni_api_filter:
            for doc_row in Documentdata:
                document = doc_row.Document  # Access the Document model instance
                inv_id = document.idDocument  # Extract document ID

                # Fetch all related line item tag descriptions
                doclinetags = (
                    db.query(model.DocumentLineItemTags)
                    .options(Load(model.DocumentLineItemTags).load_only("TagName"))
                    .filter(
                        model.DocumentLineItemTags.idDocumentLineItemTags.in_(
                            db.query(model.DocumentLineItems.lineItemtagID)
                            .filter_by(documentID=inv_id)
                            .distinct()
                        )
                    )
                    .all()
                )

                # For each line tag, fetch its associated line items and updates
                for row in doclinetags:
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
                            Load(model.DocumentUpdates).load_only(
                                "OldValue", "UpdatedOn"
                            ),
                        )
                        .filter(
                            model.DocumentLineItems.lineItemtagID
                            == row.idDocumentLineItemTags,
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

                    # Apply universal search filter for line items if necessary
                    if uni_api_filter:
                        query = query.filter(
                            or_(
                                model.DocumentLineItems.Value.ilike(
                                    f"%{uni_api_filter}%"
                                ),
                                model.DocumentLineItems.ErrorDesc.ilike(
                                    f"%{uni_api_filter}%"
                                ),
                            )
                        )

                    # Retrieve the line item data
                    linedata = query.all()

                    # Attach the line item data to the document
                    row.linedata = linedata

                # Attach the document with its line items and line tags to the result
                result.append(
                    {
                        "document": doc_row,
                        "lineitems": doclinetags,  # Attach all line tags and their items for this document
                    }
                )
        else:
            # If no uni_api_filter, just return document data without line items
            result = [{"document": doc_row} for doc_row in Documentdata]
        # Return paginated document data with line items
        return {"ok": {"Documentdata": result, "TotalCount": total_count}}

    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def read_invoice_data(u_id, inv_id, db, uni_api_filter):
    """This function reads the invoice list, contains following parameters
    :u_id: It is a function parameters that is of integer type, it provides the
    user Id.

    :inv_id : It is a function parameter that is of integer type, it
    provides the invoice id. :db: It provides a session to interact with
    the backend Database,that is of Session Object Type.
    :return: It return a result of dict type.
    """
    try:
        servicedata = ""
        vendordata = ""
        # getting invoice data for later operation
        invdat = (
            db.query(model.Document)
            .options(
                load_only(
                    "docPath", "supplierAccountID", "vendorAccountID", "uploadtime"
                )
            )
            .filter_by(idDocument=inv_id)
            .one()
        )

        # provide service provider details
        if invdat.supplierAccountID:
            servicedata = (
                db.query(model.ServiceProvider, model.ServiceAccount)
                .options(
                    Load(model.ServiceProvider).load_only(
                        "ServiceProviderName",
                        "ServiceProviderCode",
                        "LocationCode",
                        "City",
                        "Country",
                    ),
                    Load(model.ServiceAccount).load_only(
                        "Account", "Email", "MeterNumber"
                    ),
                )
                .filter(
                    model.ServiceAccount.idServiceAccount == invdat.supplierAccountID
                )
                .join(
                    model.ServiceAccount,
                    model.ServiceAccount.serviceProviderID
                    == model.ServiceProvider.idServiceProvider,
                    isouter=True,
                )
                .all()
            )

        # provide vendor details
        if invdat.vendorAccountID:
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
                .filter(model.VendorAccount.idVendorAccount == invdat.vendorAccountID)
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
            .join(
                model.DocumentUpdates,
                model.DocumentUpdates.documentDataID
                == model.DocumentData.idDocumentData,
                isouter=True,
            )
            .filter(
                or_(
                    model.DocumentData.IsUpdated == 0,
                    model.DocumentUpdates.IsActive == 1,
                )
            )
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

        # # form a line item structure
        # for row in doclinetags:
        #     linedata = db.query(model.DocumentLineItems, model.DocumentUpdates).options(
        #         Load(model.DocumentLineItems).load_only("Value", "IsUpdated", "isError",
        #                                                 "ErrorDesc", "Xcord", "Ycord", "Width",
        #                                                 "Height", "itemCode"),
        #         Load(model.DocumentUpdates).load_only("OldValue", "UpdatedOn")).filter(
        #         model.DocumentLineItems.lineItemtagID == row.idDocumentLineItemTags,
        #         model.DocumentLineItems.documentID == inv_id).join(
        #         model.DocumentUpdates,
        #         model.DocumentUpdates.documentLineItemID == model.DocumentLineItems.idDocumentLineItems,
        #         isouter=True).filter(
        #         or_(model.DocumentLineItems.IsUpdated == 0, model.DocumentUpdates.IsActive == 1)).all()

        #     row.linedata = linedata

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

            # Apply the filter if uni_api_filter is present (before executing the query)
            if uni_api_filter:
                query = query.filter(
                    (model.DocumentLineItems.Value.ilike(f"%{uni_api_filter}%"))
                    | (model.DocumentLineItems.ErrorDesc.ilike(f"%{uni_api_filter}%"))
                )

            # Execute the query to retrieve the data
            linedata = query.all()

            # Attach the linedata to the row
            row.linedata = linedata

        return {
            "ok": {
                "vendordata": vendordata,
                "servicedata": servicedata,
                "headerdata": headerdata,
                "linedata": doclinetags,
                "uploadtime": invdat.uploadtime,
            }
        }

    except Exception:
        logger.error(f"Error in line item :{traceback.format_exc()}")
        return Response(status_code=500, headers={"codeError": "Server Error"})
    finally:
        db.close()


async def read_invoice_file(u_id, inv_id, db):
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
                account_name = fr_data.ConnectionString.split("AccountName=")[1].split(
                    ";AccountKey"
                )[0]
                account_url = f"https://{account_name}.blob.core.windows.net"
                blob_service_client = BlobServiceClient(
                    account_url=account_url, credential=credential
                )
                if invdat.supplierAccountID:
                    blob_client = blob_service_client.get_blob_client(
                        container=fr_data.ContainerName, blob=invdat.docPath
                    )
                if invdat.vendorAccountID:
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
                except Exception as e:
                    print(f"Error in file type : {e}")
                invdat.docPath = base64.b64encode(blob_client.download_blob().readall())
            except:
                invdat.docPath = ""

        return {"result": {"filepath": invdat.docPath, "content_type": content_type}}

    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500, headers={"codeError": "Server Error"})
    finally:
        db.close()


async def update_invoice_data(u_id, inv_id, inv_data, db):
    """This function update the invoice line item data, contains following
    parameters :u_id: It is a function parameters that is of integer type, it
    provides the user Id.

    :inv_id : It is a function parameter that is of integer type, it
    provides the invoice id.
    :param inv_data: It is Body parameter that is of a Pydantic type
        object, It takes member data for updating of new invoice line
        item. :db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dict type.
    """
    try:
        # avoid data updates by other users if in lock
        dt = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        for row in inv_data:
            try:
                # check to see if the document id and document data are related
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
            # to check if the document update table, already has rows present in it
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
                # if present set active status to false for old row
                if row.documentDataID:
                    db.query(model.DocumentUpdates).filter_by(
                        documentDataID=row.documentDataID, IsActive=1
                    ).update({"IsActive": 0})
                else:
                    db.query(model.DocumentUpdates).filter_by(
                        documentLineItemID=row.documentLineItemID, IsActive=1
                    ).update({"IsActive": 0})
                db.flush()
            data = dict(row)
            data["IsActive"] = 1
            # data["updatedBy"] = u_id
            data["UpdatedOn"] = dt
            data = model.DocumentUpdates(**data)
            db.add(data)
            db.flush()
            if row.documentDataID:
                doc_table_match = {
                    "InvoiceTotal": "totalAmount",
                    "InvoiceDate": "documentDate",
                    "InvoiceId": "docheaderID",
                    "PurchaseOrder": "PODocumentID",
                }
                ser_doc_table_match = {
                    "Issue Date": "documentDate",
                    "Total Due Inc VAT": "totalAmount",
                    "Invoice ID": "docheaderID",
                }
                tag_def_inv_id = (
                    db.query(
                        model.DocumentData.documentTagDefID,
                        model.DocumentData.documentID,
                    )
                    .filter_by(idDocumentData=row.documentDataID)
                    .one()
                )
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
                # to update the document if header data is updated for service provider
                if label in ser_doc_table_match.keys():
                    value = data.NewValue
                    if label == "Total Due Inc VAT":
                        value = float(re.sub(r"[^0-9.]", "", value))
                    db.query(model.Document).filter_by(
                        idDocument=tag_def_inv_id.documentID
                    ).update({ser_doc_table_match[label]: value})
                db.query(model.DocumentData).filter_by(
                    idDocumentData=row.documentDataID
                ).update({"IsUpdated": 1, "Value": data.NewValue})
            else:
                db.query(model.DocumentLineItems).filter_by(
                    idDocumentLineItems=row.documentLineItemID
                ).update({"IsUpdated": 1, "Value": data.NewValue})
            db.flush()
        # last updated time stamp
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
    """This function update column position of the tab, contains following
    parameters :param u_id: It is a function parameters that is of integer
    type, it provides the user Id.

    :param tabtype: It is a function parameters that is of string type,
        it provides the type of tab.
    :param col_data: It is a function parameters that is of Pydantic
        type, It provides column data for updating column position.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dict type.
    """
    try:
        UpdatedOn = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        for items in col_data:
            items = dict(items)
            items["UpdatedOn"] = UpdatedOn
            items["documentColumnPos"] = items.pop("ColumnPos")
            # result = (
            #     db.query(model.DocumentColumnPos)
            #     .filter_by(idDocumentColumn=items.pop("idtabColumn"))
            #     .filter_by(uid=u_id)
            #     .filter_by(tabtype=tabtype)
            #     .update(items)
            # )  # TODO: Unused variable
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


async def read_column_pos(u_id, tabtype, db):
    """This function reads the column position of a given tab type, contains
    following parameters :param u_id: It is a function parameters that is of
    integer type, it provides the user Id.

    :param tabtype: It is a function parameters that is of string type,
        it provides the type of tab.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dict type.
    """
    try:
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
                model.DocumentColumnPos.uid == u_id,
                model.DocumentColumnPos.tabtype == tabtype,
            )
            .all()
        )
        if len(column_data) == 0:
            allcolumns = (
                db.query(model.DocumentColumnPos)
                .filter(model.DocumentColumnPos.userID == 1)
                .all()
            )
            for ac in allcolumns:
                to_insert = {
                    "columnNameDefID": ac.columnNameDefID,
                    "documentColumnPos": ac.documentColumnPos,
                    "isActive": ac.isActive,
                    "tabtype": ac.tabtype,
                    "uid": u_id,
                }
                db.add(model.DocumentColumnPos(**to_insert))
                db.commit()
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
                    model.DocumentColumnPos.uid == u_id,
                    model.DocumentColumnPos.tabtype == tabtype,
                )
                .all()
            )
        # Convert the tuple to a list of dictionaries
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
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def get_role_priority(u_id, db):
    try:
        sub_query = db.query(model.AccessPermission.permissionDefID).filter_by(
            userID=u_id
        )
        # return priority
        return (
            db.query(model.AccessPermissionDef.Priority)
            .filter_by(idAccessPermissionDef=sub_query.scalar_subquery())
            .scalar()
        )
    except Exception:
        logger.error(traceback.format_exc())
        return 0
    finally:
        db.close()


async def setdochistorylog(u_id, inv_id, stat_id, dochist, db):
    """This function saves the status of the invoice thus preserving its
    history , contains following parameters.

    :param u_id: It is a function parameters that is of integer type, it
        provides the user Id.
    :param inv_id: It is a function parameters that is of integer type,
        it provides the invoice Id.
    :param stat_id:
    :param dochist: It is a function parameters that is of dict type, it
        provides the description and amount which are optional.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of 1 integer type.
    """
    # return dochist["documentdescription"]
    if (
        dochist["documentdescription"] is None
        or "documentdescription" not in dochist.keys()
    ):
        user_name = (
            db.query(model.User.firstName, model.User.lastName)
            .filter_by(idUser=u_id)
            .one()
        )
        description = {
            1: f"Edit approved by user {user_name[0] if user_name[0] else ''} {user_name[1] if user_name[1] else ''}",
            5: f"Invoice assigned to id {user_name[0] if user_name[0] else ''} {user_name[1] if user_name[1] else ''}",
            6: f"Invoice edited by user id {user_name[0] if user_name[0] else ''} {user_name[1] if user_name[1] else ''}",
        }
        dochist["documentdescription"] = description[stat_id]
    try:
        db.query(model.Document).filter_by(idDocument=inv_id).update(
            {
                "UpdatedOn": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "documentStatusID": stat_id,
            }
        )
        dochist["documentID"] = inv_id
        dochist["documentStatusID"] = stat_id
        dochist["userID"] = u_id
        dochist["CreatedOn"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        dochist = model.DocumentHistoryLogs(**dochist)
        db.add(dochist)
        db.commit()
        return 1
    except Exception:
        logger.error(traceback.format_exc())
        return None
    finally:
        db.close()


async def read_invoice_status_history(u_id, inv_id, db):
    """This function is used to read invoice history logs, contains following
    parameters.

    :param u_id: It is a function parameters that is of integer type, it
        provides the user Id.
    :param inv_id: It is a function parameters that is of integer type,
        it provides the invoice Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of list type.
    """
    try:
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
                        model.DocumentHistoryLogs.documentStatusID == None,
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
        logger.error(traceback.format_exc())
        return Response(status_code=500, headers={"Error": "Server Error"})
    finally:
        db.close()


async def read_doc_data(inv_id, db):
    data = (
        db.query(
            model.Document.entityID,
            model.Document.vendorAccountID,
            model.Document.supplierAccountID,
            model.Document.documentDate,
            model.Document.PODocumentID,
            model.Document.docheaderID,
        )
        .filter(model.Document.idDocument == inv_id)
        .first()
    )
    entityID = data[0]
    vendoraccountId = data[1]
    supplierAccountId = data[2]
    inv_date = data[3]
    po = data[4]
    invoicenum = data[5]
    grndata = (
        db.query(model.Document.docheaderID)
        .filter(model.Document.PODocumentID == po, model.Document.idDocumentType == 2)
        .first()
    )
    grn = grndata[0] if grndata is not None else ""
    if vendoraccountId is not None:
        vendordata = (
            db.query(model.VendorAccount.vendorID)
            .filter(model.VendorAccount.idVendorAccount == vendoraccountId)
            .first()
        )
        vendor = (
            db.query(model.Vendor.VendorName)
            .filter(model.Vendor.idVendor == vendordata[0])
            .first()
        )
    if supplierAccountId is not None:
        spdata = (
            db.query(model.ServiceAccount.serviceProviderID)
            .filter(model.ServiceAccount.idServiceAccount == supplierAccountId)
            .first()
        )
        vendor = (
            db.query(model.ServiceProvider.ServiceProviderName)
            .filter(model.ServiceProvider.idServiceProvider == spdata[0])
            .first()
        )
    entity = (
        db.query(model.Entity.EntityName)
        .filter(model.Entity.idEntity == entityID)
        .first()
    )
    return {
        "Entity": entity[0],
        "Vendor": vendor[0],
        "InvoiceDate": (
            inv_date.strftime("%Y-%m-%d %H:%M:%S") if inv_date is not None else ""
        ),
        "PO": po if po is not None else "",
        "GRN": grn if grn is not None else "",
        "InvoiceNumber": invoicenum if invoicenum is not None else "",
    }


async def read_doc_history(inv_id, db):
    """This function is used to read invoice history logs, contains following
    parameters.

    :param u_id: It is a function parameters that is of integer type, it
        provides the user Id.
    :param inv_id: It is a function parameters that is of integer type,
        it provides the invoice Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of list type.
    """
    try:
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
                        model.DocumentHistoryLogs.documentStatusID == None,
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
                (model.DocumentHistoryLogs.documentStatusID == 10, "Invoice Rejected"),
                (model.DocumentHistoryLogs.documentStatusID == 14, "Posted In ERP"),
            ]
        ).label("dochistorystatus")
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
            .all()
        )
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500, headers={"Error": "Server Error"})
    finally:
        db.close()


async def read_document_lock_status(u_id, inv_id, client_ip, db):
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
            # if lock info not null, compare the lock session time, if lesser than current time reset info
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
        # get the lock uid value
        lock_uid = (
            db.query(model.Document.lock_user_id)
            .filter(model.Document.idDocument == inv_id)
            .scalar()
        )
        # session status value
        session_datetime = dict(session_datetime)
        # if user id is not null or lock uid is some other user raise return response
        if lock_uid and lock_uid != u_id:
            return Response(
                status_code=403,
                headers={"ClientError": "check lock status before updating"},
            )
        else:
            if session_datetime["session_status"]:
                session_time = datetime.utcnow()
                session_time = session_time + timedelta(minutes=5)
                session_time = session_time.strftime("%Y-%m-%d %H:%M:%S")
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
                session_time = ""
                db.query(model.Document).filter_by(idDocument=inv_id).update(
                    {"lock_info": None, "lock_user_id": None}
                )
            db.commit()
            return {"result": "updated", "session_end_time": session_time}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500, headers={"Error": "Server error"})
    finally:
        db.close()


async def get_stamp_data_fields(u_id, inv_id, db):
    """Function to get stamp data fields based on document ID.

    :param u_id: User ID of the requestor.
    :param inv_id: Document ID to filter stamp data.
    :param db: Async session to interact with the database.
    :return: Stamp data matching the document ID, with specific fields.
    """
    # Query to get StampData filtered by document ID
    stamp_data = (
        db.query(model.StampData)
        .filter(model.StampData.DOCUMENT_ID == inv_id)
        .options(
            Load(model.StampData).load_only(
                "DEPTNAME",
                "RECEIVING_DATE",
                "CONFIRMATION_NUMBER",
                "RECEIVER",
                "SELECTED_DEPT",
                "storenumber",
            )
        )
        .first()
    )

    # If no data is found, return None or handle accordingly
    if not stamp_data:
        return {"message": "No stamp data found for the provided document ID."}

    return stamp_data


async def update_stamp_data_fields(u_id, inv_id, update_data, db):
    """Function to update stamp data fields based on document ID.

    :param u_id: User ID of the requestor.
    :param inv_id: Document ID to filter the stamp data for updating.
    :param update_data: Data to update the specific fields.
    :param db: Session to interact with the database.
    :return: The updated StampData object.
    """
    # Query to get the specific StampData by document ID
    stamp_data = (
        db.query(model.StampData).filter(model.StampData.DOCUMENT_ID == inv_id).first()
    )

    # If no data is found, return None
    if not stamp_data:
        return None

    # Update the fields if they are provided in the update_data
    if update_data.DEPTNAME is not None:
        stamp_data.DEPTNAME = update_data.DEPTNAME
    if update_data.RECEIVING_DATE is not None:
        stamp_data.RECEIVING_DATE = update_data.RECEIVING_DATE
    if update_data.CONFIRMATION_NUMBER is not None:
        stamp_data.CONFIRMATION_NUMBER = update_data.CONFIRMATION_NUMBER
    if update_data.RECEIVER is not None:
        stamp_data.RECEIVER = update_data.RECEIVER
    if update_data.SELECTED_DEPT is not None:
        stamp_data.SELECTED_DEPT = update_data.SELECTED_DEPT
    if update_data.storenumber is not None:
        stamp_data.storenumber = update_data.storenumber

    # Update the `updated_at` field with the current timestamp
    stamp_data.UPDATED_ON = datetime.now()

    # Commit the transaction to save the changes
    db.commit()

    # Refresh the instance to reflect the updated state
    db.refresh(stamp_data)

    return stamp_data


async def new_get_stamp_data_by_document_id(u_id, inv_id, db):
    """Function to retrieve all stamptagname with their respective stampvalue
    from the StampDataValidation table based on the document ID.

    :param inv_id: Document ID to filter the stamp data.
    :param db: Session to interact with the database.
    :return: A list of dictionaries containing stamptagname and
        stampvalue.
    """
    try:
        # Query to filter records based on the document ID
        stamp_data_records = (
            db.query(model.StampDataValidation)
            .filter(model.StampDataValidation.documentid == inv_id)
            .all()
        )

        # Check if records are found
        if not stamp_data_records:
            return {"error": f"No stamp data found for document ID: {inv_id}"}

        # # Prepare the result in a list of dictionaries
        # result = [
        #     {"stamptagname": record.stamptagname, "stampvalue": record.stampvalue}
        #     for record in stamp_data_records
        # ]

        return stamp_data_records

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

    :param u_id: User ID of the requestor.
    :param inv_id: Document ID to filter the stamp data for updating.
    :param update_data: Data to update the specific fields.
    :param db: Session to interact with the database.
    :return: The updated StampData object.
    """
    dt = datetime.now().strftime("%m-%d-%Y %H:%M:%S")
    updated_records = []

    try:
        for update_data in update_data_list:
            try:
                # Extract data from the current update dictionary
                stamptagname = update_data.stamptagname
                new_value = update_data.NewValue
                old_value = update_data.OldValue

                # Query the database to find the record
                stamp_data = (
                    db.query(model.StampDataValidation)
                    .filter(
                        model.StampDataValidation.documentid == inv_id,
                        model.StampDataValidation.stamptagname == stamptagname,
                    )
                    .first()
                )

                # If no record is found, log an error and continue
                if not stamp_data:
                    updated_records.append(
                        {
                            "stamptagname": stamptagname,
                            "error": f"No matching stamp data found for stamptagname: {stamptagname}",
                        }
                    )
                    continue

                # Update the OldValue, stampvalue (new value), IsUpdated, and UpdatedOn
                stamp_data.OldValue = old_value
                stamp_data.stampvalue = new_value
                stamp_data.IsUpdated = 1
                stamp_data.UpdatedOn = dt

                # Add the updated object to the list (it will be refreshed later)
                updated_records.append(stamp_data)

            except SQLAlchemyError:
                logger.error(traceback.format_exc())
                # Catch any SQLAlchemy-specific error during the update of a single record
                updated_records.append(
                    {
                        "stamptagname": stamptagname,
                        "error": f"Database error occurred: {str(traceback.format_exc())}",
                    }
                )
                # Continue to next record without breaking the loop

        # Commit the changes to the database
        db.commit()

        # Refresh and return the updated records
        for stamp_data in updated_records:
            if isinstance(stamp_data, model.StampDataValidation):
                db.refresh(stamp_data)

    except SQLAlchemyError:
        logger.error(traceback.format_exc())
        # Handle any general SQLAlchemy error and rollback the transaction
        db.rollback()
        return {
            "error": f"Transaction failed due to a database error: {str(traceback.format_exc())}"
        }

    except Exception:
        logger.error(traceback.format_exc())
        # Handle any other unexpected errors and rollback
        db.rollback()
        print(traceback.format_exc())
        return Response(status_code=500, headers={"Error": "Internal Server error"})

    return updated_records

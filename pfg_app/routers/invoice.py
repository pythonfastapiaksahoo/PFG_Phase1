import io
import os
from typing import List, Literal, Optional

import pandas as pd
from fastapi import APIRouter, BackgroundTasks, Depends
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.orm import Session

from pfg_app.azuread.auth import get_user
from pfg_app.azuread.schemas import AzureUser
from pfg_app.crud import InvoiceCrud as crud
from pfg_app.FROps import pdfcreator
from pfg_app.schemas import InvoiceSchema as schema
from pfg_app.session.session import get_db

router = APIRouter(
    prefix="/apiv1.1/Invoice",
    tags=["invoice"],
    responses={404: {"description": "Not found"}},
)


# Checked (new) - used in the frontend
@router.get("/readPaginatedDocumentINVListWithLnItems")
async def read_paginate_doc_inv_list_with_ln_item(
    ven_id: Optional[int] = None,
    status: Optional[str] = None,  # Accept a colon-separated string
    offset: int = 1,
    limit: int = 10,
    date_range: Optional[str] = None,  # New parameter for start date
    uni_search: Optional[str] = None,
    ven_status: Optional[str] = None,
    sort_column: Optional[str] = None,  # New parameter for sorting column
    sort_order: str = "asc",  # New parameter for sorting order
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user),
):
    """API route to retrieve a paginated list of invoice documents with line
    item details as optional when filters is applied  .

    Parameters:
    ----------
    ven_id : int, optional
        Vendor ID for filtering documents (default is None).
    status : Literal, optional
        Status of the invoice document to filter by.
        Options: 'posted', 'rejected', 'exception', 'VendorNotOnboarded',
        'VendorUnidentified' (default is None).
    offset : int
        The page number for pagination (default is 1).
    limit : int
        Number of records per page (default is 10).
    uni_search : str, optional
        Universal search term to filter documents (default is None).
    ven_status : str, optional
        Vendor status to filter documents (default is None).
    sort_column : str, optional
        The column to sort the results by (default is None).
        Available columns: 'docheaderID', 'VendorCode', 'VendorName', 'JournalNumber',
        'Store', 'Department', 'Status', 'SubStatus'.
    sort_order : str
        The sorting order ('asc' or 'desc', default is 'asc').
    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    List of invoice documents filtered and paginated according to the input parameters.
    """

    docs = await crud.read_paginate_doc_inv_list_with_ln_items(
        user.idUser,
        ven_id,
        "ven",
        status,
        (offset, limit),
        db,
        uni_search,
        ven_status,
        date_range,
        sort_column,
        sort_order,
    )
    return docs


@router.get("/readInvoiceData/idInvoice/{inv_id}")
async def read_invoice_data_item(
    inv_id: int,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user),
):
    """API route to retrieve invoice document data based on the invoice ID.

    Parameters:
    ----------
    inv_id : int
        Invoice ID used to select the document and return its data.
    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    Dictionary containing the invoice document data, including:
        - Vendor details
        - Header details
        - Line details
        - upload time
    """
    return await crud.read_invoice_data(user.idUser, inv_id, db)


# Checked - used in the frontend
@router.get("/readInvoiceFile/idInvoice/{inv_id}")
async def read_invoice_file_item(
    inv_id: int, db: Session = Depends(get_db), user: AzureUser = Depends(get_user)
):
    """API route to retrieve invoice file data based on the invoice ID.

    Parameters:
    ----------
    inv_id : int
        Invoice ID used to select the document and return its data.
    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    Dictionary containing the following:
        - Base64-encoded PDF string
        - content_type
    """
    return await crud.read_invoice_file(user.idUser, inv_id, db)


# Checked - used in the frontend
@router.post("/updateInvoiceData/idInvoice/{inv_id}")
async def update_invoice_data_item(
    inv_id: int,
    inv_data: List[schema.UpdateServiceAccountInvoiceData],
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user),
):
    """API route to update invoice document data.

    Parameters:
    ----------
    inv_id : int
        Invoice ID provided as a path parameter to identify
        which document to update.
    inv_data : List[UpdateServiceAccountInvoiceData]
        Body parameter containing a list of updated invoice
        data represented as a Pydantic model.
    db : Session
        Database session object used to interact with the
        backend database.
    user : Depends(get_user)
        User object retrieved from the authentication system
        to identify the user making the request.

    Returns:
    -------
    dict
        A dictionary containing the result of the update
        operation, indicating success or failure.
    """
    return await crud.update_invoice_data(user.idUser, inv_id, inv_data, db)


# Checked - used in the frontend
@router.post("/updateColumnPos")
async def update_column_pos_item(
    bg_task: BackgroundTasks,
    col_data: List[schema.columnpos],
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user),
):
    """API route to update the column position for a user.

    Parameters:
    ----------
    bg_task : BackgroundTasks
        Background task manager for handling asynchronous tasks.
    col_data : List[columnpos]
        Body parameter containing a list of column positions
        represented as a Pydantic model.
    db : Session
        Database session object used to interact with the backend database.
    user : Depends(get_user)
        User object retrieved from the authentication system
        to identify the user making the request.

    Returns:
    -------
    dict
        A dictionary containing the result of the update operation,
        indicating success or failure.
    """
    return await crud.update_column_pos(user.idUser, 1, col_data, bg_task, db)


# Checked - used in the frontend
@router.get("/readColumnPos")
async def read_column_pos_item(
    db: Session = Depends(get_db), user: AzureUser = Depends(get_user)
):
    """API route to read the column position for a user.

    Parameters:
    ----------
    db : Session
        Database session object used to interact with the backend database.
    user : Depends(get_user)
        User object retrieved from the authentication system
        to identify the user making the request.

    Returns:
    -------
    dict
        A dictionary containing the column position data for the specified tab.
    """
    data = await crud.read_column_pos(user.idUser, 1, db)
    return data


# Checked - used in the frontend
@router.get("/get_stamp_data_new/{inv_id}")
async def new_get_stamp_data_fields(
    inv_id: int, db: Session = Depends(get_db), user: AzureUser = Depends(get_user)
):
    """API route to retrieve stamp data fields based on the document ID.

    Parameters:
    ----------
    inv_id : int
        Document ID used to select and return its associated stamp data fields.
    db : Session
        Database session object, used to interact with the backend database.
    user : Depends(get_user)
        User object retrieved from the authentication system
        to identify the user making the request.

    Returns:
    -------
    dict
        Returns the invoice stamp data fields.
    """
    return await crud.new_get_stamp_data_by_document_id(user.idUser, inv_id, db)


# Checked - used in the frontend
@router.put("/new_update_stamp_data/{inv_id}")
async def new_update_stamp_data(
    inv_id: int,
    update_data: List[schema.UpdateStampData],
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user),
):
    """API route to update stamp data fields for a given document.

    Parameters:
    ----------
    inv_id : int
        Document ID used to filter the stamp data for updating.
    update_data : List[UpdateStampData]
        List of fields to update, including the tag name, old value, and new value.
    db : Session
        Database session object, used to interact with the backend database.
    user : Depends(get_user)
        User object retrieved from the authentication system to identify
        the user making the request.

    Returns:
    -------
    dict
        A dictionary containing the response message indicating
        the result of the operation.
    """
    updated_stamp_data = await crud.new_update_stamp_data_fields(
        user.idUser, inv_id, update_data, db
    )

    return {"response": updated_stamp_data}


@router.get("/journeydoc/docid/{inv_id}")
async def download_journeydoc(
    inv_id: int, download: bool = False, db: Session = Depends(get_db)
):
    """### API route to download journey document.

    It contains following parameters.
    :param inv_id: It is an path parameter for selecting document id to
        return its data, it is of int type.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: journey doc as pdf.
    """
    try:
        for f in os.listdir():
            if os.path.isfile(f) and f.endswith(".pdf"):
                os.unlink(f)
        all_status = await crud.read_doc_history(inv_id, download, db)
        if download:
            filename = pdfcreator.createdoc(all_status, inv_id)
            return FileResponse(
                path=filename, filename=filename, media_type="application/pdf"
            )
        else:
            return all_status
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error in downloading journey document: {e}",
        }


@router.get("/downloadDocumentInvoiceList")
async def download_documents(
    ven_id: Optional[int] = None,
    status: Optional[
        Literal[
            "posted",
            "rejected",
            "exception",
            "VendorNotOnboarded",
            "VendorUnidentified",
        ]
    ] = None,
    uni_search: Optional[str] = None,
    ven_status: Optional[str] = None,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user),
):
    """Endpoint to fetch document invoice data, convert it to Excel, and allow
    download.

    Parameters:
    -----------
    u_id : int
        User ID
    ven_id : int
        Vendor ID to filter
    inv_type : str
        Type of invoice ("ser" for service, "ven" for vendor)
    stat : Optional[str]
        Status filter
    uni_api_filter : Optional[str]
        Universal search filter for the invoice
    ven_status : Optional[str]
        Vendor status ("A" for active, "I" for inactive)
    db : Session
        Database session injected by FastAPI

    Returns:
    --------
    StreamingResponse
        An Excel file download of the filtered document data.
    """

    # Fetch the document data using the existing function
    result = await crud.read_all_doc_inv_list(
        user.idUser, ven_id, "ven", status, db, uni_search, ven_status
    )

    # Check if result was successful
    if "ok" not in result or not result["ok"]["Documentdata"]:
        return {"error": "No document data found."}

    document_data = result["ok"]["Documentdata"]

    # Extract data into a list of dictionaries to create the DataFrame
    extracted_data = []
    for doc in document_data:
        # Accessing attributes of the Document, Vendor, and other objects directly
        created_on = pd.to_datetime(doc.Document.CreatedOn).tz_localize(
            None
        )  # Convert to timezone-naive
        extracted_data.append(
            {
                "Invoice Number": doc.Document.docheaderID,
                "Vendor Name": doc.Vendor.VendorName if doc.Vendor else None,
                "Vendor Code": doc.Vendor.VendorCode if doc.Vendor else None,
                "Vendor Address": doc.Vendor.Address if doc.Vendor else None,
                "Amount": doc.Document.totalAmount,
                "Confirmation Number": doc.Document.JournalNumber,
                "Invoice Type": doc.Document.UploadDocType,
                "Invoice Date": doc.Document.documentDate,
                "Status": doc.docstatus,
                "Sub Status": (
                    doc.DocumentSubStatus.status if doc.DocumentSubStatus else None
                ),
                "Sender": doc.Document.sender,
                "Store": doc.Document.store,
                "Department": doc.Document.dept,
                "Upload Date": created_on,
                "Voucher ID": doc.Document.voucher_id,
            }
        )

    # Convert the extracted data to a pandas DataFrame
    df = pd.DataFrame(extracted_data)

    # Create an in-memory Excel file using pandas and io
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="DocumentInvoices")

    output.seek(0)

    # Return the Excel file as a StreamingResponse for download
    headers = {"Content-Disposition": "attachment; filename=document_invoices.xlsx"}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


# Checked - used in the frontend
@router.post("/updateRejectedDocumentStatus/{inv_id}")
async def update_rejected_invoice_status(
    inv_id: int,
    reason: str,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user),
):
    """API route to update the status of a rejected invoice.

    Parameters:
    ----------
    inv_id : int
        The ID of the invoice to update the status for.
    reason : str
        The reason for rejecting the invoice.
    db : Session
        Database session object used to interact with the backend database.
    user : AzureUser
        User object retrieved from the authentication system, used to identify the
        user making the request.

    Returns:
    -------
    dict
        A response indicating the success or failure of the operation.
    """
    return await crud.reject_invoice(user.idUser, inv_id, reason, db)


@router.get("/getEmailRowAssociatedFiles")
async def get_email_row_associated_files(
    offset: int = 1,
    limit: int = 10,
    uni_api_filter: Optional[str] = None,
    column_filter: Optional[str] = None,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user),
):
    """API route to retrieve a paginated list of invoice documents with line
    item details as optional when filters is applied  .

    Parameters:
    ----------

    offset : int
        The page number for pagination (default is 1).

    limit : int
        Number of records per page (default is 10).

    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    List of invoice documents filtered and paginated according to the input parameters.
    """

    docs = await crud.get_email_row_associated_files(
        user.idUser, (offset, limit), uni_api_filter, column_filter, db
    )
    return docs


# API to read all vendor names
@router.get("/departmentnamelist")
async def get_dept_names_list(db: Session = Depends(get_db)):
    """API route to retrieve a list of all active department names.

    Parameters:
    ----------

    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    List of active department names.
    """
    return await crud.readdeptname(db)

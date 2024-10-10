from typing import List, Literal, Optional

from fastapi import APIRouter, BackgroundTasks, Depends
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
import os
from pfg_app.azuread.auth import get_user
from pfg_app.azuread.schemas import AzureUser
from pfg_app.crud import InvoiceCrud as crud
from pfg_app.schemas import InvoiceSchema as schema
from pfg_app.session.session import get_db
from pfg_app.FROps import pdfcreator

router = APIRouter(
    prefix="/apiv1.1/Invoice",
    tags=["invoice"],
    responses={404: {"description": "Not found"}},
)


# Checked - used in the frontend
@router.get("/readPaginatedDocumentINVList")
async def read_paginate_doc_inv_list_item(
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
    offset: int = 1,
    limit: int = 10,
    uni_search: Optional[str] = None,
    ven_status: Optional[str] = None,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user),
):
    """API route to retrieve a paginated list of invoice documents with various
    filters.

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
    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    List of invoice documents filtered and paginated according to the input parameters.
    """
    return await crud.read_paginate_doc_inv_list(
        user.idUser, ven_id, "ven", status, (offset, limit), db, uni_search, ven_status
    )


# Checked (new) - used in the frontend
@router.get("/readPaginatedDocumentINVListWithLnItems")
async def read_paginate_doc_inv_list_with_ln_item(
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
    offset: int = 1,
    limit: int = 10,
    uni_search: Optional[str] = None,
    ven_status: Optional[str] = None,
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
    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    List of invoice documents filtered and paginated according to the input parameters.
    """

    docs = await crud.read_paginate_doc_inv_list_with_ln_items(
        user.idUser, ven_id, "ven", status, (offset, limit), db, uni_search, ven_status
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
async def download_journeydoc(inv_id: int,download: bool = False, db: Session = Depends(get_db)):
    """
    ### API route to download journey document. It contains following parameters.
    :param inv_id: It is an path parameter for selecting document id to return its data, it is of int type.
    :param db: It provides a session to interact with the backend Database,that is of Session Object Type.
    :return: journey doc as pdf.
    """
    try:
        for f in os.listdir():
            if os.path.isfile(f) and f.endswith(".pdf"):
                os.unlink(f)
        all_status = await crud.read_doc_history(inv_id,download,db)
        if download:
            filename = pdfcreator.createdoc(all_status,inv_id)
            return FileResponse(path=filename, filename=filename, media_type='application/pdf')
        else:
            return all_status
    except Exception as e:
        return {"status":"error","message":f"Error in downloading journey document: {e}"}

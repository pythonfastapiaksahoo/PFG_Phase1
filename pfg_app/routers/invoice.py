from typing import List, Literal, Optional

from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy.orm import Session

from pfg_app.azuread.auth import get_user
from pfg_app.crud import InvoiceCrud as crud
from pfg_app.schemas import InvoiceSchema as schema
from pfg_app.session import get_db

router = APIRouter(
    prefix="/apiv1.1/Invoice",
    tags=["invoice"],
    dependencies=[Depends(get_user)],
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
    user=Depends(get_user),
):
    """API route to read the Invoice Documents.

    :param u_id: Unique identifier used to identify a user.
    :param ven_id: Optional parameter for filtering documents based on
        vendor ID, of int type.
    :param sp_id: Optional parameter for filtering documents based on
        service provider ID, of int type.
    :param usertype: Dependent function which returns the type of user.
    :param db: Provides a session to interact with the backend Database.
    :return: Invoice document list.
    """
    return await crud.read_paginate_doc_inv_list(
        user.id, ven_id, "ven", status, (offset, limit), db, uni_search, ven_status
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
    user=Depends(get_user),
):
    """API route to read the Invoice Documents.

    :param u_id: Unique identifier used to identify a user.
    :param ven_id: Optional parameter for filtering documents based on
        vendor ID, of int type.
    :param sp_id: Optional parameter for filtering documents based on
        service provider ID, of int type.
    :param usertype: Dependent function which returns the type of user.
    :param db: Provides a session to interact with the backend Database.
    :return: Invoice document list.
    """
    return await crud.read_paginate_doc_inv_list_with_ln_items(
        ven_id, "ven", status, (offset, limit), db, uni_search, ven_status
    )


# Checked - used in the frontend
@router.get("/readInvoiceData/idInvoice/{inv_id}")
async def read_invoice_data_item(
    inv_id: int,
    uni_search: Optional[str] = None,
    db: Session = Depends(get_db),
    user=Depends(get_user),
):
    """### API route to read Document data.

    It contains following parameters.
    :param u_id: Unique Unique identifier used to identify a user.
    :param inv_id: It is an path parameter for selecting document id to
        return its data, it is of int type.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns the Document data [header details, line details,
        base64pdf string].
    """
    return await crud.read_invoice_data(user.id, inv_id, db, uni_search)


# Checked - used in the frontend
@router.get("/readInvoiceFile/idInvoice/{inv_id}")
async def read_invoice_file_item(
    inv_id: int, db: Session = Depends(get_db), user=Depends(get_user)
):
    """### API route to read Document data.

    It contains following parameters.
    :param u_id: Unique Unique identifier used to identify a user.
    :param inv_id: It is an path parameter for selecting document id to
        return its data, it is of int type.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns the Document data [header details, line details,
        base64pdf string].
    """
    return await crud.read_invoice_file(user.id, inv_id, db)


# Checked - used in the frontend
@router.post("/updateInvoiceData/idInvoice/{inv_id}")
async def update_invoice_data_item(
    inv_id: int,
    inv_data: List[schema.UpdateServiceAccountInvoiceData],
    db: Session = Depends(get_db),
    user=Depends(get_user),
):
    """### API route to update Document data.

    It contains following parameters.
    :param u_id: Unique Unique identifier used to identify a user.
    :param inv_id: It is an path parameter for selecting document id to
        return its data, it is of int type.
    :param inv_data: It is Body parameter that is of a Pydantic class
        object, holds list of updated invoice data for updating.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return flag result [success or failed]
    """
    return await crud.update_invoice_data(user.id, inv_id, inv_data, db)


# Checked - used in the frontend
@router.post("/updateColumnPos")
async def update_column_pos_item(
    bg_task: BackgroundTasks,
    col_data: List[schema.columnpos],
    db: Session = Depends(get_db),
    user=Depends(get_user),
):
    """### API route to update column position of a user.

    It contains following parameters.
    :param u_id: Unique Unique identifier used to identify a user.
    :param tabtype: It is an path parameter for selecting the tab, it is
        of string type.
    :param col_data: It is Body parameter that is of a Pydantic class
        object, hold list of column position for updating the tab.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return flag result [success or failed]
    """
    return await crud.update_column_pos(user.id, 1, col_data, bg_task, db)


# Checked - used in the frontend
@router.get("/readColumnPos")
async def read_column_pos_item(db: Session = Depends(get_db), user=Depends(get_user)):
    """### API route to read column position of a user.

    It contains following parameters.
    :param u_id: Unique Unique identifier used to identify a user.
    :param tabtype: It is an path parameter for selecting the tab, it is
        of string type.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return the column position data for a tab.
    """
    data = await crud.read_column_pos(user.id, 1, db)
    return data


# Checked - used in the frontend
@router.get("/get_stamp_data_new/{inv_id}")
async def new_get_stamp_data_fields(
    inv_id: int, db: Session = Depends(get_db), user=Depends(get_user)
):
    """### API route to read Stamp data fields.

    It contains following parameters.
    :param u_id: u_id: Unique Unique identifier used to identify a user.
    :param inv_id: It is an path parameter for selecting document id to
        return its data, it is of int type.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns Invoice Stamp data fields.
    """
    return await crud.new_get_stamp_data_by_document_id(user.id, inv_id, db)


# Checked - used in the frontend
@router.put("/new_update_stamp_data/{inv_id}")
async def new_update_stamp_data(
    inv_id: int,
    update_data: List[schema.UpdateStampData],
    db: Session = Depends(get_db),
    user=Depends(get_user),
):
    """API route to update Stamp data fields.

    :param inv_id: Document ID to filter the stamp data for updating.
    :param update_data: Data to update the specific fields.
    :param db: Session to interact with the backend Database.
    :return: A message indicating the result of the operation.
    """
    updated_stamp_data = await crud.new_update_stamp_data_fields(
        user.id, inv_id, update_data, db
    )

    return {"response": updated_stamp_data}

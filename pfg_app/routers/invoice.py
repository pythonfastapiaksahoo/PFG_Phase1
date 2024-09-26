import base64
import json
import os
import sys
from typing import List, Literal, Optional

import model
from azuread.auth import get_user
from crud import InvoiceCrud as crud
from fastapi import APIRouter, BackgroundTasks, Depends

# from typing_extensions import Annotated
from fastapi.responses import FileResponse
from PyPDF2 import PdfFileMerger
from schemas import InvoiceSchema as schema
from session import get_db
from sqlalchemy.orm import Session
from Utilities import pdfcreator, uploadtoblob

sys.path.append("..")


router = APIRouter(
    prefix="/apiv1.1/Invoice",
    tags=["invoice"],
    dependencies=[Depends(get_user)],
    responses={404: {"description": "Not found"}},
)


@router.get("/readDocumentINVList")
async def read_doc_inv_list_item(
    ven_id: Optional[int] = None,
    status: Optional[Literal["posted", "rejected", "exception"]] = None,
    # usertype: int = Depends(dependencies.check_usertype),
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
    return await crud.read_doc_inv_list(user.id, ven_id, "ven", status, db)


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


@router.get("/readInvoiceStatusHistory/idInvoice/{inv_id}")
async def read_invoice_status_history_item(
    inv_id: int, db: Session = Depends(get_db), user=Depends(get_user)
):
    """### API route to read Invoice document Status History.

    It contains following parameters.
    :param u_id: u_id: Unique Unique identifier used to identify a user.
    :param inv_id: It is an path parameter for selecting document id to
        return its data, it is of int type.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns Invoice document status history.
    """
    return await crud.read_invoice_status_history(user.id, inv_id, db)


@router.post("/getDocumentLockInfo/idDocument/{inv_id}")
async def read_document_lock_status(
    inv_id: int,
    client_ip: schema.SessionTime,
    db: Session = Depends(get_db),
    user=Depends(get_user),
):
    """### API route to read Invoice document Status History.

    It contains following parameters.
    :param u_id: u_id: Unique Unique identifier used to identify a user.
    :param inv_id: It is an path parameter for selecting document id to
        return its data, it is of int type.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns Invoice document status history.
    """
    return await crud.read_document_lock_status(user.id, inv_id, client_ip, db)


@router.post("/updateDocumentLockInfo/idDocument/{inv_id}")
async def update_document_lock_status(
    inv_id: int,
    session_datetime: schema.SessionTime,
    db: Session = Depends(get_db),
    user=Depends(get_user),
):
    """### API route to read Invoice document Status History.

    It contains following parameters.
    :param u_id: u_id: Unique Unique identifier used to identify a user.
    :param inv_id: It is an path parameter for selecting document id to
        return its data, it is of int type.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns Invoice document status history.
    """
    return await crud.update_document_lock_status(user.id, inv_id, session_datetime, db)


@router.get("/journeydoc/docid/{inv_id}")
async def download_journeydoc(inv_id: int, db: Session = Depends(get_db)):
    """### API route to download journey document.

    It contains following parameters.
    :param inv_id: It is an path parameter for selecting document id to
        return its data, it is of int type.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: journey doc as pdf.
    """
    for f in os.listdir():
        if os.path.isfile(f) and f.endswith(".pdf"):
            os.unlink(f)
    all_status = await crud.read_doc_history(inv_id, db)
    doc_data = await crud.read_doc_data(inv_id, db)
    filenames = pdfcreator.createdoc(all_status, doc_data)
    merger = PdfFileMerger()
    for f in filenames:
        merger.append(f)
    mergefile = f"Invoice#{inv_id}_JourneyMap.pdf"
    merger.write(mergefile)
    merger.close()
    with open(mergefile, "rb") as file:
        file_content = file.read()
        uploadtoblob.upload_to_blob(mergefile, file_content, db)
        base64_file_content: str = base64.b64encode(file_content).decode("utf-8")
        db.query(model.Document).filter(model.Document.idDocument == inv_id).update(
            {
                "journey_doc": json.dumps(
                    {
                        "filename": mergefile,
                        "content": base64_file_content,
                        "filetype": "application/pdf",
                    }
                )
            }
        )
        db.commit()
    db.close()
    return FileResponse(
        path=mergefile, filename=mergefile, media_type="application/pdf"
    )


@router.get("/get_stamp_data/{inv_id}")
async def get_stamp_data_fields(
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
    return await crud.get_stamp_data_fields(user.id, inv_id, db)


@router.put("/update_stamp_data/{inv_id}")
async def update_stamp_data(
    inv_id: int,
    update_data: schema.StampDataUpdate,
    db: Session = Depends(get_db),
    user=Depends(get_user),
):
    """API route to update Stamp data fields.

    :param inv_id: Document ID to filter the stamp data for updating.
    :param update_data: Data to update the specific fields.
    :param db: Session to interact with the backend Database.
    :return: A message indicating the result of the operation.
    """
    updated_stamp_data = await crud.update_stamp_data_fields(
        user.id, inv_id, update_data, db
    )

    # if not updated_stamp_data:
    #     raise HTTPException(status_code=404, detail="Stamp data not found for the provided document ID.")

    return {"message": "Stamp data updated successfully", "data": updated_stamp_data}


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

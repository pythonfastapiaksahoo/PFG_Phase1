from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from pfg_app.auth import AuthHandler
from pfg_app.azuread.auth import get_admin_user
from pfg_app.crud import VendorCrud as crud
from pfg_app.session.session import get_db

auth_handler = AuthHandler()

router = APIRouter(
    prefix="/apiv1.1/Vendor",
    tags=["Vendor"],
    dependencies=[Depends(get_admin_user)],
    responses={404: {"description": "Not found"}},
)


# # Checked(Doubt) - used in the frontend
# # API to read all vendor list
# @router.get("/vendorlist")
# async def read_vendor(db: Session = Depends(get_db)):
#     """###This function creates an api route for Reading Vendor list.

#     It contains 2 parameters.
#     :param db: It provides a session to interact with the backend
#         Database,that is of Session Object Type.
#     :return: it returns vendor list
#     """
#     return await crud.readvendor(db)


# Checked(Doubt) - used in the frontend
# API to read all vendor list
@router.get("/vendorlist/{u_id}")
async def read_vendor_list(
    u_id: int,
    ent_id: Optional[int] = None,
    ven_code: Optional[str] = None,
    onb_status: Optional[str] = None,
    offset: int = 1,
    limit: int = 10,
    vendor_type: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """###This function creates an api route for Reading Vendor list.

    It contains 2 parameters.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: it returns vendor list
    """
    data = await crud.readvendorbyuid(
        u_id,
        vendor_type,
        db,
        (offset, limit),
        {"ent_id": ent_id, "ven_code": ven_code, "onb_status": onb_status},
    )
    return data

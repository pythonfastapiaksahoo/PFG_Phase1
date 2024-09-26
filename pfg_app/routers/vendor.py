import sys
from typing import Optional

import model
from auth import AuthHandler
from azuread.auth import get_admin_user
from crud import VendorCrud as crud
from fastapi import APIRouter, BackgroundTasks, Depends, Request, Response, status
from fastapi.responses import StreamingResponse
from schemas import VendorSchema as schema
from session import get_db
from sqlalchemy.orm import Session

sys.path.append("..")

auth_handler = AuthHandler()

router = APIRouter(
    prefix="/apiv1.1/Vendor",
    tags=["Vendor"],
    dependencies=[Depends(get_admin_user)],
    responses={404: {"description": "Not found"}},
)


@router.get("/getuserDetails", status_code=status.HTTP_200_OK)
async def getuser(req: Request, db: Session = Depends(get_db)):
    try:
        token = req.headers["Authorization"].split(" ")[1]
        token_details = auth_handler.decode_token(token)
        try:
            username = token_details["sub"]
            user_id = (
                db.query(model.Credentials.userID).filter_by(LogName=username).scalar()
            )
            email, firstName = (
                db.query(model.User.email, model.User.firstName)
                .filter_by(idUser=user_id)
                .one()
            )
            user = {
                "username": username,
                "id": user_id,
                "email": email,
                "name": firstName,
            }
        except BaseException:
            return Response(status_code=404)
        return user
    except BaseException:
        return Response(status_code=500)


@router.post("/updateVendor/{vu_id}/idVendor/{v_id}", status_code=status.HTTP_200_OK)
async def update_vendor_erp(
    vu_id: int,
    bg_task: BackgroundTasks,
    v_id: int,
    UpdateVendor: schema.UpdateVendor,
    db: Session = Depends(get_db),
):
    """This function creates an api route to update Vendor.

    It contains 4 parameters.
    :param vu_id: It is a path parameters that is of integer type, it
        provides the vendor user Id.
    :param v_id: It is a path parameters that is of integer type, it
        provides the vendor Id.
    :param UpdateVendor: It is Body parameter that is of a Pydantic
        class object, It takes member data for updating of Vendor.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns a flag result.
    """
    return await crud.UpdateVendorERP(vu_id, v_id, UpdateVendor, db)


@router.put(
    "/updateVendorAccount/{vu_id}/idVendorAccount/{va_id}",
    status_code=status.HTTP_200_OK,
)
async def update_vendor_account_erp(
    vu_id: int,
    bg_task: BackgroundTasks,
    va_id: int,
    UpdateVendorAcc: schema.UpdateVendorAccount,
    db: Session = Depends(get_db),
):
    """This function creates an api route to update Vendor Account.

    It contains 4 parameters.
    :param vu_id: It is a path parameters that is of integer type, it
        provides the vendor user Id.
    :param va_id: It is a path parameters that is of integer type, it
        provides the vendor account Id.
    :param UpdateVendorAcc: It is Body parameter that is of a Pydantic
        class object, It takes member data for updating of Vendor
        account.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns a flag result.
    """
    return await crud.UpdateVendorAccERP(vu_id, va_id, UpdateVendorAcc, db)


@router.post("/newVendor/{vu_id}", status_code=status.HTTP_201_CREATED)
async def create_new_vendor_nonerp(
    request: Request, vu_id: int, db: Session = Depends(get_db)
):
    """This function creates an api route to create a new Vendor.

    It contains 3 parameters.
    :param vu_id: It is a path parameters that is of integer type, it
        provides the vendor user Id.
    :param NewVendor: It is Body parameter that is of a Pydantic class
        object, It takes member data for creating a new vendor.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns the newly created record.
    """
    NewVendor = await request.json()
    return await crud.NewVendor(vu_id, NewVendor, db)


@router.post(
    "/newVendorAccount/{vu_id}/idVendor/{v_id}", status_code=status.HTTP_201_CREATED
)
async def create_new_vendoracc_nonerp(
    request: Request, vu_id: int, v_id: int, db: Session = Depends(get_db)
):
    """This function creates an api route to create a new Vendor Account.

    It contains 4 parameters.
    :param vu_id: It is a path parameters that is of integer type, it
        provides the vendor user Id.
    :param v_id: It is a path parameters that is of integer type, it
        provides the vendor Id.
    :param NewVendorAcc: It is Body parameter that is of a Pydantic
        class object, It takes member data for creating a new vendor
        account.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns the newly created record.
    """
    NewVendorAcc = await request.json()
    return await crud.NewVendorAcc(vu_id, v_id, NewVendorAcc, db)


@router.get("/readVendorDetails/{vu_id}")
async def read_vendor_user(vu_id: int, db: Session = Depends(get_db)):
    """###This function creates an api route for Reading Vendor details.

    It contains 2 parameters.
    :param vu_id: It is a path parameters that is of integer type, it
        provides the vendor user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns vendor details associated with the vendor user
    """
    return await crud.read_vendor_details(db, vu_id)


# API to read all vendor list
@router.get("/vendorlist")
async def read_vendor(db: Session = Depends(get_db)):
    """###This function creates an api route for Reading Vendor list.

    It contains 2 parameters.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: it returns vendor list
    """
    return await crud.readvendor(db)


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


# API to read paginated vendor list


@router.get("/paginatedvendorlist/{u_id}")
async def read_vendor_list_paginated(
    u_id: int,
    ent_id: Optional[int] = None,
    ven_code: Optional[str] = None,
    onb_status: Optional[str] = None,
    offset: int = 1,
    limit: int = 10,
    ven_status: Optional[str] = None,
    vendor_type: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """###This function creates an api route for Reading paginated Vendor list.

    It contains 2 parameters.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: it returns vendor list
    """
    data = await crud.readpaginatedvendorlist(
        u_id,
        vendor_type,
        db,
        (offset, limit),
        {"ent_id": ent_id, "ven_code": ven_code, "onb_status": onb_status},
        ven_status,
    )
    return data


@router.get("/check_onboarded/{u_id}")
async def check_onboard(u_id: int, db: Session = Depends(get_db)):
    """This API will check if the vendor is onboarded or not for each entity.

    Parameter passed is vendor code
    """
    return await crud.checkonboarded(u_id, db)


# API to read all vendor list
@router.get("/vendorAccountPermissionList/{u_id}")
async def read_vendor_account_permission(u_id: int, db: Session = Depends(get_db)):
    """###This function creates an api route for Reading Vendor list.

    It contains 2 parameters.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: it returns vendor list
    """
    return await crud.read_vendor_account_permission(u_id, db)


# API to read particulaar vendor details using unique id
@router.get("/vendordetails/{v_id}")
async def read_vendordetails(v_id: int, db: Session = Depends(get_db)):
    """###This function creates an api route for Reading Vendor list.

    It contains 2 parameters.
    :param v_id: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: it returns vendor list
    """
    return await crud.readvendorbyid(db, v_id=v_id)


# API to read all vendor user list
@router.get("/vendorUserList/{vu_id}")
async def read_vendoruser(vu_id: int, db: Session = Depends(get_db)):
    """###This function creates an api route for Reading Vendor user list.

    It contains 2 parameters.
    :param vu_id: It is a path parameters that is of integer type, it
        provides the vendor user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns non admin vendor user list
    """
    return await crud.read_vendor_user(db, vu_id)


# API to read all vendor account
@router.get("/vendorAccount/{vu_id}")
async def read_vendoraccount(
    vu_id: int, ent_id: Optional[int] = None, db: Session = Depends(get_db)
):
    """###This function creates an api route for Reading Vendor accounts.

    It contains 2 parameters.
    :param vu_id: It is a path parameters that is of integer type, it
        provides the vendor user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns vendor account for a vendor user.
    """
    return await crud.readvendoraccount(vu_id, ent_id, db)


@router.get("/vendorAccountpo/{vu_id}")
async def read_vendoraccount_uploadpo(vu_id: int, db: Session = Depends(get_db)):
    """###This function creates an api route for Reading Vendor accounts.

    It contains 2 parameters.
    :param vu_id: It is a path parameters that is of integer type, it
        provides the vendor user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns vendor account for a vendor user.
    """
    return await crud.readvendoraccount_uploadpo(db, vu_id)


# To read vendor Sites
@router.get("/vendorSite/{u_id}/idVendor/{v_id}")
async def read_vendorsites(u_id: int, v_id: int, db: Session = Depends(get_db)):
    """###This function creates an api route for Reading Vendor accounts.

    It contains 2 parameters.
    :param u_id: It is a path parameters that is of integer type, it
        provides the user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns vendor account for a vendor user.
    """
    return await crud.readvendorsites(db, u_id, v_id)


# API to read all vendor account
@router.get("/submitVendorInvoice/{vu_id}")
async def submit_invoice_vendor(
    vu_id: int,
    re_upload: bool,
    bg_task: BackgroundTasks,
    inv_id: int,
    uploadtime: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """###This function creates an api route for Reading Vendor accounts.

    It contains 2 parameters.
    :param vu_id: It is a path parameters that is of integer type, it
        provides the vendor user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns vendor account for a vendor user.
    """
    return await crud.submit_invoice_vendor(
        vu_id, inv_id, re_upload, uploadtime, bg_task, db
    )


# To read unique Vendor Names along with their codes
@router.get("/vendorNameCode/{u_id}")
async def read_vendor_name_codes(
    u_id: int,
    offset: int = 0,
    limit: int = 0,
    ven_name: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """###This function creates an api route for Reading Vendor accounts.

    It contains 2 parameters.
    :param u_id: It is a path parameters that is of integer type, it
        provides the user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns vendor account for a vendor user.
    """
    return await crud.read_vendor_name_codes(db, u_id, (offset, limit), ven_name)


@router.get("/vendorNameCodeMatch/{u_id}")
async def getVendor_name_list_match(
    u_id: str,
    ven_name: str,
    ven_name_search: Optional[str] = None,
    offset: Optional[int] = 0,
    limit: Optional[int] = 0,
    db: Session = Depends(get_db),
):
    """This function creates an api route for Reading Vendor names.

    It contains 2 parameters.
    :param u_id: It is a path parameters that is of integer type, it
        provides the user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns a list of Vendor names .
    """
    return await crud.getVendor_name_list_match(
        u_id, ven_name, ven_name_search, (offset, limit), db
    )


@router.get("/getVendorOnboardStatus/{u_id}")
async def get_vendor_onbaord_status(
    u_id: str, ven_code: str, db: Session = Depends(get_db)
):
    """This function creates an api route for Reading Vendor names.

    It contains 2 parameters.
    :param u_id: It is a path parameters that is of integer type, it
        provides the user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It returns a list of Vendor names .
    """
    return await crud.get_vendor_onbaord_status(u_id, ven_code, db)


# API to read all vendor list
@router.get("/vendorlistDownloadStatus/{u_id}")
async def download_vendor(
    u_id: int,
    ent_id: Optional[int] = None,
    onb_status: Optional[str] = None,
    vendor_type: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """###This function creates an api route for Reading Vendor list.

    It contains 2 parameters.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: it returns vendor list
    """
    data = await crud.downloadvendorbyuid(
        u_id, vendor_type, db, {"ent_id": ent_id, "onb_status": onb_status}
    )
    return StreamingResponse(data, media_type="application/vnd.openxmlformats-")

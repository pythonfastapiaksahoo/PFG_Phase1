import io
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from pfg_app.auth import AuthHandler
from pfg_app.azuread.auth import get_admin_user, get_user_dependency
from pfg_app.azuread.schemas import AzureUser
from pfg_app.crud import VendorCrud as crud
from pfg_app.session.session import get_db

auth_handler = AuthHandler()

router = APIRouter(
    prefix="/apiv1.1/Vendor",
    tags=["Vendor"],
    # dependencies=[Depends(get_user_dependency(["DSD_ConfigPortal_User","Admin"]))],
    # dependencies=[Depends(get_admin_user)],
    responses={404: {"description": "Not found"}},
)


# API to read all vendor names
@router.get("/vendornamelist")
async def get_vendor_names_list(db: Session = Depends(get_db)):
    """API route to retrieve a list of all active vendor names.

    Parameters:
    ----------

    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    List of active vendor names.
    """
    return await crud.readvendorname(db)


# API to read paginated vendor list
@router.get("/paginatedvendorlist")
async def read_paginated_vendor_details(
    ent_id: Optional[int] = None,
    ven_code: Optional[str] = None,
    onb_status: Optional[str] = None,
    offset: int = 1,
    limit: int = 10,
    ven_status: Optional[str] = None,
    vendor_type: Optional[str] = None,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["DSD_ConfigPortal_User", "Admin"])),
):
    """API route to retrieve a paginated list of vendors based on various
    filters.

    Parameters:
    ----------

    ent_id : int, optional
        Entity ID to filter vendors by (default is None).
    ven_code : str, optional
        Vendor code to filter by (default is None).
    onb_status : str, optional
        Onboarding status to filter vendors (default is None).
    offset : int
        The page number for pagination (default is 1).
    limit : int
        Number of records per page (default is 10).
    ven_status : str, optional
        Vendor status to filter vendors (default is None).
    vendor_type : str, optional
        Type of vendor to filter by (default is None).
    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    List of vendor data filtered and paginated according to the input parameters.
    """
    data = await crud.readpaginatedvendorlist(
        user.idUser,
        vendor_type,
        db,
        (offset, limit),
        {"ent_id": ent_id, "ven_code": ven_code, "onb_status": onb_status},
        ven_status,
    )
    return data


@router.get("/download-vendor-list")
async def download_vendor_list(
    ent_id: Optional[int] = None,
    ven_code: Optional[str] = None,
    onb_status: Optional[str] = None,
    ven_status: Optional[str] = None,
    vendor_type: Optional[str] = None,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["DSD_ConfigPortal_User", "Admin"])),
    
):
    """API route to export the list of vendors based on various filters to
    excel.

    Parameters:
    ----------

    ent_id : int, optional
        Entity ID to filter vendors by (default is None).
    ven_code : str, optional
        Vendor code to filter by (default is None).
    onb_status : str, optional
        Onboarding status to filter vendors (default is None).
    ven_status : str, optional
        Vendor status to filter vendors (default is None).
    vendor_type : str, optional
        Type of vendor to filter by (default is None).
    db : Session
        Database session object, used to interact with the database.

    Returns:
    -------
    List of vendor data filtered and according to the input parameters.
    """

    # Call the readvendorlist function
    try:
        result = await crud.readvendorlist(
            user.idUser,
            vendor_type,
            db,
            {"ent_id": ent_id, "ven_code": ven_code, "onb_status": onb_status},
            ven_status,
        )

        # Check if the result is valid
        if "data" not in result or not result["data"]:
            raise HTTPException(status_code=404, detail="No vendor data found")

        # Convert the result data into a pandas DataFrame
        vendor_data = result["data"]

        # Normalize the vendor_data structure for DataFrame
        data_for_df = []
        for vendor in vendor_data:
            vendor_info = {
                "Vendor ID": vendor["Vendor"]["idVendor"],
                "Vendor Name": vendor["Vendor"]["VendorName"],
                "Vendor Code": vendor["Vendor"]["VendorCode"],
                "Vendor Type": vendor["Vendor"]["vendorType"],
                "Address": vendor["Vendor"]["Address"],
                "City": vendor["Vendor"]["City"],
                "Onboarded Status": vendor["OnboardedStatus"],
            }
            data_for_df.append(vendor_info)

        df = pd.DataFrame(data_for_df)

        # Export to Excel using BytesIO as an in-memory file
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Vendors")

        # Seek to the beginning of the stream
        output.seek(0)

        # Return the excel file as a streaming response
        headers = {"Content-Disposition": "attachment; filename=vendors.xlsx"}
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # noqa: E501
            headers=headers,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

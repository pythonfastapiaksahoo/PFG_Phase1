import secrets
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy.orm import Session

from pfg_app import settings

# from pfg_app import settings
from pfg_app.auth import AuthHandler
from pfg_app.azuread.auth import get_user_dependency
from pfg_app.crud import ERPIntegrationCrud as crud

from pfg_app.schemas.ERPIntegrationSchema import (
    PFGAccount,
    PFGDepartment,
    PFGProject,
    PFGProjectActivity,
    PFGReceipt,
    PFGStore,
    PFGVendor,
    PFGStrategicLedger,
    RequestPayload,
    VchrImpRequestBody,
)
from pfg_app.session.session import get_db

# Basic authentication scheme
security = HTTPBasic()
username = settings.erp_user
password = settings.erp_password


# Dependency function to verify admin credentials
def get_admin_user(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, username)
    correct_password = secrets.compare_digest(credentials.password, password)

    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


auth_handler = AuthHandler()
router = APIRouter(
    prefix="/apiv1.1/ERPIntegration",
    tags=["ERPIntegration"],
    dependencies=[Depends(get_user_dependency(["Admin"]))],
    # dependencies=[Depends(get_admin_user)],
    responses={404: {"description": "Not found"}},
)


@router.get("/getdepartmentmaster", status_code=status.HTTP_200_OK)
async def get_department_master(db: Session = Depends(get_db)):

    return await crud.getDepartmentMaster(db)


@router.get("/getstoremaster", status_code=status.HTTP_200_OK)
async def get_store_master(db: Session = Depends(get_db)):

    return await crud.getStoreMaster(db)


@router.get("/getvendormaster", status_code=status.HTTP_200_OK)
async def get_vendor_master(db: Session = Depends(get_db)):

    return await crud.getVendorMaster(db)


@router.get("/getaccountmaster", status_code=status.HTTP_200_OK)
async def get_account_master(db: Session = Depends(get_db)):

    return await crud.getAccountMaster(db)


@router.get("/getprojectmaster", status_code=status.HTTP_200_OK)
async def get_project_master(db: Session = Depends(get_db)):

    return await crud.getProjectMaster(db)


@router.get("/getprojectactivitymaster", status_code=status.HTTP_200_OK)
async def get_project_activity_master(db: Session = Depends(get_db)):

    return await crud.getProjectActivityMaster(db)


@router.get("/getreceiptmaster", status_code=status.HTTP_200_OK)
async def get_receipt_master(db: Session = Depends(get_db)):

    return await crud.getReceiptMaster(db)


@router.post(
    "/updatedepartmentmaster",
    # status_code=status.HTTP_200_OK
)
async def update_department_master(
    data: List[PFGDepartment], db: Session = Depends(get_db)
):
    return await crud.updateDepartmentMaster(data, db)


@router.post("/updatestoremaster", status_code=status.HTTP_200_OK)
async def update_store_master(data: List[PFGStore], db: Session = Depends(get_db)):

    return await crud.updateStoreMaster(data, db)


@router.post("/updatevendormaster", status_code=status.HTTP_200_OK)
async def update_vendor_master(data: List[PFGVendor], db: Session = Depends(get_db)):

    return await crud.updateVendorMaster(data, db)


@router.post("/updateaccountmaster", status_code=status.HTTP_200_OK)
async def update_account_master(data: List[PFGAccount], db: Session = Depends(get_db)):

    return await crud.updateAccountMaster(data, db)


@router.post("/updateprojectmaster", status_code=status.HTTP_200_OK)
async def update_project_master(data: List[PFGProject], db: Session = Depends(get_db)):

    return await crud.updateProjectMaster(data, db)


@router.post("/updateprojectactivitymaster", status_code=status.HTTP_200_OK)
async def update_project_activity_master(
    data: List[PFGProjectActivity], db: Session = Depends(get_db)
):

    return await crud.updateProjectActivityMaster(data, db)


@router.post("/updatereceiptmaster", status_code=status.HTTP_200_OK)
async def update_receipt_master(data: List[PFGReceipt], db: Session = Depends(get_db)):

    return await crud.updateReceiptMaster(data, db)

@router.post(
    "/updatestrategicledgermaster",
    # status_code=status.HTTP_200_OK
)
async def update_strategic_ledger_master(
    data: List[PFGStrategicLedger], db: Session = Depends(get_db)
):
    return await crud.updateStrategicLedgerMaster(data, db)

@router.get("/getstrategicledgermaster", status_code=status.HTTP_200_OK)
async def get_strategic_ledger_master(db: Session = Depends(get_db)):

    return await crud.getStrategicLedgerMaster(db)



# API endpoint to handle the invoice creation request
@router.post(
    "/createinvoicevoucher/{inv_id}"
    # response_model=VchrImpResponseBody
)
async def create_invoice_voucher(inv_id: int, db: Session = Depends(get_db)):
    try:
        # Process the request using the mock CRUD function
        response = crud.processInvoiceVoucher(inv_id, db)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# API endpoint to handle the invoice creation request
@router.post(
    "/pushPayloadToPST"
    # response_model=VchrImpRequestBody
)
async def push_payload_to_pst(payload_data: VchrImpRequestBody):
    try:
        # Process the request using the mock CRUD function
        response = crud.processInvoicePayload(payload_data)
        return response
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}

# API endpoint to handle the invoice status request
@router.post(
    "/pullinvoicestatus",
    # response_model=InvoiceResponse
)
async def pull_invoice_status(payload_data: RequestPayload):
    try:
        # Process the request using the mock CRUD function
        response = crud.pullInvoiceStatus(payload_data)
        return response
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}
    
    
# # API endpoint to handle the invoice status request
# @router.post(
#     "/bulkupdateinvoicestatus",
#     # response_model=InvoiceResponse
# )
# async def bulk_update_invoice_status(db: Session = Depends(get_db)):
#     try:
#         # Process the request using the mock CRUD function
#         response = crud.newbulkupdateInvoiceStatus(db)
#         return response
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# # API endpoint to handle the invoice status request
# @router.post(
#     "/bulkprocessvoucherdata",
#     # response_model=InvoiceResponse
# )
# async def bulk_process_voucher_data(db: Session = Depends(get_db)):
#     try:
#         # Process the request using the mock CRUD function
#         response = crud.bulkProcessVoucherData(db)
#         return response
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

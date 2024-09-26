import re
import sys
from datetime import datetime, timedelta
from io import BytesIO
from typing import Optional

import magic
import model
from auth import AuthHandler
from azure.identity import DefaultAzureCredential
from azure.storage.blob import (
    BlobServiceClient,
    ContainerSasPermissions,
    generate_blob_sas,
)
from crud import VendorPortalCrud as crud
from fastapi import APIRouter, Depends, File, Response, UploadFile
from schemas import InvoiceSchema as schema
from session import get_db
from sqlalchemy.orm import Session

sys.path.append("..")

credential = DefaultAzureCredential()


auth_handler = AuthHandler()

router = APIRouter(
    prefix="/apiv1.1/VendorPortal",
    tags=["Vendor Portal"],
    dependencies=[Depends(auth_handler.auth_wrapper)],
    responses={404: {"description": "Not found"}},
)


@router.get("/getponumbers/{vendorAccountID}")
async def get_po_numbers(
    vendorAccountID: int,
    ent_id: Optional[int] = None,
    u_id: Optional[int] = None,
    db: Session = Depends(get_db),
):
    """"""
    return await crud.read_po_numbers(u_id, vendorAccountID, ent_id, db)


@router.post("/addlabel/{invoicemodelID}")
async def add_label(
    invoicemodelID: int, labelDef: schema.TagDef, db: Session = Depends(get_db)
):
    """API to add a new label definition to a document model."""
    return await crud.add_label(invoicemodelID, labelDef, db)


@router.post("/addlineitem/{invoicemodelID}")
async def add_lineitem_tag(
    invoicemodelID: int, lineitemDef: schema.LineItemDef, db: Session = Depends(get_db)
):
    """API to add a new line item definition to a document model."""
    return await crud.add_label(invoicemodelID, lineitemDef, db)


@router.post("/uploadfile/{vendorAccountID}")
async def upload_file(
    vendorAccountID: int, file: UploadFile = File(...), db: Session = Depends(get_db)
):
    content = await file.read()
    configs = getOcrParameters(1, db)
    print(vendorAccountID)
    filecontent = BytesIO(content)
    filetype = magic.from_buffer(content, mime=True)
    accepted_filetype = ["application/pdf", "image/jpg", "image/jpeg", "image/png"]
    if filetype.lower() not in accepted_filetype:
        return {"code": 400, "message": "invalid file type"}
    filename = re.sub("[^A-Za-z0-9.]+", "", file.filename)
    containername = configs.ContainerName
    connection_str = configs.ConnectionString
    account_name = connection_str.split("AccountName=")[1].split(";AccountKey")[0]
    account_url = f"https://{account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(
        account_url=account_url, credential=credential
    )
    account_key = connection_str.split("AccountKey=")[1].split(";EndpointSuffix")[0]

    container_client = blob_service_client.get_container_client(containername)
    blob_client = container_client.upload_blob(
        name="Uploadeddocs/" + filename, data=filecontent, overwrite=True
    )
    sas_token = generate_blob_sas(
        account_name=blob_service_client.account_name,
        container_name=containername,
        blob_name="Uploadeddocs/" + filename,
        account_key=account_key,
        permission=ContainerSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=1),
        content_type=filetype,
    )
    filepath = f"https://{blob_service_client.account_name}.blob.core.windows.net/{containername}/Uploadeddocs/{filename}?{sas_token}"
    # file_location = f"Uploaded_docs/{file.filename}"
    # with open(file_location, "wb+") as buffer:
    #     shutil.copyfileobj(file.file, buffer)
    return {"filepath": filepath, "filename": filename, "filetype": filetype}


def getOcrParameters(customerID, db):
    try:
        configs = (
            db.query(model.FRConfiguration)
            .filter(model.FRConfiguration.idCustomer == customerID)
            .first()
        )
        return configs
    except Exception as e:
        return Response(
            status_code=500, headers={"DB Error": "Failed to get OCR parameters"}
        )

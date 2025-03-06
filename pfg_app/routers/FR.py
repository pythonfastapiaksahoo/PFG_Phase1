import io
import json
import os
import re
import time
import traceback
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional

import pandas as pd
from azure.storage.blob import BlobServiceClient
from fastapi import APIRouter, Depends, File, Request, Response, UploadFile, status
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.orm import Session

from pfg_app.azuread.schemas import AzureUser
import pfg_app.model as model
from pfg_app import settings
from pfg_app.auth import AuthHandler
from pfg_app.azuread.auth import get_admin_user, get_user_dependency
from pfg_app.core.utils import get_credential
from pfg_app.crud import FRCrud as crud
from pfg_app.FROps.model_validate import model_validate_final
from pfg_app.FROps.reupload import reupload_file_azure
from pfg_app.FROps.upload import upload_files_to_azure
from pfg_app.schemas import FRSchema as schema
from pfg_app.session.session import get_db

auth_handler = AuthHandler()


router = APIRouter(
    prefix="/apiv1.1/fr",
    tags=["Form Recogniser"],
    dependencies=[Depends(get_user_dependency(["DSD_ConfigPortal_User", "Admin"]))],
    # dependencies=[Depends(get_admin_user)],
    responses={404: {"description": "Not found"}},
)

temp_dir_obj = None


# Checked - used in the frontend
@router.get("/getfrconfig/{userID}", status_code=status.HTTP_200_OK)
async def get_fr_config(
    userID: int,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["DSD_ConfigPortal_User", "Admin"])),
    
    ):
    """<b> API route to get Form Recogniser Configuration. It contains
    following parameters.</b>

    - userID : Unique indetifier used to indentify a user
    - db: It provides a session to interact with the backend Database,
    that is of Session Object Type.
    - return: It returns the result status.
    """
    return await crud.getFRConfig(user.idUser, userID, db)


# Checked - used in the frontend
@router.get("/getfrmetadata/{documentId}")
async def get_fr_data(
    documentId: int,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["DSD_ConfigPortal_User", "Admin"])),
    ):
    return await crud.getMetaData(user.idUser, documentId, db)


# Checked - used in the frontend
@router.get("/getTrainTestResults/{modelId}")
async def get_test_data(
    modelId: int,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["DSD_ConfigPortal_User", "Admin"])),
    ):
    return await crud.getTrainTestRes(user.idUser, modelId, db)


# Checked - used in the frontend
@router.get("/getActualAccuracy/{type}")
async def getAccuracy(
    type: str,
    name: str,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["DSD_ConfigPortal_User", "Admin"])),
    ):
    return await crud.getActualAccuracy(user.idUser, type, name, db)


@router.get("/getAccuracyByEntity/{type}")
async def getAccuracyByEntity(
    type: str,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["DSD_ConfigPortal_User", "Admin"])),
    ):
    # Fetch the data from the database using the CRUD function
    data = await crud.getActualAccuracyByEntity(user.idUser, type, db)

    # Check if the data is a Response object (indicating an error)
    if isinstance(data, Response):
        return data  # If it's a Response object, return it directly

    # If data is a valid dictionary, proceed with processing
    records = []
    for entity, metrics in data.items():
        for key, value in metrics.items():
            records.append((key, value))  # Append a tuple with the key and its value

    # Create an Excel file in memory using a BytesIO buffer
    output = io.BytesIO()
    df = pd.DataFrame(records, columns=["Metric", "Details"])  # Set the column names
    df.to_excel(output, index=False, engine="openpyxl")  # Write DataFrame to Excel

    # Rewind the buffer to the beginning so it can be read
    output.seek(0)

    # Return the file as a streaming response with correct headers
    headers = {
        "Content-Disposition": (
            'attachment; filename="EntityLevelAccuracyReport.xlsx"'
        ),
        "Content-Type": (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    }
    return StreamingResponse(output, headers=headers)


# Checked - used in the frontend
@router.get("/entityTaggedInfo")
async def get_entity_levelTaggedInfo(
    tagtype: Optional[str] = None,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["DSD_ConfigPortal_User", "Admin"])),
):
    for f in os.listdir():
        if os.path.isfile(f) and f.endswith(".xlsx"):
            os.unlink(f)
    if tagtype == "vendor":
        filename = await crud.get_entity_level_taggedInfo(user.idUser, db)
    else:
        filename = await crud.get_entity_level_taggedInfo(user.idUser, db)
    return FileResponse(
        path=filename, filename=filename, media_type="application/vnd.ms-excel"
    )


# Checked - used in the frontend
@router.put("/update_metadata/{documentId}")
async def update_metadata(
    frmetadata:dict,
    documentId: int,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["DSD_ConfigPortal_User", "Admin"])),
):
    try:
        blb_fldr = frmetadata["FolderPath"]
        mandatoryheadertags = (
            frmetadata["mandatoryheadertags"].split(",")
            if frmetadata["mandatoryheadertags"] != ""
            else []
        )
        mandatorylinetags = (
            frmetadata["mandatorylinetags"].split(",")
            if frmetadata["mandatorylinetags"] != ""
            else []
        )
        optionallinertags = (
            frmetadata["optionallinertags"].split(",")
            if frmetadata["optionallinertags"] != ""
            else []
        )
        optionalheadertags = (
            frmetadata["optionalheadertags"].split(",")
            if frmetadata["optionalheadertags"] != ""
            else []
        )
        vendorname = (
            frmetadata["vendorName"]
            if "vendorName" in frmetadata
            else frmetadata["ServiceProviderName"]
        )
        vendor_code = None
        if "vendorCode" in frmetadata:
            syn = frmetadata["synonyms"]
            vendor_code = frmetadata["vendorCode"]
            # vendorname = vendorname.replace("'","''")  >> uncomment this if
            # it's MySQL DB
            # db.query(model.Vendor).filter(model.Vendor.VendorName == vendorname).update(
            #     {"Synonyms": json.dumps(syn)}
            # )
            # Update the specific row based on both VendorName and VendorCode
            if syn not in (None, "", [], {}):
                db.query(model.Vendor).filter(
                    model.Vendor.VendorName == vendorname,
                    model.Vendor.VendorCode == vendor_code,
                ).update({"Synonyms": json.dumps(syn)})
                db.commit()
            # del frmetadata["synonyms"]
            del frmetadata["vendorName"]
            del frmetadata["vendorCode"]
        else:
            del frmetadata["ServiceProviderName"]
        configs = getOcrParameters(1, db)
        containername = configs.ContainerName
        # connection_str = configs.ConnectionString
        # account_name = connection_str.split("AccountName=")[1].split(";AccountKey")[0]
        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=get_credential()
        )
        container_client = blob_service_client.get_container_client(containername)
        list_of_blobs = container_client.list_blobs(name_starts_with=blb_fldr)
        mandatoryheaderfields = []
        definitions: Dict[str, Dict[str, Any]] = {
            "tab_1_object": {
                "fieldKey": "tab_1_object",
                "fieldType": "object",
                "fieldFormat": "not-specified",
                "fields": [],
            }
        }
        lineitemupdates = []
        if len(mandatorylinetags) > 0:
            for m in mandatorylinetags:
                definitions["tab_1_object"]["fields"].append(
                    {
                        "fieldKey": m,
                        "fieldType": "string",
                        "fieldFormat": "not-specified",
                    }
                )
                lineitemupdates.append(m)
        if len(optionallinertags) > 0:
            for m in optionallinertags:
                definitions["tab_1_object"]["fields"].append(
                    {
                        "fieldKey": m,
                        "fieldType": "string",
                        "fieldFormat": "not-specified",
                    }
                )
                lineitemupdates.append(m)
        for b in list_of_blobs:
            if b.name.endswith("labels.json"):
                blob = (
                    blob_service_client.get_blob_client(containername, b.name)
                    .download_blob()
                    .readall()
                )
                labjson = json.loads(blob)
                for line in labjson["labels"]:
                    if not line["label"].startswith("tab_1"):
                        if (
                            line["label"] not in mandatoryheadertags
                            and line["label"] not in optionalheadertags
                        ):
                            index = next(
                                (
                                    index
                                    for (index, d) in enumerate(labjson["labels"])
                                    if d["label"] == line["label"]
                                ),
                                None,
                            )
                            if index:
                                del labjson["labels"][index]
                    else:
                        if line["label"].split("/")[2] not in lineitemupdates:
                            index = next(
                                (
                                    index
                                    for (index, d) in enumerate(labjson["labels"])
                                    if d["label"] == line["label"]
                                ),
                                None,
                            )
                            if index:
                                del labjson["labels"][index]

                bloblient = blob_service_client.get_blob_client(
                    containername, blob=b.name
                )
                bloblient.upload_blob(json.dumps(labjson), overwrite=True)

        for m in mandatoryheadertags:
            mandatoryheaderfields.append(
                {"fieldKey": m, "fieldType": "string", "fieldFormat": "not-specified"}
            )
        for m in optionalheadertags:
            mandatoryheaderfields.append(
                {"fieldKey": m, "fieldType": "string", "fieldFormat": "not-specified"}
            )
        if len(mandatorylinetags) > 0 or len(optionallinertags) > 0:
            mandatoryheaderfields.append(
                {
                    "fieldKey": "tab_1",
                    "fieldType": "array",
                    "fieldFormat": "not-specified",
                    "itemType": "tab_1_object",
                }
            )
            jso = {
                "$schema": "https://schema.cognitiveservices.azure.com/"
                + "formrecognizer/2021-03-01/fields.json",
                "definitions": definitions,
                "fields": mandatoryheaderfields,
            }
        else:
            jso = {
                "$schema": "https://schema.cognitiveservices.azure.com/"
                + "formrecognizer/2021-03-01/fields.json",
                "fields": mandatoryheaderfields,
            }
        data = json.dumps(jso)
        blobclient = blob_service_client.get_blob_client(
            containername, blob=blb_fldr + "/fields.json"
        )
        blobclient.upload_blob(data=data, overwrite=True)
        return await crud.updateMetadata(user.idUser, documentId, frmetadata, db)
    except BaseException:
        print(traceback.format_exc())
        return {"result": "Failed", "records": {}}


# Checked - used in the frontend
@router.post("/upload_blob")
# min_no: int, max_no: int, file_size_accepted: int, cnt_str: str, cnt_nm:
# str, local_path: str
def upload_blob(uploadParams: schema.FrUpload):
    local_pth = rf"{uploadParams.local_path}"
    accepted_file_type = ["pdf", "json", "txt", "jpg", "png"]
    nl_upload_status, fnl_upload_msg, blob_fld_name = upload_files_to_azure(
        uploadParams.min_no,
        uploadParams.max_no,
        accepted_file_type,
        uploadParams.file_size_accepted,
        uploadParams.cnx_str,
        uploadParams.cont_name,
        local_pth,
        uploadParams.folderpath,
    )
    return {
        "nl_upload_status": nl_upload_status,
        "fnl_upload_msg": fnl_upload_msg,
        "blob_fld_name": blob_fld_name,
    }


# Checked - used in the frontend
@router.post("/reupload_blob")
def reupload_blob(uploadParams: schema.FrReUpload):

    local_pth = rf"{uploadParams.local_path}"
    accepted_file_type = uploadParams.accepted_file_type.split(",")
    print(uploadParams.old_folder)
    reupload_status, reupload_status_msg, blob_fld_name = reupload_file_azure(
        uploadParams.min_no,
        uploadParams.max_no,
        accepted_file_type,
        uploadParams.file_size_accepted,
        uploadParams.cnx_str,
        uploadParams.cont_name,
        local_pth,
        uploadParams.old_folder,
        uploadParams.upload_type,
    )
    return {
        "nl_upload_status": reupload_status,
        "fnl_upload_msg": reupload_status_msg,
        "blob_fld_name": blob_fld_name,
    }


# Checked - used in the frontend
@router.post("/model_validate")
def model_validate(
    validateParas: schema.FrValidate,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["DSD_ConfigPortal_User", "Admin"])),
    ):

    mandatoryheadertags = (
        db.query(model.FRMetaData.mandatoryheadertags)
        .filter(model.FRMetaData.idInvoiceModel == validateParas.model_id)
        .scalar()
    )
    if validateParas.mandatory_field_check == 1:
        try:
            mand_fld_list = mandatoryheadertags.split(",")
        except BaseException:
            mand_fld_list = []
    else:
        mand_fld_list = []
    model_path = json.loads(validateParas.model_path)
    template_metadata: dict[str, Any] = {}
    fr_modelid = validateParas.fr_modelid
    model_validate_final_status, model_validate_final_msg, model_id, file_path, data = (
        model_validate_final(
            user.idUser,
            model_path,
            fr_modelid,
            validateParas.req_fields_accuracy,
            validateParas.req_model_accuracy,
            mand_fld_list,
            validateParas.cnx_str,
            validateParas.cont_name,
            validateParas.VendorAccount,
            validateParas.ServiceAccount,
            template_metadata,
            validateParas.mandatory_field_check,
            validateParas.folderPath,
        )
    )
    # update model ID
    # model_update = crud.updateInvoiceModel(
    #     validateParas.model_id, {"modelID": model_id}, db)
    # # store final data to json db
    # jsonDb = TinyDB('db.json')
    # jsonDb.insert({"model_id": model_id, "data": data})

    account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(
        account_url=account_url, credential=get_credential()
    )
    container_client = blob_service_client.get_container_client(validateParas.cont_name)
    jso = container_client.get_blob_client("db.json").download_blob().readall()
    jso = json.loads(jso)
    allids = []
    for m in jso:
        allids.append(m["model_id"])
    if model_id not in allids:
        jso.append({"model_id": model_id, "data": data})
    container_client.upload_blob(name="db.json", data=json.dumps(jso), overwrite=True)
    return {
        "model_validate_status": model_validate_final_status,
        "model_validate_msg": model_validate_final_msg,
        "model_id": model_id,
        "model_updates": {"result": "Updated", "records": {"modelID": model_id}},
        "final_data": data,
    }


@router.post("/uploadfolder")
async def create_upload_files(
    files: List[UploadFile] = File(...), db: Session = Depends(get_db)
):
    try:
        ts = str(time.time())
        dir_path = ts.replace(".", "_")
        configs = getOcrParameters(1, db)
        containername = configs.ContainerName

        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=get_credential()
        )
        container_client = blob_service_client.get_container_client(containername)
        for file in files:
            content = await file.read()
            file_location = f"{dir_path}/{re.sub('[^A-Za-z0-9.]+','',file.filename)}"
            container_client.upload_blob(
                name=file_location, data=BytesIO(content), overwrite=True
            )
        return {"filepath": dir_path}
    except Exception as e:
        print(e)
        return {"filepath": ""}


# Checked - used in the frontend
@router.post("/createmodel/{userID}", status_code=status.HTTP_200_OK)
async def create_invoice_model(
    userID: int,
    invoiceModel: schema.InvoiceModel,
    db: Session = Depends(get_db),
    # user=Depends(get_admin_user),
    user: AzureUser = Depends(get_user_dependency(["DSD_ConfigPortal_User", "Admin"])),
):
    """<b> API route to create a new Invoice model with associated tag
    definitions. It contains following parameters.</b>

    - userID : Unique indetifier used to indentify a user
    - invoiceModel: It is Body parameter that is of a Pydantic class object,
    creates member data for creating a new Invoice Model
    - tags: It is Body parameter that is of a Pydantic class object,
    creates member data for creating a set of tag definitions
    - db: It provides a session to interact with the backend Database,
    that is of Session Object Type.
    - return: It returns the result status.
    """

    return crud.createInvoiceModel(userID, user, invoiceModel, db)


# Checked (new) - used in the frontend
@router.get("/checkduplicatevendors/{vendoraccountID}/{modelname}")
async def getduplicates(
    vendoraccountID: int, modelname: str, db: Session = Depends(get_db)
):
    """This API checks if a same vendorname has multiple vendorcodes."""
    status = crud.check_same_vendors_same_entity(vendoraccountID, modelname, db)
    return status


@router.get("/copymodels/{vendoraccountID}/{modelname}")
async def copyallmodels(
    vendoraccountID: int, modelname: str, db: Session = Depends(get_db)
):
    """This API will copy the same model for a vendor."""
    status = crud.copymodels(vendoraccountID, modelname, db)
    return status


# Checked - used in the frontend
@router.post("/updatemodel/{modelID}", status_code=status.HTTP_200_OK)
async def update_invoicemodel(
    modelID: int,
    invoiceModel: schema.InvoiceModel,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["DSD_ConfigPortal_User", "Admin"])),
    # user=Depends(get_admin_user),
):
    """<b> API route to update invoice status. It contains following
    parameters.</b>

    - userID : Unique indetifier used to indentify a user
    - invoiceID : Unique indetifier used to indentify a particular invoice in database
    - invoiceStatus: It is Body parameter that is of a Pydantic class object,
    creates member data for updating invoice status
    - db: It provides a session to interact with the backend Database,
    that is of Session Object Type.
    - return: It returns the result status.
    """
    try:
        db.query(model.DocumentModel).filter(
            model.DocumentModel.idDocumentModel == modelID
        ).update(
            {
                "UpdatedOn": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "update_by": user.firstName if user.firstName else user.name,
            }
        )
        db.commit()
    except BaseException:
        print(traceback.format_exc())
    return crud.updateInvoiceModel(modelID, invoiceModel, db)


# Checked - used in the frontend
@router.get("/getmodellist/{vendorID}")
async def get_modellist(
    vendorID: int,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user_dependency(["DSD_ConfigPortal_User", "Admin"])),
    ):
    """<b> API route to get Form Recogniser Configuration. It contains
    following parameters.</b>

    - userID : Unique indetifier used to indentify a user
    - db: It provides a session to interact with the backend Database,
    that is of Session Object Type.
    - return: It returns the result status.
    """
    return crud.getmodellist(user.idUser, vendorID, db)


# Checked - used in the frontend
@router.get("/vendoraccount/{v_id}")
async def read_vendoraccount(v_id: int, db: Session = Depends(get_db)):
    db_user = crud.readvendoraccount(db, v_id=v_id)
    # if db_user is None:
    #     raise HTTPException(status_code=404, detail="User not found")
    return await db_user


# Displaying document rules list
@router.get("/documentrules", status_code=status.HTTP_200_OK)
async def read_docrules(db: Session = Depends(get_db)):
    return await crud.readdocumentrules(db)


# Displaying new doc rules list
@router.get("/documentrulesnew", status_code=status.HTTP_200_OK)
async def read_docrulesnew(db: Session = Depends(get_db)):
    return await crud.readnewdocrules(db)


def getOcrParameters(customerID, db):
    try:
        configs = (
            db.query(model.FRConfiguration)
            .filter(model.FRConfiguration.idCustomer == customerID)
            .first()
        )
        return configs
    except Exception as e:
        print(e)
        return Response(
            status_code=500, headers={"DB Error": "Failed to get OCR parameters"}
        )


# Checked - used in the frontend
@router.post("/updateEmailInfo", status_code=status.HTTP_200_OK)
async def updateEmailInfo(
    emailInfo: schema.EmailListenerInfo, db: Session = Depends(get_db)
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
    return await crud.update_email_info(emailInfo, db)


# Checked(Doubt) - used in the frontend
@router.get("/getEmailInfo", status_code=status.HTTP_200_OK)
async def getEmailInfo(db: Session = Depends(get_db)):
    """This function creates an API route to retrieve email listener info.

    :param db: It provides a session to interact with the backend
        Database, that is of Session Object Type.
    :return: It returns the email_listener_info data as a dictionary.
    """
    return await crud.get_email_info(db)


# Missed api's
@router.get("/getalltags")
async def get_fr_tags(tagtype: Optional[str] = None, db: Session = Depends(get_db)):
    return await crud.getall_tags(tagtype, db)


# check duplicate synonyms
@router.get("/checkduplicatesynonyms/{synonym:path}")
async def check_duplicate_synonyms(synonym: str, db: Session = Depends(get_db)):
    try:
        Synonyms = (
            db.query(model.Vendor).filter(model.Vendor.Synonyms.isnot(None)).all()
        )
        vendor_name = None
        for s in Synonyms:
            synonyms_list = json.loads(s.Synonyms, strict=False)
            if synonym.lower() in [s.lower() for s in synonyms_list]:
                # synonym is already present in the list
                vendor_name = s.VendorName
                return {
                    "status": "exists",
                    "message": f"Synonym already exists for {vendor_name}",
                }
        # synonym is not present in the list
        return {"status": "not exists", "message": "Synonym does not exist"}
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error in checking duplicate synonyms: {e}",
        }


# check duplicate synonyms
@router.get("/checkduplicatemodel")
async def check_duplicate_model(model_name: str, db: Session = Depends(get_db)):
    """Check if the given model_name already exists in the DocumentModel table.

    Args:
        db (Session): SQLAlchemy database session.
        model_name (str): The name of the model to check.

    Returns:
        str: A message indicating if the model is duplicate or not.
    """
    try:
        # Query the DocumentModel table using db.query
        existing_model = (
            db.query(model.DocumentModel)
            .filter(model.DocumentModel.modelName == model_name)
            .first()
        )

        if existing_model:
            return True
        else:
            return False
    except Exception as e:
        return f"An error occurred while checking for duplicates: {str(traceback.format_exc())}"

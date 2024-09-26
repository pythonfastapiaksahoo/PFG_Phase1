import json
import os
import re
import shutil
import sys
import time
import traceback
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional

import model
import pandas as pd
from auth import AuthHandler
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azuread.auth import get_admin_user
from crud import FRCrud as crud
from fastapi import APIRouter, Depends, File, Request, Response, UploadFile, status
from fastapi.responses import FileResponse
from FROps.model_validate import model_validate_final
from FROps.reupload import reupload_file_azure
from FROps.upload import upload_files_to_azure
from schemas import FRSchema as schema
from session import get_db
from sqlalchemy.orm import Session

sys.path.append("..")


auth_handler = AuthHandler()


router = APIRouter(
    prefix="/apiv1.1/fr",
    tags=["Form Recogniser"],
    dependencies=[Depends(get_admin_user)],
    responses={404: {"description": "Not found"}},
)

temp_dir_obj = None
credential = DefaultAzureCredential()


@router.get("/getfrconfig/{userID}", status_code=status.HTTP_200_OK)
async def get_fr_config(userID: int, db: Session = Depends(get_db)):
    """<b> API route to get Form Recogniser Configuration. It contains
    following parameters.</b>

    - userID : Unique indetifier used to indentify a user
    - db: It provides a session to interact with the backend Database,that is of Session Object Type.
    - return: It returns the result status.
    """
    return await crud.getFRConfig(userID, db)


@router.post("/updatefrconfig/{userID}", status_code=status.HTTP_200_OK)
async def update_fr_config(
    userID: int, frConfig: schema.FrConfig, db: Session = Depends(get_db)
):
    """<b> API route to update invoice status. It contains following
    parameters.</b>

    - userID : Unique indetifier used to indentify a user
    - invoiceID : Unique indetifier used to indentify a particular invoice in database
    - invoiceStatus: It is Body parameter that is of a Pydantic class object, creates member data for updating invoice status
    - db: It provides a session to interact with the backend Database,that is of Session Object Type.
    - return: It returns the result status.
    """

    return await crud.updateFRConfig(userID, frConfig, db)


@router.get("/getfrmetadata/{documentId}")
async def get_fr_data(documentId: int, db: Session = Depends(get_db)):
    return await crud.getMetaData(documentId, db)


@router.get("/getTrainTestResults/{modelId}")
async def get_test_data(modelId: int, db: Session = Depends(get_db)):
    return await crud.getTrainTestRes(modelId, db)


@router.get("/getActualAccuracy/{type}")
async def getAccuracy(type: str, name: str, db: Session = Depends(get_db)):
    return await crud.getActualAccuracy(type, name, db)


@router.get("/getAccuracyByEntity/{type}")
async def getAccuracyByEntity(type: str, db: Session = Depends(get_db)):
    for f in os.listdir():
        if os.path.isfile(f) and f.endswith("AccuracyReport.xlsx"):
            os.unlink(f)
    data = await crud.getActualAccuracyByEntity(type, db)
    pd.DataFrame(data).to_excel("EntityLevelAccuracyReport.xlsx")
    return FileResponse(
        path="EntityLevelAccuracyReport.xlsx",
        filename="EntityLevelAccuracyReport.xlsx",
        media_type="application/vnd.ms-excel",
    )


@router.get("/getalltags")
async def get_fr_tags(tagtype: Optional[str] = None, db: Session = Depends(get_db)):
    return await crud.getall_tags(tagtype, db)


@router.get("/entityTaggedInfo")
async def get_entity_levelTaggedInfo(
    tagtype: Optional[str] = None, db: Session = Depends(get_db)
):
    for f in os.listdir():
        if os.path.isfile(f) and f.endswith(".xlsx"):
            os.unlink(f)
    if tagtype == "vendor":
        filename = await crud.get_entity_level_taggedInfo(db)
    else:
        filename = await crud.get_entity_level_taggedInfo(db)
    return FileResponse(
        path=filename, filename=filename, media_type="application/vnd.ms-excel"
    )


@router.put("/update_metadata/{documentId}")
async def update_metadata(
    request: Request, documentId: int, db: Session = Depends(get_db)
):
    try:
        frmetadata = await request.json()
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
        if "vendorName" in frmetadata:
            syn = frmetadata["synonyms"]
            # vendorname = vendorname.replace("'","''")  >> uncomment this if
            # it's MySQL DB
            db.query(model.Vendor).filter(model.Vendor.VendorName == vendorname).update(
                {"Synonyms": json.dumps(syn)}
            )
            db.commit()
            # del frmetadata["synonyms"]
            del frmetadata["vendorName"]
        else:
            del frmetadata["ServiceProviderName"]
        configs = getOcrParameters(1, db)
        containername = configs.ContainerName
        connection_str = configs.ConnectionString
        account_name = connection_str.split("AccountName=")[1].split(";AccountKey")[0]
        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=credential
        )
        container_client = blob_service_client.get_container_client(containername)
        list_of_blobs = container_client.list_blobs(name_starts_with=blb_fldr)
        mandatoryheaderfields = []
        definitions: Dict[str, Dict[str, Any]] = {
            "tab_1_object": {
                "fieldKey": "tab_1_object",
                "fieldType": "object",
                "fieldFormat": "not-specified",
                "itemType": None,
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
                        "itemType": None,
                        "fields": None,
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
                        "itemType": None,
                        "fields": None,
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
                    if line["label"].startswith("tab_1") == False:
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
                    "fields": None,
                }
            )
            jso = {
                "$schema": "https://schema.cognitiveservices.azure.com/formrecognizer/2021-03-01/fields.json",
                "definitions": definitions,
                "fields": mandatoryheaderfields,
            }
        else:
            jso = {
                "$schema": "https://schema.cognitiveservices.azure.com/formrecognizer/2021-03-01/fields.json",
                "fields": mandatoryheaderfields,
            }
        data = json.dumps(jso)
        blobclient = blob_service_client.get_blob_client(
            containername, blob=blb_fldr + "/fields.json"
        )
        blobclient.upload_blob(data=data, overwrite=True)
        return await crud.updateMetadata(documentId, frmetadata, db)
    except BaseException:
        print(traceback.format_exc())
        return {"result": "Failed", "records": {}}


# @router.post("/createtags")
# async def create_fr_tags(Default_fields: schema.DefaultFieldsS):
#     return await crud.create_tags(Default_fields,db)


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


@router.post("/model_validate")
def model_validate(validateParas: schema.FrValidate, db: Session = Depends(get_db)):

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

    account_name = validateParas.cnx_str.split("AccountName=")[1].split(";AccountKey")[
        0
    ]
    account_url = f"https://{account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(
        account_url=account_url, credential=credential
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


@router.post("/uploadfile")
async def create_upload_file(file: UploadFile = File(...)):
    file_location = f"train_docs/{file.filename}"
    with open(file_location, "wb+") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return {"filepath": r"%s" % os.getcwd() + "/train_docs/" + file.filename}


@router.post("/uploadfolder")
async def create_upload_files(
    files: List[UploadFile] = File(...), db: Session = Depends(get_db)
):
    try:
        ts = str(time.time())
        dir_path = ts.replace(".", "_")
        configs = getOcrParameters(1, db)
        containername = configs.ContainerName
        connection_str = configs.ConnectionString
        account_name = connection_str.split("AccountName=")[1].split(";AccountKey")[0]
        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=credential
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


@router.post("/createmodel/{userID}", status_code=status.HTTP_200_OK)
async def create_invoice_model(
    userID: int,
    invoiceModel: schema.InvoiceModel,
    db: Session = Depends(get_db),
    user=Depends(get_admin_user),
):
    """<b> API route to create a new Invoice model with associated tag
    definitions. It contains following parameters.</b>

    - userID : Unique indetifier used to indentify a user
    - invoiceModel: It is Body parameter that is of a Pydantic class object, creates member data for creating a new Invoice Model
    - tags: It is Body parameter that is of a Pydantic class object, creates member data for creating a set of tag definitions
    - db: It provides a session to interact with the backend Database,that is of Session Object Type.
    - return: It returns the result status.
    """

    return crud.createInvoiceModel(userID, user, invoiceModel, db)


@router.get("/checkduplicatevendors/{vendoraccountID}/{modelname}")
async def getduplicates(
    vendoraccountID: int, modelname: str, db: Session = Depends(get_db)
):
    """This API checks if a vendor has multiple entities."""
    status = crud.check_same_vendors_different_entities(vendoraccountID, modelname, db)
    return status


@router.get("/checkduplicatesp/{serviceaccountID}/{modelname}")
async def getduplicatesSP(
    serviceaccountID: int, modelname: str, db: Session = Depends(get_db)
):
    """This API checks if a vendor has multiple entities."""
    status = crud.check_same_sp_different_entities(serviceaccountID, modelname, db)
    return status


@router.get("/copymodels/{vendoraccountID}/{modelname}")
async def copyallmodels(
    vendoraccountID: int, modelname: str, db: Session = Depends(get_db)
):
    """This API will copy the same model for a vendor for all different
    entities."""
    status = crud.copymodels(vendoraccountID, modelname, db)
    return status


@router.get("/copymodelsSP/{serviceaccountID}/{modelname}")
async def copyallmodelsSP(
    serviceaccountID: int, modelname: str, db: Session = Depends(get_db)
):
    """This API will copy the same model for a vendor for all different
    entities."""
    status = crud.copymodelsSP(serviceaccountID, modelname, db)
    return status


@router.get("/get_entities")
async def get_entities(db: Session = Depends(get_db)):
    """This api will return the list of Entities."""
    all_entities = db.query(model.Entity).all()
    return all_entities


@router.get("/get_all_entities/{u_id}")
async def get_all_entities(u_id: int, db: Session = Depends(get_db)):
    """This api will return the list of Entities per user entity access."""
    sub_query = (
        db.query(model.UserAccess.EntityID)
        .filter_by(UserID=u_id, isActive=1)
        .distinct()
    )
    all_entities = (
        db.query(model.Entity).filter(model.Entity.idEntity.in_(sub_query)).all()
    )
    return all_entities


@router.put("/update_entity/{eid}")
async def update_ent(
    eid: int, EntityModel: schema.Entity, db: Session = Depends(get_db)
):
    """This API will update the Entity values."""
    try:
        db.query(model.Entity).filter(model.Entity.idEntity == eid).update(
            dict(EntityModel)
        )
        db.commit()
        return {"message": "success"}
    except Exception as e:
        return {"message": f"exception {e}"}


@router.post("/updatemodel/{modelID}", status_code=status.HTTP_200_OK)
async def update_invoicemodel(
    modelID: int,
    invoiceModel: schema.InvoiceModel,
    db: Session = Depends(get_db),
    user=Depends(get_admin_user),
):
    """<b> API route to update invoice status. It contains following
    parameters.</b>

    - userID : Unique indetifier used to indentify a user
    - invoiceID : Unique indetifier used to indentify a particular invoice in database
    - invoiceStatus: It is Body parameter that is of a Pydantic class object, creates member data for updating invoice status
    - db: It provides a session to interact with the backend Database,that is of Session Object Type.
    - return: It returns the result status.
    """
    try:
        db.query(model.DocumentModel).filter(
            model.DocumentModel.idDocumentModel == modelID
        ).update(
            {
                "UpdatedOn": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "update_by": user.name,
            }
        )
        db.commit()
    except BaseException:
        print(traceback.format_exc())
    return crud.updateInvoiceModel(modelID, invoiceModel, db)


@router.get("/getmodellist/{vendorID}")
async def get_modellist(vendorID: int, db: Session = Depends(get_db)):
    """<b> API route to get Form Recogniser Configuration. It contains
    following parameters.</b>

    - userID : Unique indetifier used to indentify a user
    - db: It provides a session to interact with the backend Database,that is of Session Object Type.
    - return: It returns the result status.
    """
    return crud.getmodellist(vendorID, db)


@router.get("/getmodellistsp/{serviceProviderID}")
async def get_modellistsp(serviceProviderID: int, db: Session = Depends(get_db)):
    """<b> API route to get Form Recogniser Configuration. It contains
    following parameters.</b>

    - userID : Unique indetifier used to indentify a user
    - db: It provides a session to interact with the backend Database,that is of Session Object Type.
    - return: It returns the result status.
    """
    return crud.getmodellistsp(serviceProviderID, db)


@router.get("/getfinaldata/{modelID}")
async def get_finaldata(modelID: str, db: Session = Depends(get_db)):
    """<b> API route to get post processed final data. It contains following
    parameters.</b>

    - modelID : Unique indetifier used to indentify a model
    - return: It returns the result status.
    """
    try:
        configs = getOcrParameters(1, db)
        containername = configs.ContainerName
        connection_str = configs.ConnectionString
        account_name = connection_str.split("AccountName=")[1].split(";AccountKey")[0]
        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=credential
        )
        container_client = blob_service_client.get_container_client(containername)
        jso = container_client.get_blob_client("db.json").download_blob().readall()
        jso = json.loads(jso)
        data = list(filter(lambda x: x["model_id"] == modelID, jso))
        print(data)
        data = data[0]["data"]
        return {"final_data": data}
    except BaseException:
        return {"final_data": {}}


# API to read all vedor list


@router.get("/vendorlist", status_code=status.HTTP_200_OK)
async def read_vendor(db: Session = Depends(get_db)):
    return await crud.readvendor(db)


@router.get("/vendoraccount/{v_id}")
async def read_vendoraccount(v_id: int, db: Session = Depends(get_db)):
    db_user = crud.readvendoraccount(db, v_id=v_id)
    # if db_user is None:
    #     raise HTTPException(status_code=404, detail="User not found")
    return await db_user


@router.get("/serviceaccount/{s_id}")
async def read_spaccount(s_id: int, db: Session = Depends(get_db)):
    db_user = crud.readspaccount(db, s_id=s_id)
    # if db_user is None:
    #     raise HTTPException(status_code=404, detail="User not found")
    return await db_user


# API for adding new user item mapping


@router.post("/addItemMapping")
async def addItemMapping(mapData: schema.ItemMapping, db: Session = Depends(get_db)):
    """API route to add a new user item mapping.

    - mapData: It is Body parameter that is of a Pydantic class object, creates member data for new item mapping
    - db: It provides a session to interact with the backend Database,that is of Session Object Type.
    """
    return await crud.addItemMapping(mapData, db)


@router.post("/addfrmetadata/{m_id}/RuleId/{r_id}", status_code=status.HTTP_201_CREATED)
async def new_fr_meta_data(
    m_id: int, r_id: int, n_fr_mdata: schema.FrMetaData, db: Session = Depends(get_db)
):
    resp_fr_mdata = await crud.addfrMetadata(m_id, r_id, n_fr_mdata, db)
    return {"Result": "Updated", "Records": [resp_fr_mdata]}


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


@router.get("/getEmailInfo", status_code=status.HTTP_200_OK)
async def getEmailInfo(db: Session = Depends(get_db)):
    """This function creates an API route to retrieve email listener info.

    :param db: It provides a session to interact with the backend
        Database, that is of Session Object Type.
    :return: It returns the email_listener_info data as a dictionary.
    """
    return await crud.get_email_info(db)


# @router.post("/updateEmailInfo/{vu_id}", status_code=status.HTTP_201_CREATED)
# async def updateEmailInfo2(request: Request, vu_id: int,db: Session = Depends(get_db)):
#     """
#     This function creates an api route to create a new Vendor. It contains 3 parameters.
#     :param vu_id: It is a path parameters that is of integer type, it provides the vendor user Id.
#     :param NewVendor: It is Body parameter that is of a Pydantic class object, It takes member data for creating a new vendor.
#     :param db: It provides a session to interact with the backend Database,that is of Session Object Type.
#     :return: It returns the newly created record.
#     """
#     EmailInfo = await request.json()
#     return await crud.EmailInfo(vu_id, EmailInfo, db)

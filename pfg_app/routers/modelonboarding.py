import base64
import json
import os
import traceback
from datetime import datetime
from io import BytesIO
from typing import Optional

import pytz
from azure.storage.blob import BlobServiceClient
from fastapi import APIRouter, Depends, File, Request, Response, UploadFile, status
from pdf2image import convert_from_bytes
from sqlalchemy.orm import Session

import pfg_app.model as model
from pfg_app import settings
from pfg_app.auth import AuthHandler
from pfg_app.azuread.auth import get_admin_user
from pfg_app.core import azure_fr as core_fr
from pfg_app.core.utils import convert_dates, get_credential  # , get_container_sas
from pfg_app.crud import ModelOnBoardCrud as crud
from pfg_app.FROps import util as ut
from pfg_app.logger_module import logger
from pfg_app.schemas import InvoiceSchema as schema
from pfg_app.schemas.modelOnboarding import ModelTrainSchema
from pfg_app.session.session import get_db

auth_handler = AuthHandler()

router = APIRouter(
    prefix="/apiv1.1/ModelOnBoard",
    tags=["Model On-Boarding"],
    dependencies=[Depends(get_admin_user)],
    responses={404: {"description": "Not found"}},
)


# Checked - used in the frontend
@router.post(
    "/newModel/{modelID}/{userId}",
    status_code=status.HTTP_200_OK,
    response_model=schema.Response,
)
async def onboard_invoice_model(
    request: Request, userId: int, modelID: int, db: Session = Depends(get_db)
):
    """<b>API route to onboard a new invoice template Form Recognizer
    output.</b>

    - userID : Unique indetifier used to indentify a user
    - invoiceTemplate: The Form Recognizer output, passed as API body
    - db: It provides a session to interact with
    the backend Database,that is of Session Object Type.
    - return: It returns the result status.

    <b> CRUD Ops</b>
    1. Create Model from Invoice template data
    2. Add associated tag definitions of model from the template data to db
    3. Add associated line item fields into line item definition table
    """
    invoiceTemplate = await request.json()
    return crud.ParseInvoiceData(modelID, userId, invoiceTemplate, db)


# Checked - used in the frontend
@router.get("/get_tagging_info/{documentId}")
async def get_tagging_details(
    request: Request, documentId: int, db: Session = Depends(get_db)
):
    try:
        folder_path = request.headers.get("path")
        configs = getOcrParameters(1, db)
        container = configs.ContainerName
        savelabels = "{}"

        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=get_credential()
        )
        container_client = blob_service_client.get_container_client(container)
        list_of_blobs = container_client.list_blobs(name_starts_with=folder_path)
        file_list = []
        fields = crud.getFields(documentId, db)
        if fields is None:
            fields = '{"fields":[]}'
        fields = json.loads(fields)
        fieldexist = False
        acceptedext = [".pdf", ".png", ".jpeg", ".jpg"]
        for b in list_of_blobs:
            if b.name.endswith("fields.json"):
                blob = (
                    blob_service_client.get_blob_client(container, b.name)
                    .download_blob()
                    .readall()
                )
                fields = json.loads(blob)
            if os.path.splitext(b.name)[1].lower() in acceptedext:
                bdata = (
                    blob_service_client.get_blob_client(container, b.name)
                    .download_blob()
                    .readall()
                )
                # bdata = blob_client_service.get_blob_to_bytes(container,b.name)
                obj = {}
                filename = b.name.split("/")[-1]
                if os.path.splitext(b.name)[1].lower() == ".pdf":
                    images = convert_from_bytes(bdata, dpi=92, poppler_path="/usr/bin")
                    # images = convert_from_bytes(
                    #     bdata, dpi=92, poppler_path=r"C:\\poppler-24.07.0\\Library\\bin" # noqa: E501
                    # )
                    for i in images:
                        im_bytes = BytesIO()
                        i.save(im_bytes, format="JPEG")
                        b64 = base64.b64encode(im_bytes.getvalue()).decode("utf-8")
                        obj[filename] = "data:image/jpeg;base64," + str(b64)
                        break
                else:
                    b64 = base64.b64encode(BytesIO(bdata).getvalue()).decode("utf-8")
                    obj[filename] = "data:image/jpeg;base64," + str(b64)
                file_list.append(obj)
        if len(fields["fields"]) > 0:
            fieldexist = True
        return {
            "message": "success",
            "file_list": file_list,
            "fields": fields,
            "fieldexist": fieldexist,
            "savedlabels": savelabels,
        }
    except Exception as e:
        return {
            "message": f"exception {e}",
            "file_list": [],
            "fields": {},
            "fieldexist": False,
            "savedlabels": "{}",
        }
    finally:
        db.close()


@router.get("/get_model_status/{idDocumentModel}")
async def get_model_status(idDocumentModel: int, db: Session = Depends(get_db)):
    """This route returns the status of the model.

    0 - Inactive
    1 - Active
    """
    return crud.get_model_status(idDocumentModel, db)


@router.post("/update_model_status/{idDocumentModel}/{is_active}")
async def update_model_status(
    idDocumentModel: int, is_active: int, db: Session = Depends(get_db)
):
    """This route updates the status of the model.

    success - if the status is updated successfully
    exception - if there is an exception
    """
    return crud.update_model_status(idDocumentModel, is_active, db)


# Checked - used in the frontend
@router.get("/get_labels_info/{filename}")
async def get_tagging_details_labels_info(
    request: Request, filename: str, db: Session = Depends(get_db)
):
    try:
        folder_path = request.headers.get("folderpath")
        configs = getOcrParameters(1, db)
        containername = configs.ContainerName

        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=get_credential()
        )
        container_client = blob_service_client.get_container_client(containername)
        list_of_blobs = container_client.list_blobs(name_starts_with=folder_path)
        labels = {"blob": {}, "labelexist": False}
        for b in list_of_blobs:
            if b.name == folder_path + "/" + filename + ".labels.json":
                try:
                    blob = (
                        blob_service_client.get_blob_client(containername, b.name)
                        .download_blob()
                        .readall()
                    )
                    # Apply the function to add the missing page info
                    labels_with_pages = core_fr.add_page_to_labels(
                        json.loads(blob.decode("utf-8"))
                    )
                    labels = {"blob": labels_with_pages, "labelexist": True}
                    return {"message": "success", "labels": labels}
                except BaseException:
                    logger.error(traceback.format_exc())
                    labels = {"blob": {}, "labelexist": False}
                    return {"message": "error", "labels": labels}
        return {"message": "success", "labels": labels}
    except Exception as e:
        return {"message": f"exception {e}", "labels": {}}
    finally:
        db.close()


@router.post("/save_labels_file")
async def save_labels_file(request: Request):
    try:
        body = await request.json()
        container = body["container"]
        filename = body["filename"]
        # connstr = body['connstr']
        labeljson = body["labelJson"]
        # Apply the function to add the missing page info
        labeljson = core_fr.add_page_to_labels(labeljson)
        # savejson = body["saveJson"]
        # idDocumentModel = body["documentId"]
        # crud.updateLabels(idDocumentModel,savejson,db)
        blob_name = filename + ".labels.json"
        json_string = json.dumps(labeljson)
        # account_name = connstr.split("AccountName=")[1].split(";AccountKey")[0]
        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=get_credential()
        )
        bloblient = blob_service_client.get_blob_client(container, blob=blob_name)
        bloblient.upload_blob(json_string, overwrite=True)
        return {"message": "success"}
    except Exception as e:
        return {"message": f"exception {e}"}


# Checked - used in the frontend
@router.post("/save_fields_file")
async def save_fields_file(request: Request, db: Session = Depends(get_db)):
    try:
        body = await request.json()
        fields = body["fields"]
        documentId = body["documentId"]
        folderpath = body["folderpath"]
        container = body["container"]
        crud.updateFields(documentId, json.dumps(fields), db)
        blob_name = folderpath + "/" + "fields.json"
        json_string = json.dumps(fields)
        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=get_credential()
        )
        blobclient = blob_service_client.get_blob_client(container, blob=blob_name)
        blobclient.upload_blob(json_string, overwrite=True)
        return {"message": "success"}
    except Exception as e:
        return {"message": f"exception {e}"}
    finally:
        db.close()


# Checked - used in the frontend
@router.get("/get_analyze_result/{container}")
async def get_result(
    request: Request, container: str, db: Session = Depends(get_db)
):  #
    try:
        filename = request.headers.get("filename")
        # connstr = request.headers.get("connstr")
        # frconfigs = getOcrParameters(1, db)
        # fr_endpoint = frconfigs.Endpoint
        # fr_key = frconfigs.Key1
        # storage = request.headers.get("account")
        # account_key = connstr.split("AccountKey=")[1].split(";EndpointSuffix")[0]
        ext = os.path.splitext(filename)[1]
        content_type = ""
        if ext == ".jpg":
            content_type = "image/jpg"
        elif ext == ".jpeg":
            content_type = "image/jpeg"
        elif ext == ".png":
            content_type = "image/png"
        else:
            content_type = "application/pdf"

        # token = generate_blob_sas(
        #     account_name=storage,
        #     container_name=container,
        #     blob_name=filename,
        #     account_key=account_key,
        #     permission=ContainerSasPermissions(
        #         read=True, write=True, list=True, delete=True
        #     ),
        #     start=datetime.utcnow() - timedelta(hours=3),
        #     expiry=datetime.utcnow() + timedelta(hours=3),
        #     content_type=content_type,
        # )
        # file_url = (
        #     "https://"
        #     + settings.storage_account_name
        #     + ".blob.core.windows.net/"
        #     + container
        #     + "/"
        #     + filename
        #     # + "?"
        #     # + token
        # )
        # Generate the URL for the 'get_file' route from another router
        file_url = str(
            request.url_for("get_blob_file").include_query_params(
                container_name=container, blob_path=filename
            )
        )
        # print(fr_endpoint)
        # url = f"{fr_endpoint}/formrecognizer/documentModels/\
        #     prebuilt-layout:analyze?api-version=2023-07-31\
        #         &stringIndexType=utf16CodeUnit&features=ocrHighResolution"
        # account_name = connstr.split("AccountName=")[1].split(";AccountKey")[0]
        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=get_credential()
        )
        blob_client = blob_service_client.get_blob_client(
            container, blob=filename + ".ocr.json"
        )
        if blob_client.exists():
            bdata = blob_client.download_blob().readall()
            json_result = json.loads(bdata)

            # Convert snake_case to camelCase and flatten polygons
            camel_case_json = core_fr.convert_snake_to_camel(json_result)
            json_result = core_fr.process_polygons(camel_case_json)

            # Check if the JSON result is of old Format , if not convert it
            #  to old format
            if "status" not in json_result:
                # This is the new format, convert it to the old format
                # Get the current time in UTC
                utc_timezone = pytz.utc
                current_time_utc = datetime.now(utc_timezone)

                # Format the time in ISO 8601 format, similar to your example
                timestamp = current_time_utc.isoformat()

                # Example old_structure using dynamic time zone-aware timestamps
                old_structure = {
                    "status": "succeeded",
                    "createdDateTime": timestamp,  # Current timestamp in UTC
                    "lastUpdatedDateTime": timestamp,  # Current timestamp in UTC
                    "analyzeResult": json_result,  # Your transformed data
                }

                # json_string = json.dumps(old_structure)
                json_string = old_structure
                blob_client.upload_blob(json.dumps(json_string), overwrite=True)
            else:
                # json_string = json.dumps(json_result)
                json_string = json_result

        else:
            # headers = {
            #     "Content-Type": content_type,
            #     "Ocp-Apim-Subscription-Key": fr_key,
            # }
            blob_client1 = blob_service_client.get_blob_client(container, blob=filename)
            bdata = blob_client1.download_blob().readall()
            body = BytesIO(bdata)
            # json_result = fr.analyzeForm(url=url, headers=headers, body=body)
            json_result = core_fr.analyze_form(
                input_file=body,
                endpoint=settings.form_recognizer_endpoint,
                api_version=settings.api_version,
                invoice_model_id="prebuilt-layout",
            )
            if (
                "message" in json_result
                and json_result["message"] == "failure to fetch"
            ):
                return {
                    "message": "failure",
                    "json_result": {},
                    "file_url": "",
                    "content_type": "",
                }
            # json_result = util.correctAngle(json_result)

            date_corrected_json = convert_dates(json_result)

            # Convert snake_case to camelCase and flatten polygons
            camel_case_json = core_fr.convert_snake_to_camel(date_corrected_json)
            date_corrected_json = core_fr.process_polygons(camel_case_json)

            # Save the JSON result to the Azure Blob Storage after wrapping it in the
            # required old top-level fields like status, createdDateTime,
            # lastUpdatedDateTime, and analyzeResult

            # Get the current time in UTC
            utc_timezone = pytz.utc
            current_time_utc = datetime.now(utc_timezone)

            # Format the time in ISO 8601 format, similar to your example
            timestamp = current_time_utc.isoformat()

            # Example old_structure using dynamic time zone-aware timestamps
            old_structure = {
                "status": "succeeded",
                "createdDateTime": timestamp,  # Current timestamp in UTC
                "lastUpdatedDateTime": timestamp,  # Current timestamp in UTC
                "analyzeResult": date_corrected_json,  # Your transformed data
            }

            # json_string = json.dumps(old_structure)
            json_string = old_structure
            blob_client.upload_blob(json.dumps(json_string), overwrite=True)
        return {
            "message": "success",
            "json_result": json_string,
            "file_url": file_url,
            "content_type": content_type,
        }
    except Exception as e:
        return {
            "message": f"exception {e}",
            "json_result": {},
            "file_url": "",
            "content_type": "",
        }
    finally:
        db.close()


# Checked - used in the frontend
@router.post("/test_analyze_result/{modelid}")
async def get_test_result(
    modelid: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    try:
        metadata, f, valid_file = await ut.get_file(file, 900)
        if not valid_file:
            return {"message": "File is invalid"}
        acceptedfiletype = ["application/pdf", "image/jpg", "image/png", "image/jpeg"]
        contenttype = "application/pdf"
        if metadata[0] in acceptedfiletype:
            contenttype = metadata[0]
        fileurl = ""
        if contenttype != "application/pdf":
            b64 = base64.b64encode(f.getvalue()).decode("utf-8")
            fileurl = "data:image/jpeg;base64," + str(b64)
        body = f.getvalue()
        # json_result = fr.analyzeForm(url=url, headers=headers, body=body)
        json_result = core_fr.analyze_form(
            body, settings.form_recognizer_endpoint, settings.api_version, modelid
        )
        if "message" in json_result and json_result["message"] == "failure to fetch":
            return {
                "message": "failure",
                "json_result": {},
                "content_type": "",
                "url": "",
            }
        # json_result = ut.correctAngle(json_result)
        return {
            "message": "success",
            "json_result": json_result,
            "content_type": contenttype,
            "url": fileurl,
        }
    except Exception as e:
        return {
            "message": f"exception {e}",
            "json_result": {},
            "content_type": "",
            "url": "",
        }
    finally:
        db.close()


# Checked - used in the frontend
@router.get("/get_training_result/{documentmodelId}")
async def get_training_res(documentmodelId: int, db: Session = Depends(get_db)):
    try:
        training_res = crud.get_fr_training_result(db, documentmodelId)
        return {"message": "success", "result": [training_res]}
    except Exception as e:
        return {"message": f"exception {e}", "result": []}
    finally:
        db.close()


# Checked - used in the frontend
@router.post("/create_training_result")
async def create_result(request: Request, db: Session = Depends(get_db)):
    try:
        req_body = await request.json()
        fr_result = req_body["fr_result"]
        docid = req_body["docid"]
        create_result = crud.updateTrainingResult(docid, fr_result, db)
        return {"message": create_result}
    except Exception:
        logger.error(traceback.format_exc())
        return {"message": "exception"}
    finally:
        db.close()


# Checked - used in the frontend
@router.post("/create_compose_result")
async def create_result_compose_result(request: Request, db: Session = Depends(get_db)):
    try:
        req_body = await request.json()
        if req_body["vendorAccountId"]:
            vendor = (
                db.query(model.VendorAccount.vendorID)
                .filter(
                    model.VendorAccount.idVendorAccount == req_body["vendorAccountId"]
                )
                .first()
            )
            vendor = (
                db.query(model.Vendor.VendorName)
                .filter(model.Vendor.idVendor == vendor.vendorID)
                .first()
            )
            ven_ids = (
                db.query(model.Vendor.idVendor)
                .filter(model.Vendor.VendorName == vendor.VendorName)
                .all()
            )
            ven_ids = [v[0] for v in ven_ids]
            ven_acc_ids = (
                db.query(model.VendorAccount.idVendorAccount)
                .filter(model.VendorAccount.vendorID.in_(ven_ids))
                .all()
            )
            for v in ven_acc_ids:
                req_body["vendorAccountId"] = v[0]
                crud.createOrUpdateComposeModel(req_body, db)
        else:
            serviceprovider = (
                db.query(model.ServiceProvider.ServiceProviderName)
                .filter(
                    model.ServiceProvider.idServiceProvider
                    == req_body["serviceproviderID"]
                )
                .first()
            )
            ser_ids = (
                db.query(model.ServiceProvider.idServiceProvider)
                .filter(
                    model.ServiceProvider.ServiceProviderName
                    == serviceprovider.ServiceProviderName
                )
                .all()
            )
            for s in ser_ids:
                req_body["serviceproviderID"] = s[0]
                crud.createOrUpdateComposeModel(req_body, db)

        return {"message": "success"}
    except Exception:
        logger.error(traceback.format_exc())
        return {"message": "exception"}
    finally:
        db.close()


# Checked - used in the frontend
@router.post("/compose_model")
async def compose_model(request: Request, db: Session = Depends(get_db)):
    try:
        req_body = await request.json()
        model_ids = req_body["modelIds"]
        model_id = req_body["modelName"]

        core_fr.compose_model(settings.form_recognizer_endpoint, model_id, model_ids)

        json_response = core_fr.get_model(settings.form_recognizer_endpoint, model_id)

        return {"message": "success", "result": json_response}
    except Exception as e:
        return {"message": f"exception {e}", "result": {}}
    finally:
        db.close()


# Checked - used in the frontend
@router.post("/train-model")
async def train_model(data: ModelTrainSchema, db: Session = Depends(get_db)):
    try:
        req_body = data.dict()
        # req_body = await request.json()
        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=get_credential()
        )
        folder_path = req_body["folderpath"]
        container = req_body["container"]
        container_client = blob_service_client.get_container_client(container)
        list_of_blobs = container_client.list_blobs(name_starts_with=folder_path)
        blob_list = []
        for b in list_of_blobs:
            if b.name != "fields.json":
                blob_list.append(b.name)
        acceptedext = [".pdf", ".png", ".jpeg", ".jpg"]
        file_counter = 0
        error_list = []
        for b in blob_list:
            if os.path.splitext(b)[1].lower() in acceptedext:
                file_counter += 1
                if b + ".labels.json" not in blob_list:
                    error_list.append(
                        {"file": b, "message": "File missing labels.json file"}
                    )
                if b + ".ocr.json" not in blob_list:
                    error_list.append(
                        {"file": b, "message": "File missing ocr.json file"}
                    )
        if len(error_list) > 0:
            return {"errorlist": error_list}
        if file_counter < 5:
            return {"error": "Training files should be more than 5"}
        model_id = req_body["modelName"]
        container_url = (
            f"https://{settings.storage_account_name}.blob.core.windows.net/{container}"
        )
        # Get the SAS token for the container
        # sas_token = get_container_sas(container)
        # container_url += "?" + sas_token #TODO: Check if this is needed

        json_resp = core_fr.get_model(
            settings.form_recognizer_endpoint, model_id=model_id
        )
        if json_resp["result"] is None:

            json_resp = core_fr.train_model(
                settings.form_recognizer_endpoint,
                model_id,
                container_url,
                prefix=folder_path + "/",
            )
            if json_resp["message"] != "success":
                return {"message": json_resp["message"], "result": {}}
        return {"message": json_resp["message"], "result": json_resp["result"]}
    except Exception as e:
        print(traceback.format_exc())
        return {"message": f"exception {e}", "result": {}, "post_resp": ""}
    finally:
        db.close()


def getOcrParameters(customerID, db):
    try:
        configs = (
            db.query(model.FRConfiguration)
            .filter(model.FRConfiguration.idCustomer == customerID)
            .first()
        )
        return configs
    except Exception:
        logger.error(traceback.format_exc())
        return Response(
            status_code=500, headers={"DB Error": "Failed to get OCR parameters"}
        )


# Checked - used in the frontend
@router.delete("/DeleteBlob")
async def delete_blob_container(blob: str, db: Session = Depends(get_db)):
    try:
        return await crud.delete_blob_container(db, blob)
    except BaseException:
        print(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


# Checked - used in the frontend
@router.get("/runlayout/{folder:path}")
async def get_result_run_layout(folder: str, db: Session = Depends(get_db)):
    try:
        frconfigs = getOcrParameters(1, db)
        # fr_endpoint = frconfigs.Endpoint
        # connstr = frconfigs.ConnectionString
        containername = frconfigs.ContainerName
        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=get_credential()
        )
        container_client = blob_service_client.get_container_client(containername)
        folder_with_slash = f"{folder}/" if folder else ""
        for blob in container_client.list_blobs(name_starts_with=folder_with_slash):
            if blob.name.endswith((".pdf", ".png", ".jpg", ".jpeg")):

                blob_client = blob_service_client.get_blob_client(
                    containername, blob=blob.name
                )
                bdata = blob_client.download_blob().readall()
                body = BytesIO(bdata)
                json_result = core_fr.analyze_form(
                    body,
                    settings.form_recognizer_endpoint,
                    settings.api_version,
                    "prebuilt-layout",
                )

                if (
                    "message" in json_result
                    and json_result["message"] == "failure to fetch"
                ):
                    return {"message": "failure"}
                # json_result = util.correctAngle(json_result)
                json_string = json.dumps(json_result)
                blob_client_ocr = blob_service_client.get_blob_client(
                    containername, blob=blob.name + ".ocr.json"
                )
                blob_client_ocr.upload_blob(json_string, overwrite=True)
        return {"message": "success"}
    except Exception as e:
        print(traceback.format_exc())
        return {"message": f"exception {e}"}
    finally:
        db.close()


# Checked - used in the frontend
@router.get("/autoLabels/{folder:path}")
async def get_labels_pdf_image(
    folder: str, filename: Optional[str] = None, db: Session = Depends(get_db)
):
    try:
        ocr_engine = "Azure Form Recognizer 3.1"
        configs = getOcrParameters(1, db)
        containername = configs.ContainerName
        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=get_credential()
        )
        container_client = blob_service_client.get_container_client(containername)
        blob_client = container_client.get_blob_client(f"{folder}/fields.json")
        fields = json.loads(blob_client.download_blob().content_as_text())
        line = []
        if (
            "definitions" in fields
            and "tab_1_object" in fields["definitions"]
            and len(fields["definitions"]["tab_1_object"]["fields"]) > 0
        ):
            line = [
                _l["fieldKey"] for _l in fields["definitions"]["tab_1_object"]["fields"]
            ]
        keys = {"header": [h["fieldKey"] for h in fields["fields"]], "line": line}
        if filename:
            file = f"{folder}/{filename}"
            blob_client = container_client.get_blob_client(file)
            blob_data = blob_client.download_blob().readall()

            # if file.endswith((".png", ".jpg", ".jpeg")):
            #     getlabel_image(blob_data, file, db, keys, ocr_engine)

            # elif file.endswith(".pdf"):
            return getlabels(blob_data, file, db, keys, ocr_engine)

        else:
            files = []
            folder_with_slash = f"{folder}/" if folder else ""
            for blob in container_client.list_blobs(name_starts_with=folder_with_slash):
                if blob.name.endswith((".pdf", ".png", ".jpg", ".jpeg")):
                    files.append(blob.name)

            for file in files:
                blob_client = container_client.get_blob_client(file)
                blob_data = blob_client.download_blob().readall()

                # if file.endswith((".png", ".jpg", ".jpeg")):
                #     getlabel_image(blob_data, file, db, keys, ocr_engine)

                # elif file.endswith(".pdf"):
            return getlabels(blob_data, file, db, keys, ocr_engine)

    except Exception as ex:
        print(ex)
        return "Exception"


def getlabels(filedata, document_name, db, keyfields, ocr_engine):
    try:
        # configs = getOcrParameters(1, db)
        get_resp = {}
        folderpath = "/".join(document_name.split("/")[:2])
        language = (
            db.query(model.FRMetaData.temp_language)
            .filter_by(FolderPath=folderpath)
            .first()
        )
        # try:
        #     post_resp = requests.post(
        #         f"{fr_endpoint}/\
        #             formrecognizer/documentModels/prebuilt-invoice:analyze\
        #                 ?api-version=2023-07-31&locale={language[0]}&\
        #                     stringIndexType=textElements&features=ocrHighResolution",
        #         data=filedata,
        #         headers={
        #             "Content-Type": "image/jpg",
        #             "Ocp-Apim-Subscription-Key": fr_key,
        #         },
        #         timeout=60,
        #     )
        #     if post_resp.status_code == 202:
        #         get_url = post_resp.headers["operation-location"]
        #         status = "notcomplete"
        #         while status != "succeeded":
        #             get_resp = requests.get(
        #                 get_url,
        #                 headers={
        #                     "Content-Type": "image/jpeg",
        #                     "Ocp-Apim-Subscription-Key": fr_key,
        #                 },
        #                 timeout=60,
        #             )
        #             status = get_resp.json()["status"]
        # except Exception as ex:
        #     print(ex)

        get_resp = core_fr.analyze_form(
            filedata,
            settings.form_recognizer_endpoint,
            settings.api_version,
            "prebuilt-invoice",
            locale=language[0],
        )
        get_resp = core_fr.process_polygons(get_resp)
        fields = get_resp["documents"][0]["fields"]
        key_value_pairs = get_resp["key_value_pairs"]
        pages = get_resp["pages"]
        page_width = pages[0]["width"]
        page_height = pages[0]["height"]
        header = keyfields["header"]
        line = keyfields["line"]
        tags = {"VendorTaxId": "TRN", "CustomerTaxId": "CustomerTRN"}
        table_tags = {"TotalTax": "Tax"}

        labels_json = {
            "$schema": (
                "https://schema.cognitiveservices.azure.com/"
                "formrecognizer/2021-03-01/labels.json"
            ),
            "document": document_name.split("/")[-1],
            "labels": [],
            "labelingState": 2,
        }
        if ocr_engine in ["Azure Form Recognizer 3.0", "Azure Form Recognizer 3.1"]:
            del labels_json["labelingState"]
        table_name = "tab_1"
        i = 0
        for f in fields:
            if fields[f]["value_type"] == "list":
                value_list = fields[f]["value"]
                for v in value_list:
                    obj = v["value"]
                    for k, v in obj.items():
                        if k in table_tags.keys():
                            k = table_tags[k]
                        if k in line:
                            obj = {
                                "label": table_name + "/" + str(i) + "/" + k,
                                "key": None,
                                "value": [
                                    {
                                        "page": v["bounding_regions"][0]["page_number"],
                                        "text": v["content"],
                                        "boundingBoxes": [  # TODO - check this
                                            normalize_coordinates(
                                                page_width,
                                                page_height,
                                                v["bounding_regions"][0]["polygon"],
                                            )
                                        ],
                                    }
                                ],
                            }
                            if ocr_engine in [
                                "Azure Form Recognizer 3.0",
                                "Azure Form Recognizer 3.1",
                            ]:
                                del obj["key"]
                                obj["labelType"] = "Words"
                            labels_json["labels"].append(obj)
                    i = i + 1
            if (
                fields[f]["value_type"] == "string"
                or fields[f]["value_type"] == "currency"
                or fields[f]["value_type"] == "date"
                or fields[f]["value_type"] == "address"
            ):
                label = f
                if label in header:
                    if f in tags.keys():
                        label = tags[f]
                    obj = {
                        "label": label,
                        "key": None,
                        "value": [
                            {
                                "page": fields[f]["bounding_regions"][0]["page_number"],
                                "text": fields[f]["content"],
                                "boundingBoxes": [  # TODO - check this
                                    normalize_coordinates(
                                        page_width,
                                        page_height,
                                        fields[f]["bounding_regions"][0]["polygon"],
                                    )
                                ],
                            }
                        ],
                    }
                    if ocr_engine in [
                        "Azure Form Recognizer 3.0",
                        "Azure Form Recognizer 3.1",
                    ]:
                        del obj["key"]
                        obj["labelType"] = "Words"
                    labels_json["labels"].append(obj)

        # Include the key-value pairs in the labels_json, if any header is missing
        for key_value in key_value_pairs:
            if key_value["key"]["content"] in header and key_value["key"][
                "content"
            ] not in [
                standard_field["label"] for standard_field in labels_json["labels"]
            ]:
                obj = {
                    "label": key_value["key"]["content"],
                    "key": None,
                    "value": [
                        {
                            "page": key_value["value"]["bounding_regions"][0][
                                "page_number"
                            ],
                            "text": key_value["value"]["content"],
                            "boundingBoxes": [  # TODO - check this
                                normalize_coordinates(
                                    page_width,
                                    page_height,
                                    key_value["value"]["bounding_regions"][0][
                                        "polygon"
                                    ],
                                )
                            ],
                        }
                    ],
                }
                if ocr_engine in [
                    "Azure Form Recognizer 3.0",
                    "Azure Form Recognizer 3.1",
                ]:
                    del obj["key"]
                    obj["labelType"] = "Words"
                labels_json["labels"].append(obj)
        savelabelsfile(labels_json, document_name, db)

    except Exception:
        logger.error(traceback.format_exc())


# def getlabels(filedata, document_name, db, keyfields, ocr_engine):
#     try:
#         configs = getOcrParameters(1, db)
#         # fr_endpoint = configs.Endpoint
#         # fr_key = configs.Key1
#         get_resp = {}
#         folderpath = "/".join(document_name.split("/")[:2])
#         language = (
#             db.query(model.FRMetaData.temp_language)
#             .filter_by(FolderPath=folderpath)
#             .first()
#         )
#         # try:
#         #     post_resp = requests.post(
#         #         f"{fr_endpoint}/formrecognizer/documentModels
# /prebuilt-invoice:analyze\
#         #             ?api-version=2023-07-31&locale={language[0]}&\
#         #                 stringIndexType=textElements&features=ocrHighResolution",
#         #         data=filedata,
#         #         headers={
#         #             "Content-Type": "application/pdf",
#         #             "Ocp-Apim-Subscription-Key": fr_key,
#         #         },
#         #         timeout=60,
#         #     )
#         #     if post_resp.status_code == 202:
#         #         get_url = post_resp.headers["operation-location"]
#         #         status = "notcomplete"
#         #         while status != "succeeded":
#         #             get_resp = requests.get(
#         #                 get_url,
#         #                 headers={
#         #                     "Content-Type": "application/json",
#         #                     "Ocp-Apim-Subscription-Key": fr_key,
#         #                 },
#         #                 timeout=60,
#         #             )
#         #             status = get_resp.json()["status"]
#         # except Exception as ex:
#         #     print(f"Error in post request: {ex}")

#         get_resp = core_fr.analyze_form(
#             filedata,
#             settings.form_recognizer_endpoint,
#             settings.api_version,
#             "prebuilt-invoice",
#             locale=language[0],
#         )

#         fields = get_resp.json()["analyzeResult"]["documents"][0]["fields"]
#         pages = get_resp.json()["analyzeResult"]["pages"]
#         page_width = pages[0]["width"]
#         page_height = pages[0]["height"]
#         header = keyfields["header"]
#         line = keyfields["line"]
#         labels_json = {
#             "$schema": "https://schema.cognitiveservices.azure.com/\
#                 formrecognizer/2021-03-01/labels.json",
#             "document": document_name.split("/")[-1],
#             "labels": [],
#             "labelingState": 2,
#         }
#         if ocr_engine in ["Azure Form Recognizer 3.0", "Azure Form Recognizer 3.1"]:
#             del labels_json["labelingState"]
#         table_name = "tab_1"
#         i = 0
#         tags = {"VendorTaxId": "TRN", "CustomerTaxId": "CustomerTRN"}
#         table_tags = {"TotalTax": "Tax"}
#         for f in fields:
#             if fields[f]["type"] == "array":
#                 valueArray = fields[f]["valueArray"]
#                 for v in valueArray:
#                     obj = v["valueObject"]
#                     for k, v in obj.items():
#                         if k in table_tags.keys():
#                             k = table_tags[k]
#                         if k in line:
#                             obj = {
#                                 "label": table_name + "/" + str(i) + "/" + k,
#                                 "key": None,
#                                 "value": [
#                                     {
#                                         "page": v["boundingRegions"][0]["pageNumber"],
#                                         "text": v["content"],
#                                         "boundingBoxes": [
#                                             normalize_coordinates(
#                                                 page_width,
#                                                 page_height,
#                                                 v["boundingRegions"][0]["polygon"],
#                                             )
#                                         ],
#                                     }
#                                 ],
#                             }
#                             if ocr_engine in [
#                                 "Azure Form Recognizer 3.0",
#                                 "Azure Form Recognizer 3.1",
#                             ]:
#                                 del obj["key"]
#                                 obj["labelType"] = "Words"
#                             labels_json["labels"].append(obj)
#                     i = i + 1
#             if (
#                 fields[f]["type"] == "string"
#                 or fields[f]["type"] == "currency"
#                 or fields[f]["type"] == "date"
#                 or fields[f]["type"] == "address"
#             ):
#                 label = f
#                 if label in header:
#                     if f in tags.keys():
#                         label = tags[f]
#                     obj = {
#                         "label": label,
#                         "key": None,
#                         "value": [
#                             {
#                                 "page": fields[f]["boundingRegions"][0]["pageNumber"],
#                                 "text": fields[f]["content"],
#                                 "boundingBoxes": [
#                                     normalize_coordinates(
#                                         page_width,
#                                         page_height,
#                                         fields[f]["boundingRegions"][0]["polygon"],
#                                     )
#                                 ],
#                             }
#                         ],
#                     }
#                     if ocr_engine in [
#                         "Azure Form Recognizer 3.0",
#                         "Azure Form Recognizer 3.1",
#                     ]:
#                         del obj["key"]
#                         obj["labelType"] = "Words"
#                     labels_json["labels"].append(obj)
#         savelabelsfile(labels_json, document_name, db)

#     except Exception as ex:
#         print(f"Error in getlabels: {ex}")


def normalize_coordinates(page_width, page_height, polygon):
    normalized_polygon = []

    for i in range(0, len(polygon), 2):
        x_normalized = polygon[i] / page_width
        y_normalized = polygon[i + 1] / page_height
        normalized_polygon.extend([x_normalized, y_normalized])

    return normalized_polygon


def savelabelsfile(json_string, filename, db):
    try:
        configs = getOcrParameters(1, db)
        containername = configs.ContainerName
        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=get_credential()
        )
        blob_client = blob_service_client.get_blob_client(
            containername, blob=filename + ".labels.json"
        )
        json_string = json.dumps(json_string)
        blob_client.upload_blob(json_string, overwrite=True)
        print(f"saved: {filename+'.labels.json'}")
    except Exception:
        logger.error(traceback.format_exc())


# Not sure there exists a function with this name without new
@router.get("/get_training_result_vendor/{modeltype}/{vendorId}")
async def get_training_res_new(
    vendorId: int, modeltype: str, db: Session = Depends(get_db)
):
    try:
        training_res = crud.get_fr_training_result_by_vid(db, modeltype, vendorId)
        res = crud.get_composed_training_result_by_vid(db, modeltype, vendorId)
        for r in res:
            r.modelName = r.composed_name
            r.modelID = r.composed_name
            training_res.append(r)
        return {"message": "success", "result": training_res}
    except Exception as e:
        return {"message": f"exception {e}", "result": []}
    finally:
        db.close()

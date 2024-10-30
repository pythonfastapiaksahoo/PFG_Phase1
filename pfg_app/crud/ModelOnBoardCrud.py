# from sqlalchemy.orm import
import json
import os
import traceback
from datetime import datetime

import pytz as tz
from azure.storage.blob import BlobServiceClient
from fastapi.responses import Response

import pfg_app.model as model
from pfg_app import settings
from pfg_app.core.utils import get_credential
from pfg_app.logger_module import logger

tz_region_name = os.getenv("serina_tz", "Asia/Dubai")
tz_region = tz.timezone(tz_region_name)


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
        return Response(status_code=500)


def ParseInvoiceData(modelID, userId, invoiceData, db):
    """This function parse the data from recogniser into db format.

    - invoiceData: Form recogniser output JSON data
    - return: It return a result of dictionary type.
    """
    try:
        modeldetails = (
            db.query(
                model.DocumentModel.folderPath, model.DocumentModel.training_result
            )
            .filter(model.DocumentModel.idDocumentModel == modelID)
            .first()
        )
        folderpath = modeldetails[0]
        trainingresut = modeldetails[1]
        invoiceData = dict(invoiceData)
        # Add tags
        if len(invoiceData["TestResult"].keys()) > 0:
            db.query(model.DocumentModel).filter(
                model.DocumentModel.folderPath == folderpath
            ).update(
                {
                    "is_active": 1,
                    "modelID": invoiceData["ModelID"],
                    "training_result": trainingresut,
                    "test_result": json.dumps(invoiceData["TestResult"]),
                }
            )
        else:
            db.query(model.DocumentModel).filter(
                model.DocumentModel.folderPath == folderpath
            ).update(
                {"is_active": 1, "modelID": invoiceData["ModelID"], "training_result": trainingresut}
            )
        db.commit()
        all_models = (
            db.query(model.DocumentModel.idDocumentModel)
            .filter(model.DocumentModel.folderPath == folderpath)
            .all()
        )
        configs = getOcrParameters(1, db)
        containerName = configs.ContainerName
        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=get_credential()
        )
        bdata = (
            blob_service_client.get_blob_client(
                containerName, folderpath + "/fields.json"
            )
            .download_blob()
            .readall()
        )
        fields = json.loads(bdata)
        headerlabels = [
            f["fieldKey"] for f in fields["fields"] if f["fieldType"] == "string"
        ]
        tag_rsp = addTagDefinition(all_models, headerlabels, db)
        lineItem_rsp = []
        if "definitions" in fields and "tab_1_object" in fields["definitions"]:
            linefields = [
                f["fieldKey"] for f in fields["definitions"]["tab_1_object"]["fields"]
            ]
            lineItem_rsp = addLineItemTag(all_models, linefields, db)
        return {
            "result": "Updated",
            "records": {"Labels": tag_rsp, "LineItems": lineItem_rsp},
        }
    except Exception as e:
        logger.info(f"{e}")
        # print(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()

def get_model_status(idDocumentModel,db):
    return db.query(model.DocumentModel.is_active).filter(model.DocumentModel.idDocumentModel == idDocumentModel).scalar()

def update_model_status(idDocumentModel,is_active,db):
    try:
        db.query(model.DocumentModel).filter(model.DocumentModel.idDocumentModel == idDocumentModel).update({"is_active": is_active})
        db.commit()
        return "success"
    except Exception:
        logger.error(traceback.format_exc())
        return "exception"

def cleanupTags(modelID, db):
    # delete any exisiting tag definitions
    db.query(model.DocumentTagDef).filter_by(idDocumentModel=modelID).delete()
    # delete any exisiting tag definitions
    db.query(model.DocumentLineItemTags).filter_by(idDocumentModel=modelID).delete()
    db.commit()


def createInvoiceModel(invoiceModel, db):
    """This function creates a new invoice model, contains following
    parameters.

    - userID: unique identifier for a particular user
    - invoiceModel: It is function parameter that is of a Pydantic class object,
    It takes member data for creation of new Vendor.
    - db: It provides a session to interact with the backend Database,
    that is of Session Object Type.
    - return: It return a result of dictionary type.
    """
    try:
        # Add user authentication
        # invoiceModel = dict(invoiceModel)
        # Assigning current date to date fields
        invoiceModel["CreatedOn"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        invoiceModel["UpdatedOn"] = invoiceModel["CreatedOn"]
        # create sqlalchemy model, push and commit to db
        invoiceModelDB = model.DocumentModel(**invoiceModel)
        db.add(invoiceModelDB)
        db.commit()
        modelID = invoiceModelDB.idDocumentModel
        # return the updated record
        return invoiceModel, modelID
    except Exception as e:
        print(e)
        return Response(status_code=500)


def addTagDefinition(models, tags, db):
    """This function creates a new set of tag definitions, contains following
    parameters.

    - modelID: unique identifier for a particular model created in the DB
    - tags: It is function parameter that is list of a Pydantic class object,
    It takes member data for creation of new tag definition.
    - db: It provides a session to interact with the backend Database,
    that is of Session Object Type.
    - return: It return a result of dictionary type.
    """
    try:
        createdTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        # looping through the tagdef to insert recrod one by one
        for m in models:
            all_headertags = []
            for item in tags:
                tagdef = {}
                # Add created on, UpdatedOn and model7 ID to each tag
                # definition records
                tagdef["TagLabel"] = item
                all_headertags.append(item)
                tagdef["CreatedOn"] = createdTime
                tagdef["UpdatedOn"] = createdTime
                tagdef["idDocumentModel"] = m[0]
                check = (
                    db.query(model.DocumentTagDef)
                    .filter(
                        model.DocumentTagDef.idDocumentModel == m[0],
                        model.DocumentTagDef.TagLabel == item,
                    )
                    .first()
                )
                if check is None:
                    db.add(model.DocumentTagDef(**tagdef))
            all_tags = (
                db.query(model.DocumentTagDef)
                .filter(model.DocumentTagDef.idDocumentModel == m[0])
                .all()
            )
            for tag in all_tags:
                if tag.TagLabel not in all_headertags:
                    db.query(model.DocumentTagDef).filter_by(
                        idDocumentModel=m[0], TagLabel=tag.TagLabel
                    ).delete()
            db.commit()
        return tags
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)


def addLineItemTag(models, lineItemTag, db):
    """This function creates a new set of line item tag definitions, contains
    following parameters.

    - modelID: unique identifier for a particular model created in the DB
    - lineItemTag: It is function parameter that is list of a Pydantic
    class object, It takes member data for creation of new line item tag definition.
    - db: It provides a session to interact with the backend Database,
    that is of Session Object Type.
    - return: It return a result of dictionary type.
    """
    try:
        for m in models:
            all_lineitemtags = []
            for item in lineItemTag:
                lineItemDef = {}
                all_lineitemtags.append(item)
                lineItemDef["TagName"] = item
                lineItemDef["idDocumentModel"] = m[0]
                check = (
                    db.query(model.DocumentLineItemTags)
                    .filter(
                        model.DocumentLineItemTags.idDocumentModel == m[0],
                        model.DocumentLineItemTags.TagName == lineItemDef["TagName"],
                    )
                    .first()
                )
                if check is None:
                    db.add(model.DocumentLineItemTags(**lineItemDef))
                    db.commit()
            all_linetags = (
                db.query(model.DocumentLineItemTags)
                .filter(model.DocumentLineItemTags.idDocumentModel == m[0])
                .all()
            )
            for tag in all_linetags:
                if tag.TagName not in all_lineitemtags:
                    db.query(model.DocumentLineItemTags).filter_by(
                        idDocumentModel=m[0], TagName=tag.TagName
                    ).delete()
                    db.commit()
        return lineItemTag
    except Exception as e:
        print(e)
        return Response(status_code=500)


def updateLabels(documentId, labelVal, db):
    try:
        db.query(model.DocumentModel).filter(
            model.DocumentModel.idDocumentModel == documentId
        ).update({"labels": labelVal})
        db.commit()
        return "success"
    except Exception:
        logger.error(traceback.format_exc())
        return "exception"


def get_fr_training_result_by_vid(db, modeltype, Id):
    if modeltype == "vendor":
        return (
            db.query(model.DocumentModel)
            .filter(
                model.DocumentModel.idVendorAccount == Id,
                model.DocumentModel.training_result is not None,
            )
            .all()
        )
    else:
        return (
            db.query(model.DocumentModel)
            .filter(
                model.DocumentModel.serviceproviderID == Id,
                model.DocumentModel.training_result is not None,
            )
            .all()
        )


def get_composed_training_result_by_vid(db, modeltype, Id):
    if modeltype == "vendor":
        return (
            db.query(model.DocumentModelComposed)
            .filter(
                model.DocumentModelComposed.vendorAccountId == Id,
                model.DocumentModelComposed.training_result is not None,
            )
            .all()
        )
    else:
        return (
            db.query(model.DocumentModelComposed)
            .filter(
                model.DocumentModelComposed.serviceproviderID == Id,
                model.DocumentModelComposed.training_result is not None,
            )
            .all()
        )


def getFields(documentId, db):
    return (
        db.query(model.DocumentModel)
        .filter(model.DocumentModel.idDocumentModel == documentId)
        .all()[0]
        .fields
    )


def getSavedLabels(documentId, db):
    return (
        db.query(model.DocumentModel)
        .filter(model.DocumentModel.idDocumentModel == documentId)
        .all()[0]
        .labels
    )


def get_fr_training_result(db, documentId):
    result = (
        db.query(model.DocumentModel)
        .filter(model.DocumentModel.idDocumentModel == documentId)
        .all()[0]
        .training_result
    )
    if result is not None:
        return result
    else:
        return []


def updateFields(documentId, fieldsVal, db):
    try:
        db.query(model.DocumentModel).filter(
            model.DocumentModel.idDocumentModel == documentId
        ).update({"fields": fieldsVal})
        db.commit()
        return "success"
    except Exception:
        logger.error(traceback.format_exc())
        return "exception"


def createOrUpdateComposeModel(composeObj, db):
    try:
        add = "add"
        if composeObj["vendorAccountId"]:
            check = (
                db.query(model.DocumentModelComposed)
                .filter(
                    model.DocumentModelComposed.vendorAccountId
                    == composeObj["vendorAccountId"],
                    model.DocumentModelComposed.composed_name
                    == composeObj["composed_name"],
                )
                .all()
            )
        else:
            check = (
                db.query(model.DocumentModelComposed)
                .filter(
                    model.DocumentModelComposed.serviceproviderID
                    == composeObj["serviceproviderID"],
                    model.DocumentModelComposed.composed_name
                    == composeObj["composed_name"],
                )
                .all()
            )
        if len(check) == 0:
            composeObj = dict(composeObj)
            db.add(model.DocumentModelComposed(**composeObj))
            db.commit()
        return f"success {add}"
    except Exception:
        logger.error(traceback.format_exc())
        return "exception"


def updateTrainingResult(documentId, result, db):
    try:
        db.query(model.DocumentModel).filter(
            model.DocumentModel.idDocumentModel == documentId
        ).update({"training_result": result})
        db.commit()
        return "success"
    except Exception:
        logger.error(traceback.format_exc())
        return "exception"


def parseLabelValue(labelValue):
    """This function parses the list of values in form-recogniser data into
    single bounding box and value.

    - labelValue : list of values provided by form recogniser
    - return: It returns the label page number and bounding boxes.
    """
    try:
        # extract page list and bounding box list from label value
        dict_list = []
        boundingbox_list = []
        for item in labelValue:
            item = dict(item)
            dict_list.append(item)
            boundingbox_list.append(dict(item["boundingBoxes"]))
        # parse page numbers
        pagelist = [item["page"] for item in dict_list]
        # removing any duplicate page numbers
        pagelist = list(set(pagelist))
        # convert page numbers to comma seprated string
        pages = ",".join([str(item) for item in pagelist])
        # find max and min of x cordinates
        xmax = max(float(item["x"]) for item in boundingbox_list or [{"x": 0}])
        xmin = min(float(item["x"]) for item in boundingbox_list or [{"x": 0}])
        # find max and min of y cordinates
        ymax = max(float(item["y"]) for item in boundingbox_list or [{"y": 0}])
        ymin = min(float(item["y"]) for item in boundingbox_list or [{"y": 0}])
        # find height and width of last bounding box
        addheight = (
            boundingbox_list[len(boundingbox_list) - 1]["h"]
            if len(boundingbox_list) > 0
            else 0
        )
        addwidth = (
            boundingbox_list[len(boundingbox_list) - 1]["w"]
            if len(boundingbox_list) > 0
            else 0
        )
        # find total height and width
        width = str(int(float(xmax)) - int(float(xmin)) + int(float(addwidth)))
        height = str(int(float(ymax)) - int(float(ymin)) + int(float(addheight)))
        # create return dict with parsed values
        tagdef = {
            "Xcord": xmin,
            "Ycord": ymin,
            "Width": width,
            "Height": height,
            "Pages": pages,
        }
        return tagdef
    except Exception as e:
        print(e)
        return Response(status_code=500)


def parseTabelValue(labelValue):
    """This function parses the list of line item column names in form-
    recogniser data into single bounding box and value.

    - labelValue : list of values provided by form recogniser
    - return: It returns the label TagName and bounding boxes.
    """
    for item in labelValue:
        if "boundingBoxes" in item:
            if (
                "x" in item["boundingBoxes"]
                and "y" in item["boundingBoxes"]
                and "w" in item["boundingBoxes"]
                and "h" in item["boundingBoxes"]
            ):
                lineItemDef = {
                    "Xcord": item["boundingBoxes"]["x"],
                    "Ycord": item["boundingBoxes"]["y"],
                    "Width": item["boundingBoxes"]["w"],
                    "Height": item["boundingBoxes"]["h"],
                    "TagName": item["text"],
                }
            else:
                lineItemDef = {
                    "Xcord": 0,
                    "Ycord": 0,
                    "Width": 0,
                    "Height": 0,
                    "TagName": "",
                }
        else:
            lineItemDef = {
                "Xcord": 0,
                "Ycord": 0,
                "Width": 0,
                "Height": 0,
                "TagName": "",
            }

    return lineItemDef


async def delete_blob_container(db, blob):
    try:
        all_blobs = [blob, blob + ".labels.json", blob + ".ocr.json"]

        ContainerName = (
            db.query(model.FRConfiguration.ContainerName)
            .filter_by(idFrConfigurations=1)
            .scalar()
        )
        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=get_credential()
        )
        for bl in all_blobs:
            blob_client = blob_service_client.get_blob_client(
                container=ContainerName, blob=bl
            )
            blob_client.delete_blob()
        return "success"
    except BaseException:
        print(traceback.format_exc())
        return "exception"


async def check_duplicate(modelName, db):
    try:
        check = (
            db.query(model.DocumentModel)
            .filter(model.DocumentModel.modelName == modelName)
            .first()
        )
        if check:
            return {"status": True, "message": "exists"}
        else:
            return {"status": False, "message": "not exists"}
    except BaseException:
        return {"status": False, "message": "exception"}

import json
import os

# FRMetaData
import time
import traceback
from datetime import datetime
from typing import Dict

import pandas as pd
import pytz as tz
from fastapi.responses import Response
from sqlalchemy import func, or_
from sqlalchemy.orm import Load, load_only

import pfg_app.model as model
from pfg_app.logger_module import logger
from pfg_app.session.session import DB, SQLALCHEMY_DATABASE_URL

tz_region_name = os.getenv("serina_tz", "Asia/Dubai")
tz_region = tz.timezone(tz_region_name)


async def getFRConfig(userID, db):
    """This function gets the form recognizer configurations set for the user,
    contains following parameters.

    - userID: unique identifier for a particular user
    - db: It provides a session to interact with
    the backend Database,that is of Session Object Type.
    - return: It return a result of dictionary type.
    """
    try:
        customer_id = db.query(model.User.customerID).filter_by(idUser=userID).scalar()
        return (
            db.query(model.FRConfiguration)
            .filter(model.FRConfiguration.idCustomer == customer_id)
            .first()
        )
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def updateFRConfig(userID, frConfig, db):
    """This function gets the form recognizer configurations set for the user,
    contains following parameters.

    - userID: unique identifier for a particular user
    - db: It provides a session to interact with
    the backend Database,that is of Session Object Type.
    - return: It return a result of dictionary type.
    """
    try:
        frConfig = dict(frConfig)
        # pop out elements that are not having any value
        for item_key in frConfig.copy():
            if not frConfig[item_key]:
                frConfig.pop(item_key)

        db.query(model.FRConfiguration).filter(
            model.FRConfiguration.idCustomer == userID
        ).update(frConfig)
        db.commit()
        return {"result": "Updated", "records": frConfig}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


def check_same_vendors_different_entities(vendoraccountId, modelname, db):
    try:
        account = (
            db.query(model.VendorAccount.Account)
            .filter(model.VendorAccount.idVendorAccount == vendoraccountId)
            .first()
        )
        vendorcode = account[0]
        vendors = (
            db.query(model.VendorAccount.idVendorAccount)
            .filter(model.VendorAccount.Account == vendorcode)
            .all()
        )
        count = 0
        for v in vendors:
            if v[0] != vendoraccountId:
                checkmodel = (
                    db.query(model.DocumentModel)
                    .filter(
                        model.DocumentModel.idVendorAccount == v[0],
                        model.DocumentModel.modelName == modelname,
                    )
                    .first()
                )
                if checkmodel is None:
                    count = count + 1
        vendor = (
            db.query(model.Vendor.VendorName)
            .filter(model.Vendor.VendorCode == vendorcode)
            .first()
        )
        vendorName = vendor[0]
        if count >= 1:
            return {"message": "exists", "vendor": vendorName, "count": count - 1}
        else:
            return {"message": "not exists", "vendor": vendorName, "count": count}
    except Exception as e:
        print(e)
        return {"message": "exception"}
    finally:
        db.close()


# Define a function to parse JSON strings as JSON
def parse_json(x):
    try:
        return json.loads(x)
    except (TypeError, ValueError):
        return x


def copymodelsSP(serviceaccountId, modelname, db):
    try:
        sp = (
            db.query(model.ServiceProvider.ServiceProviderName)
            .filter(model.ServiceProvider.idServiceProvider == serviceaccountId)
            .first()
        )
        ServiceProviderName = sp[0]
        sps = (
            db.query(model.ServiceProvider.idServiceProvider)
            .filter(model.ServiceProvider.ServiceProviderName == ServiceProviderName)
            .all()
        )
        docmodelqr = (
            "SELECT * FROM "
            + DB
            + ".documentmodel WHERE serviceproviderID="
            + str(serviceaccountId)
            + " and modelName = '"
            + modelname
            + "';"
        )
        docmodel = pd.read_sql(docmodelqr, SQLALCHEMY_DATABASE_URL)
        inputmodel = {}
        for d in docmodel.head():
            inputmodel[d] = docmodel[d][0]
        model_id = inputmodel["idDocumentModel"]
        frmetadataqr = (
            "SELECT * FROM "
            + DB
            + ".frmetadata WHERE idInvoiceModel="
            + str(model_id)
            + ""
        )
        frmetadatares = pd.read_sql(frmetadataqr, SQLALCHEMY_DATABASE_URL)
        frmetadata = {}
        for f in frmetadatares.head():
            frmetadata[f] = frmetadatares[f][0]
        del frmetadata["idFrMetaData"]
        del inputmodel["idDocumentModel"]
        allmodelid = []
        for v in sps:
            if v[0] != serviceaccountId:
                iddocqr = (
                    db.query(model.DocumentModel.idDocumentModel)
                    .filter(
                        model.DocumentModel.serviceproviderID == v[0],
                        model.DocumentModel.modelName == modelname,
                    )
                    .first()
                )
                saccounts = (
                    db.query(model.ServiceAccount.idServiceAccount)
                    .filter(model.ServiceAccount.serviceProviderID == v[0])
                    .first()
                )
                seraccount = saccounts[0]
                if iddocqr is None:
                    inputmodel["idServiceAccount"] = seraccount
                    inputmodel["serviceproviderID"] = v[0]
                    inputmodel["CreatedOn"] = datetime.utcnow().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    inputmodel["UpdatedOn"] = inputmodel["CreatedOn"]
                    invoiceModelDB = model.DocumentModel(**inputmodel)
                    db.add(invoiceModelDB)
                    db.commit()
                    print(v[0])
                    allmodelid.append(invoiceModelDB.idDocumentModel)
        print(f"frmetadata {allmodelid}")
        for m in allmodelid:
            frmetadata["idInvoiceModel"] = m
            frmetaDataDB = model.FRMetaData(**frmetadata)
            db.add(frmetaDataDB)
            db.commit()
            print(m)
        documenttagdefqr = (
            "SELECT * FROM "
            + DB
            + ".documenttagdef WHERE idDocumentModel="
            + str(model_id)
            + ""
        )
        documenttagdefres = pd.read_sql(documenttagdefqr, SQLALCHEMY_DATABASE_URL)
        documenttagdefres.replace({pd.np.nan: None}, inplace=True)
        documenttagdefres["transformation"] = documenttagdefres["transformation"].apply(
            parse_json
        )
        documenttagdefres["isdelete"] = documenttagdefres["isdelete"].apply(parse_json)
        documenttagdefres["datatype"] = documenttagdefres["datatype"].apply(parse_json)
        documenttagdef = []
        for i in range(len(documenttagdefres)):
            obj = {}
            for f in documenttagdefres.head():
                if f != "idDocumentTagDef":
                    obj[f] = documenttagdefres[f][i]
            documenttagdef.append(obj)
        print(f"header tag {documenttagdef}")
        for m in allmodelid:
            checktag = (
                db.query(model.DocumentTagDef)
                .filter(model.DocumentTagDef.idDocumentModel == m)
                .first()
            )
            if checktag is None:
                for d in documenttagdef:
                    d["idDocumentModel"] = m
                    documenttagdefDB = model.DocumentTagDef(**d)
                    db.add(documenttagdefDB)
                    db.commit()
        documentlinedefqr = (
            "SELECT * FROM "
            + DB
            + ".documentlineitemtags WHERE idDocumentModel="
            + str(model_id)
            + ""
        )
        documentlinedefres = pd.read_sql(documentlinedefqr, SQLALCHEMY_DATABASE_URL)
        documentlinedefres.replace({pd.np.nan: None}, inplace=True)
        documentlinedefres["transformation"] = documentlinedefres[
            "transformation"
        ].apply(parse_json)
        documentlinedefres["isdelete"] = documentlinedefres["isdelete"].apply(
            parse_json
        )
        documentlinedefres["datatype"] = documentlinedefres["datatype"].apply(
            parse_json
        )
        documentlinedef = []
        for i in range(len(documentlinedefres)):
            obj = {}
            for f in documentlinedefres.head():
                if f != "idDocumentLineItemTags":
                    obj[f] = documentlinedefres[f][i]
            documentlinedef.append(obj)
        print(f"line tag {documentlinedef}")
        for m in allmodelid:
            checktag = (
                db.query(model.DocumentLineItemTags)
                .filter(model.DocumentLineItemTags.idDocumentModel == m)
                .first()
            )
            if checktag is None:
                for d in documentlinedef:
                    d["idDocumentModel"] = m
                    documentlinedefDB = model.DocumentLineItemTags(**d)
                    db.add(documentlinedefDB)
                    db.commit()
        return {"message": "success"}
    except Exception:
        logger.error(traceback.format_exc())
        return {"message": "exception"}
    finally:
        db.close()


# -------------------Updated Frmetadata function synonyms included --------------
async def updateMetadata(documentId, frmetadata, db):
    try:
        frmetadata = dict(frmetadata)
        frmetadata_copy = frmetadata.copy()
        del frmetadata["synonyms"]
        print(f"frmetadata {frmetadata}")
        meta = (
            db.query(model.FRMetaData)
            .filter(model.FRMetaData.idInvoiceModel == documentId)
            .first()
        )
        if meta is not None:
            db.query(model.FRMetaData).filter(
                model.FRMetaData.FolderPath == frmetadata["FolderPath"]
            ).update(frmetadata)
        else:
            frmetadata["idInvoiceModel"] = documentId
            db.add(model.FRMetaData(**frmetadata))
        db.commit()
        return {"result": "Updated", "records": frmetadata_copy}
    except Exception:
        logger.error(traceback.format_exc())
        return {"result": "Failed", "records": {}}
    finally:
        db.close()


def createInvoiceModel(userID, user, invoiceModel, db):
    """This function creates a new invoice model, contains following parameters
    :param userID: unique identifier for a particular user :param invoiceModel:
    It is function parameter that is of a Pydantic class object, It takes
    member data for creation of new Vendor.

    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        ts = str(time.time())
        fld_name = ts.replace(".", "_") + "/train"

        # user_details = (
        #     db.query(model.User.firstName, model.User.lastName)
        #     .filter(model.User.idUser == userID)
        #     .first()
        # )  # TODO: Unused variable
        # user_name = (
        #     user_details[0]
        #     if user_details[0] is not None
        #     else "" + " " + user_details[1] if user_details[1] is not None else ""
        # )  # TODO: Unused variable
        # Add user authentication
        invoiceModel = dict(invoiceModel)
        print(invoiceModel)
        checkmodel = (
            db.query(model.DocumentModel)
            .filter(model.DocumentModel.modelName == invoiceModel["modelName"])
            .first()
        )
        if checkmodel is not None:
            return {"result": "exists"}
        invoiceModel["folderPath"] = fld_name
        # Assigning current date to date fields
        invoiceModel["CreatedOn"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        invoiceModel["UpdatedOn"] = invoiceModel["CreatedOn"]
        try:
            invoiceModel["userID"] = user.name
            invoiceModel["update_by"] = user.name
        except Exception:
            invoiceModel["userID"] = None
            invoiceModel["update_by"] = None
        # create sqlalchemy model, push and commit to db
        invoiceModelDB = model.DocumentModel(**invoiceModel)
        db.add(invoiceModelDB)
        db.commit()
        # return the updated record
        return {"result": "Updated", "records": invoiceModel}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


def updateInvoiceModel(modelID, invoiceModel, db):
    """This function updates a new invoice model, contains following parameters
    :param modelID: unique identifier for a particular model :param
    invoiceModel: It is function parameter that is of a Pydantic class object,
    It takes member data for creation of new Vendor.

    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        # Add user authentication
        invoiceModel = dict(invoiceModel)
        # pop out elements that are not having any value
        for item_key in invoiceModel.copy():
            if not invoiceModel[item_key]:
                invoiceModel.pop(item_key)

        db.query(model.DocumentModel).filter(
            model.DocumentModel.idDocumentModel == modelID
        ).update(invoiceModel)
        db.commit()
        # return the updated record
        return {"result": "Updated", "records": invoiceModel}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


def getmodellist(vendorID, db):
    """This function gets the form recognizer configurations set for the user,
    contains following parameters.

    - userID: unique identifier for a particular user
    - db: It provides a session to interact with
    the backend Database,that is of Session Object Type.
    - return: It return a result of dictionary type.
    """
    try:
        # subquery to get access permission id from db
        sub_query = (
            db.query(model.VendorAccount.idVendorAccount)
            .filter(model.VendorAccount.vendorID == vendorID)
            .distinct()
        )
        # query to get the user permission for the user from db
        main_query = (
            db.query(model.DocumentModel)
            .filter(model.DocumentModel.idVendorAccount.in_(sub_query))
            .all()
        )
        # get the user permission and check if user
        # can create or not by checking if its not null
        if not main_query:
            return []
        return main_query
    except Exception:
        logger.error(traceback.format_exc())
        return []
    finally:
        db.close()


def getmodellistsp(serviceProviderID, db):
    """This function gets the form recognizer configurations set for the user,
    contains following parameters.

    - userID: unique identifier for a particular user
    - db: It provides a session to interact with
    the backend Database,that is of Session Object Type.
    - return: It return a result of dictionary type.
    """
    try:
        # query to get the user permission for the user from db
        main_query = (
            db.query(model.DocumentModel)
            .filter(model.DocumentModel.serviceproviderID == serviceProviderID)
            .all()
        )
        # get the user permission and check if user
        # can create or not by checking if its not null
        if not main_query:
            return Response(status_code=404)
        return main_query
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def readvendor(db):
    """This function read a Vendor.

    It contains 1 parameter.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        return (
            db.query(model.Vendor)
            .filter(or_(model.Vendor.idVendor <= 5, model.Vendor.idVendor == 2916))
            .all()
        )
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def readvendoraccount(db, v_id: int):
    """This function read Vendor account details.

    It contains 2 parameter.
    :param v_ID: It is a function parameters that is of integer type, it
        provides the vendor Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        return (
            db.query(model.VendorAccount)
            .filter(model.VendorAccount.vendorID == v_id)
            .all()
        )
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def readspaccount(db, s_id: int):
    """This function read Vendor account details.

    It contains 2 parameter.
    :param v_ID: It is a function parameters that is of integer type, it
        provides the vendor Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        return (
            db.query(model.ServiceAccount)
            .filter(model.ServiceAccount.serviceProviderID == s_id)
            .all()
        )
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def addOCRLog(logData, db):
    """This functions creates a new log in OCRlogs table based on an OCR run on
    a document.

    - logData: pydantic class object of the log data obtained from the OCR run
    - db: It provides a session to interact with
    the backend Database,that is of Session Object Type.
    """
    try:
        logData = dict(logData)
        logData["editedOn"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        db.add(model.OCRLogs(**logData))
        db.commit()
        return {"result": "Updated", "records": logData}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def addItemMapping(mapData, db):
    """This functions creates a new user defined item mapping.

    - mapData: pydantic class object of the item mapping
    - db: It provides a session to interact with
    the backend Database,that is of Session Object Type.
    """
    try:
        mapData = dict(mapData)
        mapData["createdOn"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        db.add(model.UserItemMapping(**mapData))
        db.commit()
        return {"result": "Updated", "records": mapData}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def addfrMetadata(m_id: int, r_id: int, n_fr_mdata, db):
    """This functions creates a new user defined item mapping.

    - frmData: pydantic class object of the fr meta data
    - db: It provides a session to interact with
    the backend Database,that is of Session Object Type.
    """
    try:

        frmData = dict(n_fr_mdata)
        frmData["idInvoiceModel"] = m_id
        frmData["ruleID"] = r_id
        db.add(model.FRMetaData(**frmData))
        db.commit()
        return {"result": "Inserted", "records": frmData}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def getMetaData(documentId: int, db):
    merged_data = {}
    try:
        vendor_account_id = (
            db.query(model.DocumentModel.idVendorAccount)
            .filter(model.DocumentModel.idDocumentModel == documentId)
            .scalar()
        )
        vendor_id = (
            db.query(model.VendorAccount.vendorID)
            .filter(model.VendorAccount.idVendorAccount == vendor_account_id)
            .scalar()
        )
        synonyms = (
            db.query(model.Vendor.Synonyms)
            .filter(model.Vendor.idVendor == vendor_id)
            .scalar()
        )

        frmetadata = (
            db.query(model.FRMetaData)
            .filter(model.FRMetaData.idInvoiceModel == documentId)
            .first()
        )
        # Convert SQLAlchemy object to dictionary if needed
        if frmetadata:
            frmetadata_dict = {
                column.name: getattr(frmetadata, column.name)
                for column in frmetadata.__table__.columns
            }
        else:
            frmetadata_dict = {}

        # Combine the data into a single dictionary
        merged_data = {"frmetadata": frmetadata_dict, "synonyms": synonyms}
        return merged_data
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def getTrainTestRes(modelId: int, db):
    try:
        return (
            db.query(model.DocumentModel)
            .options(load_only("training_result", "test_result"))
            .filter(model.DocumentModel.idDocumentModel == modelId)
            .first()
        )
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def getActualAccuracy(tp: str, nm: str, db):
    try:
        if tp == "vendor":
            accuracy_data = (
                db.query(
                    model.Document,
                    model.DocumentTagDef,
                    model.Vendor,
                    model.DocumentData,
                    model.VendorAccount,
                )
                .options(
                    Load(model.DocumentTagDef).load_only("TagLabel"),
                    Load(model.Vendor).load_only("VendorName"),
                    Load(model.DocumentData).load_only(
                        "Value", "IsUpdated", "isError", "ErrorDesc"
                    ),
                    Load(model.Document).load_only("idDocument"),
                    Load(model.VendorAccount).load_only("idVendorAccount"),
                )
                .join(
                    model.DocumentData,
                    model.DocumentData.documentID == model.Document.idDocument,
                )
                .join(
                    model.DocumentTagDef,
                    model.DocumentTagDef.idDocumentTagDef
                    == model.DocumentData.documentTagDefID,
                )
                .join(
                    model.VendorAccount,
                    model.VendorAccount.idVendorAccount
                    == model.Document.vendorAccountID,
                )
                .join(
                    model.Vendor, model.Vendor.idVendor == model.VendorAccount.vendorID
                )
                .filter(
                    model.Vendor.VendorName == nm, model.Document.idDocumentType == 3
                )
                .all()
            )
        else:
            accuracy_data = (
                db.query(
                    model.Document,
                    model.DocumentTagDef,
                    model.ServiceProvider,
                    model.DocumentData,
                    model.ServiceAccount,
                )
                .options(
                    Load(model.DocumentTagDef).load_only("TagLabel"),
                    Load(model.ServiceProvider).load_only("ServiceProviderName"),
                    Load(model.DocumentData).load_only(
                        "Value", "IsUpdated", "isError", "ErrorDesc"
                    ),
                    Load(model.Document).load_only("idDocument"),
                    Load(model.ServiceAccount).load_only("idServiceAccount"),
                )
                .join(
                    model.DocumentData,
                    model.DocumentData.documentID == model.Document.idDocument,
                )
                .join(
                    model.DocumentTagDef,
                    model.DocumentTagDef.idDocumentTagDef
                    == model.DocumentData.documentTagDefID,
                )
                .join(
                    model.ServiceAccount,
                    model.ServiceAccount.idServiceAccount
                    == model.Document.supplierAccountID,
                )
                .join(
                    model.ServiceProvider,
                    model.ServiceProvider.idServiceProvider
                    == model.ServiceAccount.serviceProviderID,
                )
                .filter(
                    model.ServiceProvider.ServiceProviderName == nm,
                    model.Document.idDocumentType == 3,
                )
                .all()
            )
        final_dict: Dict[str, Dict[str, int]] = {}
        documents = []
        for a in accuracy_data:
            documentid = a.Document.idDocument
            if documentid not in documents:
                documents.append(a.Document.idDocument)
            key = a.DocumentTagDef.TagLabel
            iserror = a.DocumentData.isError
            isupdated = a.DocumentData.IsUpdated

            # Ensure final_dict[key] is a dict
            if key not in final_dict or not isinstance(final_dict[key], dict):
                final_dict[key] = {"miss": 0, "match": 0}
            elif isinstance(final_dict[key], dict):
                if iserror == 1 or isupdated == 1:
                    final_dict[key]["miss"] += 1
                else:
                    final_dict[key]["match"] += 1

        final_dict["DocumentCount"] = {
            "count": len(documents)
        }  # instead of int value, it should be
        # a dict to avoid confusion and type issues
        return final_dict
    except Exception as e:
        print(e)
        return Response(status_code=500)
    finally:
        db.close()


async def getActualAccuracyByEntity(type, db):
    try:
        if type == "vendor":
            accuracy_data = (
                db.query(
                    model.Document,
                    model.DocumentTagDef,
                    model.DocumentData,
                    model.Entity,
                )
                .options(
                    Load(model.DocumentTagDef).load_only("TagLabel"),
                    Load(model.Entity).load_only("EntityName"),
                    Load(model.DocumentData).load_only(
                        "Value", "IsUpdated", "isError", "ErrorDesc"
                    ),
                    Load(model.Document).load_only("idDocument"),
                )
                .join(
                    model.DocumentData,
                    model.DocumentData.documentID == model.Document.idDocument,
                )
                .join(
                    model.DocumentTagDef,
                    model.DocumentTagDef.idDocumentTagDef
                    == model.DocumentData.documentTagDefID,
                )
                .join(model.Entity, model.Entity.idEntity == model.Document.entityID)
                .filter(
                    model.Document.vendorAccountID.isnot(None),
                    model.Document.idDocumentType == 3,
                )
            )
        else:
            accuracy_data = (
                db.query(
                    model.Document,
                    model.DocumentTagDef,
                    model.DocumentData,
                    model.Entity,
                )
                .options(
                    Load(model.DocumentTagDef).load_only("TagLabel"),
                    Load(model.Entity).load_only("EntityName"),
                    Load(model.DocumentData).load_only(
                        "Value", "IsUpdated", "isError", "ErrorDesc"
                    ),
                    Load(model.Document).load_only("idDocument"),
                )
                .join(
                    model.DocumentData,
                    model.DocumentData.documentID == model.Document.idDocument,
                )
                .join(
                    model.DocumentTagDef,
                    model.DocumentTagDef.idDocumentTagDef
                    == model.DocumentData.documentTagDefID,
                )
                .join(model.Entity, model.Entity.idEntity == model.Document.entityID)
                .filter(
                    model.Document.supplierAccountID.isnot(None),
                    model.Document.idDocumentType == 3,
                )
            )
        final_dict = {}
        entitydata = accuracy_data.all()
        for a in entitydata:
            entity = a.Entity.EntityName
            if entity not in final_dict.keys():
                final_dict[entity] = {}
                data = accuracy_data.filter(model.Entity.EntityName == entity).all()
                for d in data:
                    key = d.DocumentTagDef.TagLabel
                    iserror = d.DocumentData.isError
                    isupdated = d.DocumentData.IsUpdated
                    if key not in final_dict[entity].keys():
                        final_dict[entity][key] = {
                            "Total Invoices": 0,
                            "Miss": 0,
                            "Match": 0,
                            "Accuracy": 0,
                        }
                    else:
                        if iserror == 1 or isupdated == 1:
                            final_dict[entity][key]["Miss"] += 1
                        else:
                            final_dict[entity][key]["Match"] += 1
                    final_dict[entity][key]["Total Invoices"] = (
                        final_dict[entity][key]["Match"]
                        + final_dict[entity][key]["Miss"]
                    )
                    final_dict[entity][key]["Accuracy"] = (
                        round(
                            final_dict[entity][key]["Match"]
                            / (
                                final_dict[entity][key]["Match"]
                                + final_dict[entity][key]["Miss"]
                            )
                            * 100,
                            1,
                        )
                        if (
                            final_dict[entity][key]["Match"]
                            + final_dict[entity][key]["Miss"]
                        )
                        != 0
                        else 0.0
                    )
        return final_dict
    except Exception as e:
        print(e)
        return Response(status_code=500)
    finally:
        db.close()


async def getall_tags(tagtype, db):
    try:
        header_tags = (
            db.query(model.DefaultFields)
            .options(load_only("Name", "Ismendatory"))
            .filter(
                model.DefaultFields.Type == "Header",
                model.DefaultFields.TagType == tagtype,
            )
            .all()
        )
        line_tags = (
            db.query(model.DefaultFields)
            .options(load_only("Name", "Ismendatory"))
            .filter(
                model.DefaultFields.Type == "line",
                model.DefaultFields.TagType == tagtype,
            )
            .all()
        )
        return {"header": header_tags, "line": line_tags}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def get_entity_level_taggedInfo(db):
    try:
        # Build the query using SQLAlchemy ORM
        result = (
            db.query(
                model.Entity.EntityName.label("Entity"),
                model.Vendor.VendorName.label("Vendor"),
                model.FRMetaData.mandatoryheadertags.label("Mandatory Header Tags"),
                model.FRMetaData.mandatorylinetags.label("Mandatory Line Item Tags"),
                model.FRMetaData.optionalheadertags.label("Optional Header Tags"),
                model.FRMetaData.optionallinertags.label("Optional Line Item Tags"),
            )
            .join(model.Vendor, model.Vendor.entityID == model.Entity.idEntity)
            .join(
                model.VendorAccount,
                model.VendorAccount.vendorID == model.Vendor.idVendor,
            )
            .join(
                model.DocumentModel,
                model.DocumentModel.idVendorAccount
                == model.VendorAccount.idVendorAccount,
            )
            .join(
                model.FRMetaData,
                model.FRMetaData.idInvoiceModel == model.DocumentModel.idDocumentModel,
            )
        ).all()

        # Get the length of the result
        row_count = len(result)

        # Output the row count for debugging or further logic
        print(f"Number of entity_level_taggedInfo rows fetched: {row_count}")

        # Convert the result into a DataFrame
        df = pd.DataFrame(
            result,
            columns=[
                "Entity",
                "Vendor",
                "Mandatory Header Tags",
                "Mandatory Line Item Tags",
                "Optional Header Tags",
                "Optional Line Item Tags",
            ],
        )

        # If the DataFrame is empty, handle the case
        if df.empty:
            df = pd.DataFrame(
                columns=[
                    "Entity",
                    "Vendor",
                    "Mandatory Header Tags",
                    "Mandatory Line Item Tags",
                    "Optional Header Tags",
                    "Optional Line Item Tags",
                ]
            )

        # Save DataFrame to an Excel file
        filename = "VendorsTaggedInfo.xlsx"
        df.to_excel(filename, index=False)

        return filename

    except Exception as e:
        # Log or print the exception for debugging
        print(f"Error: {e}")

        # Create an empty Excel file in case of error
        df = pd.DataFrame(
            columns=[
                "Entity",
                "Vendor",
                "Mandatory Header Tags",
                "Mandatory Line Item Tags",
                "Optional Header Tags",
                "Optional Line Item Tags",
            ]
        )
        filename = "VendorsTaggedInfo.xlsx"
        df.to_excel(filename, index=False)

        return filename


async def readdocumentrules(db):
    """This function read document rules list.

    It contains 1 parameter.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        return db.query(model.Rule).all()
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def readnewdocrules(db):
    """This function read new document rules list for agi.

    It contains 1 parameter.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        return db.query(model.AGIRule).all()
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def update_email_info(emailInfo, db):
    """This function updates the email_listener_info for a given user.

    :param emailInfo: Form Recognizer Configuration Pydantic model.
    :param db: SQLAlchemy session to interact with the backend Database.
    :return: A result dictionary.
    """
    try:
        # Extract email_listener_info from the emailInfo object
        emailListenerInfo = emailInfo.EmailListenerInfo

        # Only update if email_listener_info is provided
        if emailListenerInfo is not None:
            # Convert emailListenerInfo to a dictionary if it is not already
            emailListenerInfo = dict(emailListenerInfo)

            # Remove elements that are not having any value
            for item_key in list(emailListenerInfo.keys()):
                if not emailListenerInfo[item_key]:
                    emailListenerInfo.pop(item_key)

            # Determine the type of invoice and set the appropriate key
            invoice_type = emailListenerInfo.get("Invoice_Type")
            if invoice_type not in ["DSD", "Corporate Expense"]:
                return {"result": "Invalid Invoice_Type provided"}

            # Retrieve the existing record from the database
            existing_config = db.query(model.FRConfiguration).first()

            if not existing_config:
                return {"result": "No existing configuration found"}

            # Initialize the email_listener_info list
            # if it doesn't exist or is not correctly formatted
            email_listener_info_list = existing_config.email_listener_info
            if not email_listener_info_list or not isinstance(
                email_listener_info_list, list
            ):
                email_listener_info_list = [{"DSD": {}}, {"Corporate Expense": {}}]
            else:
                # Ensure the list has at least two elements
                if len(email_listener_info_list) < 2:
                    email_listener_info_list = [{"DSD": {}}, {"Corporate Expense": {}}]
                elif len(email_listener_info_list) == 1:
                    if "DSD" in email_listener_info_list[0]:
                        email_listener_info_list.append({"Corporate Expense": {}})
                    else:
                        email_listener_info_list.insert(0, {"DSD": {}})

            # Update the relevant entry in the email_listener_info list
            if invoice_type == "DSD":
                email_listener_info_list[0]["DSD"] = emailListenerInfo
            elif invoice_type == "Corporate Expense":
                email_listener_info_list[1]["Corporate Expense"] = emailListenerInfo

            # Update the email_listener_info column
            db.query(model.FRConfiguration).update(
                {"email_listener_info": email_listener_info_list}
            )
            db.commit()
            return {"result": "Updated", "records": email_listener_info_list}
        else:
            return {"result": "No email_listener_info provided"}

    except Exception:
        # Log the exception with application logging
        logger.error(traceback.format_exc())
        return Response(
            status_code=500,
            content="Failed to save email info, please try again later.",
        )
    finally:
        db.close()


async def get_email_info(db):
    """This function retrieves the email_listener_info for a given
    configuration.

    :param db: SQLAlchemy session to interact with the backend Database.
    :return: A result dictionary containing the email_listener_info.
    """
    try:
        # Retrieve the email_listener_info from the database
        email_listener_info = db.query(
            model.FRConfiguration.email_listener_info
        ).first()

        if email_listener_info:
            return {"result": "Success", "email_listener_info": email_listener_info}
        else:
            return {"result": "No email_listener_info found"}

    except Exception:
        # Log the exception with application logging
        logger.error(traceback.format_exc())
        return Response(
            status_code=500,
            content="Failed to retrieve email info, please try again later.",
        )
    finally:
        db.close()


def check_same_vendors_same_entity(vendoraccountId, modelname, db):
    try:
        # Get the account for the given vendoraccountId
        account = (
            db.query(model.VendorAccount.Account)
            .filter(model.VendorAccount.idVendorAccount == vendoraccountId)
            .scalar()
        )

        # Get the vendorname for the account
        vendorname = (
            db.query(model.Vendor.VendorName)
            .filter(model.Vendor.VendorCode == account)
            .scalar()
        )

        # # Get all vendor codes associated with the vendor name as a list
        vendorcodes = [
            vc[0]
            for vc in db.query(model.Vendor.VendorCode)
            .filter(
                model.Vendor.VendorName == vendorname,
                func.jsonb_extract_path_text(
                    model.Vendor.miscellaneous, "VENDOR_STATUS"
                )
                == "A",
            )
            .all()
        ]

        # Initialize count and vendorName
        count = 0
        # Loop through each vendorcode in the list of vendorcodes

        # Get all VendorAccount IDs associated with the current vendorcode
        vendors = (
            db.query(model.VendorAccount.idVendorAccount)
            .filter(model.VendorAccount.Account.in_(vendorcodes))
            .all()
        )
        # Flatten the result from list of tuples to a list of IDs
        vendors = [vendor[0] for vendor in vendors]
        for v in vendors:
            if v != vendoraccountId:
                print(v)
                checkmodel = (
                    db.query(model.DocumentModel)
                    .filter(
                        model.DocumentModel.idVendorAccount == v,
                        model.DocumentModel.modelName == modelname,
                    )
                    .first()
                )

                if checkmodel is None:
                    count += 1

        # Return the result based on the count
        if count >= 1:
            return {"message": "exists", "vendor": vendorname, "count": count}
        else:
            return {"message": "not exists", "vendor": vendorname, "count": count}
    except Exception:
        print(traceback.format_exc())
        return {"message": "exception"}
    finally:
        db.close()


def copymodels(vendoraccountId, modelname, db):
    try:
        # Get the account for the given vendoraccountId
        account = (
            db.query(model.VendorAccount.Account)
            .filter(model.VendorAccount.idVendorAccount == vendoraccountId)
            .scalar()
        )

        # Get the vendorname for the account
        vendorname = (
            db.query(model.Vendor.VendorName)
            .filter(model.Vendor.VendorCode == account)
            .scalar()
        )

        # # Get all vendor codes associated with the vendor name as a list
        vendorcodes = [
            vc[0]
            for vc in db.query(model.Vendor.VendorCode)
            .filter(model.Vendor.VendorName == vendorname)
            .all()
        ]

        # Loop through each vendorcode in the list of vendorcodes
        for vendorcode in vendorcodes:
            # Get all VendorAccount IDs associated with the current vendorcode
            vendors = (
                db.query(model.VendorAccount.idVendorAccount)
                .filter(model.VendorAccount.Account == vendorcode)
                .all()
            )

        docmodelqr = (
            "SELECT * FROM "
            + DB
            + ".documentmodel WHERE idVendorAccount="
            + str(vendoraccountId)
            + " and modelName = '"
            + modelname
            + "';"
        )
        docmodel = pd.read_sql(docmodelqr, SQLALCHEMY_DATABASE_URL)
        inputmodel = {}
        for d in docmodel.head():
            inputmodel[d] = docmodel[d][0]
        model_id = inputmodel["idDocumentModel"]
        frmetadataqr = (
            "SELECT * FROM "
            + DB
            + ".frmetadata WHERE idInvoiceModel="
            + str(model_id)
            + ""
        )
        frmetadatares = pd.read_sql(frmetadataqr, SQLALCHEMY_DATABASE_URL)
        frmetadata = {}
        for f in frmetadatares.head():
            frmetadata[f] = frmetadatares[f][0]
        del frmetadata["idFrMetaData"]
        del inputmodel["idDocumentModel"]
        allmodelid = []
        for v in vendors:
            if v[0] != vendoraccountId:
                iddocqr = (
                    db.query(model.DocumentModel.idDocumentModel)
                    .filter(
                        model.DocumentModel.idVendorAccount == v[0],
                        model.DocumentModel.modelName == modelname,
                    )
                    .first()
                )
                if iddocqr is None:
                    inputmodel["idVendorAccount"] = v[0]
                    inputmodel["CreatedOn"] = datetime.utcnow().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    inputmodel["UpdatedOn"] = inputmodel["CreatedOn"]
                    invoiceModelDB = model.DocumentModel(**inputmodel)
                    db.add(invoiceModelDB)
                    db.commit()
                    print(v[0])
                    allmodelid.append(invoiceModelDB.idDocumentModel)
        print(f"frmetadata {allmodelid}")
        for m in allmodelid:
            frmetadata["idInvoiceModel"] = m
            frmetaDataDB = model.FRMetaData(**frmetadata)
            db.add(frmetaDataDB)
            db.commit()
            print(m)
        documenttagdefqr = (
            "SELECT * FROM "
            + DB
            + ".documenttagdef WHERE idDocumentModel="
            + str(model_id)
            + ""
        )
        documenttagdefres = pd.read_sql(documenttagdefqr, SQLALCHEMY_DATABASE_URL)
        documenttagdef = []
        for i in range(len(documenttagdefres)):
            obj = {}
            for f in documenttagdefres.head():
                if f != "idDocumentTagDef":
                    obj[f] = documenttagdefres[f][i]
            documenttagdef.append(obj)
        print(f"header tag {documenttagdef}")
        for m in allmodelid:
            checktag = (
                db.query(model.DocumentTagDef)
                .filter(model.DocumentTagDef.idDocumentModel == m)
                .first()
            )
            if checktag is None:
                for d in documenttagdef:
                    d["idDocumentModel"] = m
                    documenttagdefDB = model.DocumentTagDef(**d)
                    db.add(documenttagdefDB)
                    db.commit()
        documentlinedefqr = (
            "SELECT * FROM "
            + DB
            + ".documentlineitemtags WHERE idDocumentModel="
            + str(model_id)
            + ""
        )
        documentlinedefres = pd.read_sql(documentlinedefqr, SQLALCHEMY_DATABASE_URL)
        documentlinedef = []
        for i in range(len(documentlinedefres)):
            obj = {}
            for f in documentlinedefres.head():
                if f != "idDocumentLineItemTags":
                    obj[f] = documentlinedefres[f][i]
            documentlinedef.append(obj)
        print(f"line tag {documentlinedef}")
        for m in allmodelid:
            checktag = (
                db.query(model.DocumentLineItemTags)
                .filter(model.DocumentLineItemTags.idDocumentModel == m)
                .first()
            )
            if checktag is None:
                for d in documentlinedef:
                    d["idDocumentModel"] = m
                    documentlinedefDB = model.DocumentLineItemTags(**d)
                    db.add(documentlinedefDB)
                    db.commit()
        return {"message": "success"}
    except Exception:
        logger.error(traceback.format_exc())
        return {"message": "exception"}
    finally:
        db.close()

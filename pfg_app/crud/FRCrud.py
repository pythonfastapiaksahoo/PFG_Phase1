import json

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

tz_region = tz.timezone("US/Pacific")


async def getFRConfig(u_id, userID, db):
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
async def updateMetadata(u_id, documentId, frmetadata, db):
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
                model.FRMetaData.idInvoiceModel == documentId
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


def createInvoiceModel(u_id, user, invoiceModel, db):
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
            invoiceModel["userID"] = user.firstName if user.firstName else user.name
            invoiceModel["update_by"] = user.firstName if user.firstName else user.name
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


def getmodellist(u_id, vendorID, db):
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
    """Retrieve the details of vendor accounts associated with a specific
    vendor ID.

    Parameters:
    ----------
    v_id : int
        The ID of the vendor whose accounts are to be retrieved.
    db : Session
        SQLAlchemy session object used to interact with the database.

    Returns:
    -------
    list
        A list of VendorAccount objects associated with the given vendor ID.

    Raises:
    ------
    Exception
        In case of an error during database interaction, a 500 response is returned.
    """
    try:
        # Query to fetch all VendorAccount entries linked to the provided vendor ID
        return (
            db.query(model.VendorAccount)
            .filter(model.VendorAccount.vendorID == v_id)
            .all()
        )
    except Exception:
        # Log the error with a stack trace
        logger.error(traceback.format_exc())
        # Return a 500 response in case of failure
        return Response(status_code=500)
    finally:
        # Ensure the database session is closed after the operation
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
    """Create a new log entry in the OCRLogs table based on an OCR run on a
    document.

    Parameters:
    ----------
    logData : Pydantic model
        A Pydantic model instance containing the log data obtained from the OCR run.
    db : Session
        SQLAlchemy session object used to interact with the database.

    Returns:
    -------
    dict
        A dictionary containing the result of the insertion and the inserted records.

    Raises:
    ------
    Exception
        In case of an error during database interaction, a 500 response is returned.
    """
    try:
        # Convert Pydantic model to a dictionary
        logData = dict(logData)
        # Add the current timestamp for editedOn
        logData["editedOn"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        db.add(model.OCRLogs(**logData))
        # Commit the transaction to save changes in the database
        db.commit()
        return {"result": "Updated", "records": logData}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def addItemMapping(mapData, db):
    """Create a new user-defined item mapping in the database.

    Parameters:
    ----------
    mapData : Pydantic model
        A Pydantic model instance containing the item mapping data.
    db : Session
        SQLAlchemy session object used to interact with the database.

    Returns:
    -------
    dict
        A dictionary containing the result of the insertion and the inserted records.

    Raises:
    ------
    Exception
        In case of an error during database interaction, a 500 response is returned.
    """
    try:
        # Convert Pydantic model to a dictionary
        mapData = dict(mapData)
        # Add creation timestamp
        mapData["createdOn"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        # Create a new UserItemMapping instance and add it to the session
        db.add(model.UserItemMapping(**mapData))
        # Commit the transaction to save changes in the database
        db.commit()
        return {"result": "Updated", "records": mapData}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def addfrMetadata(m_id: int, r_id: int, n_fr_mdata, db):
    """Add a new user-defined FR metadata item mapping to the database.

    Parameters:
    ----------
    m_id : int
        The ID of the invoice model to which the metadata is associated.
    r_id : int
        The ID of the rule related to the metadata.
    n_fr_mdata : Pydantic model
        A Pydantic model instance containing the new FR metadata fields.
    db : Session
        SQLAlchemy session object used to interact with the database.

    Returns:
    -------
    dict
        A dictionary containing the result of the insertion and the inserted records.

    Raises:
    ------
    Exception
        In case of an error during database interaction, a 500 response is returned.
    """
    try:
        # Convert Pydantic model to a dictionary
        frmData = dict(n_fr_mdata)
        # Add required fields to the metadata
        frmData["idInvoiceModel"] = m_id
        frmData["ruleID"] = r_id
        # Create a new FRMetaData instance and add it to the session
        db.add(model.FRMetaData(**frmData))
        # Commit the transaction to save changes in the database
        db.commit()
        return {"result": "Inserted", "records": frmData}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def getMetaData(u_id, documentId: int, db):
    """Retrieve metadata for a specific document, including vendor synonyms and
    FR metadata.

    Parameters:
    ----------
    documentId : int
        The ID of the document for which metadata is being retrieved.
    db : Session
        SQLAlchemy session object used to interact with the database.

    Returns:
    -------
    dict
        A dictionary containing FR metadata and vendor synonyms.

    Raises:
    ------
    Exception
        In case of an error during database interaction, a 500 response is returned.
    """
    merged_data = {}
    try:
        # Retrieve vendor account ID for the given document ID
        vendor_account_id = (
            db.query(model.DocumentModel.idVendorAccount)
            .filter(model.DocumentModel.idDocumentModel == documentId)
            .scalar()
        )
        # Retrieve vendor ID using the vendor account ID
        vendor_id = (
            db.query(model.VendorAccount.vendorID)
            .filter(model.VendorAccount.idVendorAccount == vendor_account_id)
            .scalar()
        )
        # Retrieve synonyms for the vendor ID
        synonyms = (
            db.query(model.Vendor.Synonyms)
            .filter(model.Vendor.idVendor == vendor_id)
            .scalar()
        )
        # Retrieve FR metadata for the document ID
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


async def getTrainTestRes(u_id, modelId: int, db):
    """Retrieve the training and test results for a specific document model.

    Parameters:
    ----------
    modelId : int
        The ID of the document model for which the results are being retrieved.
    db : Session
        SQLAlchemy session object used to interact with the database.

    Returns:
    -------
    DocumentModel or None
        The DocumentModel object containing training and test results if found,
        or None if not found.

    Raises:
    ------
    Exception
        In case of an error during database interaction, a 500 response is returned.
    """
    try:
        # Return the found DocumentModel or None if not found
        return (
            db.query(model.DocumentModel)
            .options(load_only("training_result", "test_result"))
            .filter(model.DocumentModel.idDocumentModel == modelId)
            .first()
        )
    except Exception:
        # Log the error with a stack trace
        logger.error(traceback.format_exc())
        # Return a 500 response in case of failure
        return Response(status_code=500)
    finally:
        # Ensure the database session is closed after the operation
        db.close()


async def getActualAccuracy(u_id, tp: str, nm: str, db):
    """Retrieve accuracy metrics for documents associated with a specific
    vendor.

    Parameters:
    ----------
    tp : str
        Specifies the type of entity; either 'vendor' or 'service provider'.
    nm : str
        The name of the vendor or service provider to filter the results.
    db : Session
        SQLAlchemy session object used to interact with the database.

    Returns:
    -------
    dict
        A dictionary containing accuracy metrics for each tag label,
        along with the document count.
        The structure includes:
        - Tag labels as keys
            - 'miss': Count of errors or updates
            - 'match': Count of successful matches
        - 'DocumentCount': A dictionary with the total count of documents processed.

    Raises:
    ------
    Exception
        In case of an error during database interaction, a 500 response is returned.
    """
    try:
        # Query for accuracy data based on the entity type (vendor or service provider)
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
        # Initialize dictionary to hold accuracy metrics
        final_dict: Dict[str, Dict[str, int]] = {}
        documents = []  # Track unique document IDs
        for a in accuracy_data:
            documentid = a.Document.idDocument
            if documentid not in documents:
                documents.append(a.Document.idDocument)
            key = a.DocumentTagDef.TagLabel
            iserror = a.DocumentData.isError
            isupdated = a.DocumentData.IsUpdated

            # Initialize the dictionary entry for the tag label if not present
            if key not in final_dict or not isinstance(final_dict[key], dict):
                final_dict[key] = {"miss": 0, "match": 0}
            elif isinstance(final_dict[key], dict):
                # Update counts based on error and update flags
                if iserror == 1 or isupdated == 1:
                    final_dict[key]["miss"] += 1
                else:
                    final_dict[key]["match"] += 1
        # Store the document count as a dictionary
        final_dict["DocumentCount"] = {
            "count": len(documents)
        }  # instead of int value, it should be
        # a dict to avoid confusion and type issues
        return final_dict
    except Exception:
        # Log the error for debugging
        logger.error(traceback.format_exc())
        # Return a 500 response in case of failure
        return Response(status_code=500)
    finally:
        # Ensure the database session is closed after the operation
        db.close()


async def getActualAccuracyByEntity(u_id, type, db):
    """This function retrieves accuracy metrics of documents based on the
    entity type.

    Parameters:
    ----------
    type : str
        Specifies the entity type; either 'vendor' or another entity type.
    db : Session
        SQLAlchemy session object used to interact with the database.

    Returns:
    -------
    dict
        A dictionary containing accuracy metrics for each entity and tag label.

    Raises:
    ------
    Exception
        In case of an error during database interaction, a 500 response is returned.
    """
    try:
        # Define the base query based on the entity type
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
        # Iterate through each entity and compile accuracy metrics
        for a in entitydata:
            entity = a.Entity.EntityName
            if entity not in final_dict.keys():
                final_dict[entity] = {}
                # Query all data for the current entity
                data = accuracy_data.filter(model.Entity.EntityName == entity).all()
                for d in data:
                    key = d.DocumentTagDef.TagLabel
                    iserror = d.DocumentData.isError
                    isupdated = d.DocumentData.IsUpdated
                    # Initialize dictionary entry for the tag label if it does not exist
                    if key not in final_dict[entity].keys():
                        final_dict[entity][key] = {
                            "Total Invoices": 0,
                            "Miss": 0,
                            "Match": 0,
                            "Accuracy": 0,
                        }
                    else:
                        # Increment match or miss counters based on error and
                        # update flags
                        if iserror == 1 or isupdated == 1:
                            final_dict[entity][key]["Miss"] += 1
                        else:
                            final_dict[entity][key]["Match"] += 1
                    # Update total invoices and calculate accuracy
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
        return final_dict  # Return the final accuracy metrics
    except Exception:
        # Log the exception for debugging
        logger.error(traceback.format_exc())
        # Return a 500 response in case of failure
        return Response(status_code=500)
    finally:
        # Ensure the database session is closed after the operation
        db.close()


async def getall_tags(tagtype, db):
    """This function retrieves both header and line tags from the database
    based on the tag type.

    The tags are filtered by their type (either 'Header' or 'line') and the provided
    tag type (e.g., Invoice, Purchase Order).

    Parameters:
    ----------
    tagtype : str
        The type of tags to retrieve (e.g., 'Invoice', 'Purchase Order').
    db : Session
        SQLAlchemy session object used to interact with the database.

    Returns:
    -------
    dict
        A dictionary containing two keys:
        - "header": List of header tags.
        - "line": List of line tags.

    Raises:
    ------
    Exception
        In case of an error during database interaction, a 500 response is returned.
    """
    try:
        # Query to fetch all header tags for the given tagtype
        header_tags = (
            db.query(model.DefaultFields)
            .options(load_only("Name", "Ismendatory"))
            .filter(
                model.DefaultFields.Type == "Header",
                model.DefaultFields.TagType == tagtype,
            )
            .all()
        )
        # Query to fetch all line tags for the given tagtype
        line_tags = (
            db.query(model.DefaultFields)
            .options(load_only("Name", "Ismendatory"))
            .filter(
                model.DefaultFields.Type == "line",
                model.DefaultFields.TagType == tagtype,
            )
            .all()
        )
        # Return the header and line tags in a dictionary format
        return {"header": header_tags, "line": line_tags}
    except Exception:
        # Log the error with full traceback for debugging purposes
        logger.error(traceback.format_exc())
        # Return a 500 response in case of failure
        return Response(status_code=500)
    finally:
        # Ensure the database session is properly closed after the operation
        db.close()


async def get_entity_level_taggedInfo(u_id, db):
    """Retrieves the entity-level tagged information for vendors and saves the
    result as an Excel file.

    The function joins several tables: Entity, Vendor, VendorAccount, DocumentModel,
    and FRMetaData
    to extract metadata tags for each vendor. The final result is saved as an Excelfile.

    Parameters:
    ----------
    db : Session
        SQLAlchemy session object, used to interact with the backend database.

    Returns:
    -------
    str
        The filename of the Excel file containing the retrieved tagged information.
    """
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
    """This function retrieves the list of document rules from the Rule table.

    Parameters:
    ----------
    db : Session
        SQLAlchemy session object, used to interact with the backend database.

    Returns:
    -------
    list
        A list of Rule objects if the query is successful.
    Response
        A 500 status code response in case of an exception.
    """
    try:
        # Return the result as a list of Rule objects
        return db.query(model.Rule).all()
    except Exception:
        # Log the exception traceback for debugging purpose
        logger.error(traceback.format_exc())
        # Return a 500 Internal Server Error response in case of failure
        return Response(status_code=500)
    finally:
        db.close()


async def readnewdocrules(db):
    """This function retrieves the list of new document rules from the AGIRule
    table.

    Parameters:
    ----------
    db : Session
        SQLAlchemy session object, used to interact with the backend database.

    Returns:
    -------
    list
        A list of AGIRule objects if the query is successful.
    Response
        A 500 status code response in case of an exception.
    """
    try:
        # Return the result as a list of AGIRule objects
        return db.query(model.AGIRule).all()
    except Exception:
        # Log the exception traceback for debugging purposes
        logger.error(traceback.format_exc())
        # Return a 500 Internal Server Error response in case of failure
        return Response(status_code=500)
    finally:
        db.close()


async def update_email_info(emailInfo, db):
    """This function updates the email_listener_info for a given user in the
    FRConfiguration table.

    Parameters:
    ----------
    emailInfo : PydanticModel
        Form Recognizer Configuration Pydantic model containing email
        listener information.
    db : Session
        SQLAlchemy session used to interact with the backend database.

    Returns:
    -------
    dict
        A dictionary containing the result of the update operation.
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
    """This function retrieves the email listener information from the
    configuration table.

    Parameters:
    ----------
    db : Session
        SQLAlchemy session used to interact with the backend database.

    Returns:
    -------
    dict
        A dictionary containing the result of the operation and the email listener
        info if available.
        If no information is found, it returns a message indicating that.
    """
    try:
        # Query the database to retrieve the email_listener_info from FRConfiguration
        email_listener_info = db.query(
            model.FRConfiguration.email_listener_info
        ).first()

        if email_listener_info:
            # Return success message along with the email listener info
            return {"result": "Success", "email_listener_info": email_listener_info}
        else:
            # Return message if no email_listener_info is found
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
    """This function checks whether a document model with the specified name
    exists across multiple vendor accounts that share the same vendor name as
    the provided vendor account. It looks for active vendors and counts how
    many do not have the specified document model.

    Parameters:
    ----------
    vendoraccountId : int
        The ID of the vendor account from which to check for the document model.
    modelname : str
        The name of the document model to be checked across different vendors.
    db : Session
        The database session object used to interact with the database.

    Returns:
    -------
    dict
        A dictionary containing a message indicating whether the document model exists,
        the vendor name, and a count of missing models.
    """
    try:
        # Retrieve the account associated with the provided vendoraccountId
        account = (
            db.query(model.VendorAccount.Account)
            .filter(model.VendorAccount.idVendorAccount == vendoraccountId)
            .scalar()
        )

        # Retrieve the vendor name associated with the vendor account
        vendorname = (
            db.query(model.Vendor.VendorName)
            .filter(model.Vendor.VendorCode == account)
            .scalar()
        )

        # Retrieve all vendor codes with the same vendor name and active status
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
    """This function copies a document model and its associated metadata, tag
    definitions, and line item tags from one vendor account to other vendor
    accounts that share the same vendor name. It performs the following steps:

    1. Retrieves the vendor name for the given vendor account.
    2. Finds all other vendor accounts associated with the same vendor name.
    3. Copies the document model, metadata, tag definitions, and line item tags
        for the provided vendor account to all other associated vendor accounts.

    Parameters:
    ----------
    vendoraccountId : int
        The ID of the vendor account from which the document model is copied.
    modelname : str
        The name of the document model to be copied.
    db : Session
        The database session object used to interact with the database.

    Returns:
    -------
    dict
        A dictionary containing a success message or an exception message
        in case of an error.
    """
    try:
        # Retrieve the account associated with the provided vendoraccountId
        account = (
            db.query(model.VendorAccount.Account)
            .filter(model.VendorAccount.idVendorAccount == vendoraccountId)
            .scalar()
        )

        # Retrieve the vendor name associated with the vendor account
        vendorname = (
            db.query(model.Vendor.VendorName)
            .filter(model.Vendor.VendorCode == account)
            .scalar()
        )

        # Retrieve all vendor codes with the same vendor name and active status
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

        # Retrieve all VendorAccount IDs associated with the vendor codes
        vendors = (
            db.query(model.VendorAccount.idVendorAccount)
            .filter(model.VendorAccount.Account.in_(vendorcodes))
            .all()
        )
        # Flatten the result into a list of vendor IDs
        vendors = [vendor[0] for vendor in vendors]

        # Retrieve the document model for the given vendor account and model name
        docmodel = (
            db.query(model.DocumentModel)
            .filter(
                model.DocumentModel.idVendorAccount == vendoraccountId,
                model.DocumentModel.modelName == modelname,
            )
            .one()
        )

        # Convert the document model to a dictionary for modification
        inputmodel = docmodel.to_dict()
        # Get the model ID
        model_id = inputmodel.get("idDocumentModel")

        # Retrieve FRMetadata for the document model
        frmetadatares = (
            db.query(model.FRMetaData)
            .filter(model.FRMetaData.idInvoiceModel == model_id)
            .one()
        )

        # Convert FRMetadata to a dictionary and remove its ID to avoid duplication
        frmetadata = frmetadatares.to_dict()
        # Safely delete 'idFrMetaData' if it exists
        frmetadata.pop(
            "idFrMetaData", None
        )  # Will not raise KeyError if key is missing
        del inputmodel["idDocumentModel"]

        allmodelid = []
        # Loop through all vendors and copy the model where it does not already exist
        for v in vendors:
            if v != vendoraccountId:
                # Check if a document model for this vendor account already exists
                iddocqr = (
                    db.query(model.DocumentModel.idDocumentModel)
                    .filter(
                        model.DocumentModel.idVendorAccount == v,
                        model.DocumentModel.modelName == modelname,
                    )
                    .first()
                )
                # If no existing model, create a new one
                if iddocqr is None:
                    inputmodel["idVendorAccount"] = v
                    inputmodel["CreatedOn"] = datetime.utcnow().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    inputmodel["UpdatedOn"] = inputmodel["CreatedOn"]

                    # Insert new DocumentModel
                    invoiceModelDB = model.DocumentModel(**inputmodel)
                    db.add(invoiceModelDB)
                    db.commit()
                    allmodelid.append(invoiceModelDB.idDocumentModel)

        # Copy FRMetadata for all the newly created models
        for m in allmodelid:
            frmetadata["idInvoiceModel"] = m
            frmetaDataDB = model.FRMetaData(**frmetadata)
            db.add(frmetaDataDB)
            db.commit()

        # Retrieve DocumentTagDef associated with the original document model
        documenttagdefres = (
            db.query(model.DocumentTagDef)
            .filter(model.DocumentTagDef.idDocumentModel == model_id)
            .all()
        )
        # Prepare a list of tag definitions to be copied
        documenttagdef = []
        for tag_def in documenttagdefres:
            new_tag_def = tag_def.to_dict()
            del new_tag_def["idDocumentTagDef"]
            documenttagdef.append(new_tag_def)

        # Copy DocumentTagDef for all new models
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
                    # Create and add the new DocumentTagDef entry
                    db.add(documenttagdefDB)
                    db.commit()

        # Query for DocumentLineItemTags associated with the original document model
        documentlinedefres = (
            db.query(model.DocumentLineItemTags)
            .filter(model.DocumentLineItemTags.idDocumentModel == model_id)
            .all()
        )
        # Prepare a list of line item tags to be copied
        documentlinedef = []
        for line_tag in documentlinedefres:

            new_line_item = line_tag.to_dict()
            del new_line_item["idDocumentLineItemTags"]
            documenttagdef.append(new_line_item)
        # Copy DocumentLineItemTags for all new models
        for m in allmodelid:
            checktag = (
                db.query(model.DocumentLineItemTags)
                .filter(model.DocumentLineItemTags.idDocumentModel == m)
                .first()
            )
            if checktag is None:
                for d in documentlinedef:
                    d["idDocumentModel"] = m
                    # Add the new instance to the session
                    db.add(**d)
                    db.commit()

        return {"message": "success"}

    except Exception:
        logger.error(traceback.format_exc())
        return {"message": "exception"}

    finally:
        db.close()

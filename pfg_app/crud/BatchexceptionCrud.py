import base64
import json
import os
import re
import traceback
from datetime import datetime
from itertools import groupby
from typing import Any

import pandas as pd
import pytz as tz
from azure.storage.blob import BlobServiceClient
from fastapi.responses import Response
from sqlalchemy import and_, case, or_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Load, Session, load_only

import pfg_app.model as model
from pfg_app import settings
from pfg_app.core.utils import get_credential
from pfg_app.logger_module import logger
from pfg_app.session.session import DB, SQLALCHEMY_DATABASE_URL, engine

po_tag_map = {
    "PurchQty": "Quantity",
    "Name": "Description",
    "PurchId": "PO_HEADER_ID",
    "Qty": "Quantity",
    "POLineNumber": "PO_LINE_ID",
}
tz_region = tz.timezone("US/Pacific")


# hello world
async def switch_data(u_id: int, inv_id: int, dataswitch: bool, db: Session):
    try:
        if inv_id != 0:
            if dataswitch:
                print("prebuilt")
                prebuilt_data = (
                    "SELECT DocPrebuiltData FROM "
                    + DB
                    + ".dataswitch where documentID ='"
                    + str(inv_id)
                    + "';"
                )
                prebuilt_data_df = pd.read_sql_query(
                    prebuilt_data, SQLALCHEMY_DATABASE_URL
                )

                result = prebuilt_data_df.reset_index(drop=True).to_json(
                    orient="records"
                )
                result = json.loads(result)
                if isinstance(result, list) and result:
                    doc_prebuilt_data = json.loads(
                        result[0].get("DocPrebuiltData", "{}")
                    )
                else:
                    raise ValueError("Unexpected result format")

                prebuilt_data = doc_prebuilt_data["prebuilt"]
                result_dict: dict[str, Any] = {}
                for d in prebuilt_data:
                    if (
                        isinstance(d, dict)
                        and "data" in d
                        and "tag" in d
                        and "value" in d["data"]
                    ):
                        result_dict[d["tag"]] = d["data"]["value"]
                # result_dict = {d["tag"]: d["data"]["value"] for d in prebuilt_data}
                # # commented  this line and added above line
                print(result_dict)

                update_query = """
                UPDATE {DB}.documentdata AS t2
                JOIN {DB}.documenttagdef AS t1
                ON t2.documentTagDefID = t1.idDocumentTagDef
                SET t2.value = CASE
                    {update_cases}
                    ELSE t2.value
                END
                WHERE t2.documentID = {inv_id};
                """.format(
                    DB=DB,
                    inv_id=inv_id,
                    update_cases=" ".join(
                        "WHEN t1.TagLabel = '{taglabel}' THEN '{tagvalue}'".format(
                            taglabel=taglabel, tagvalue=result_dict.get(taglabel, "")
                        )
                        for taglabel in result_dict.keys()
                    ),
                )
            else:
                print("custom")
                custom_data = (
                    "SELECT DocCustData FROM "
                    + DB
                    + ".dataswitch where documentID ='"
                    + str(inv_id)
                    + "';"
                )
                custom_data_df = pd.read_sql_query(custom_data, SQLALCHEMY_DATABASE_URL)

                result = custom_data_df.reset_index(drop=True).to_json(orient="records")
                result = json.loads(result)
                doc_custom_data = json.loads(result[0]["DocCustData"])  # type: ignore

                custom_data = doc_custom_data["custom"]
                result_dict_custom: dict[str, Any] = {}
                for d in custom_data:
                    if (
                        isinstance(d, dict)
                        and "data" in d
                        and "tag" in d
                        and "value" in d["data"]
                    ):
                        result_dict_custom[d["tag"]] = d["data"]["value"]
                # result_dict = {d["tag"]: d["data"]["value"] for d in custom_data}
                # # commented  this line and added above line
                print(result_dict_custom)

                update_query = """
                UPDATE {DB}.documentdata AS t2
                JOIN {DB}.documenttagdef AS t1 ON
                t2.documentTagDefID = t1.idDocumentTagDef
                SET t2.value = CASE
                    {update_cases}
                    ELSE t2.value
                END
                WHERE t2.documentID = {inv_id};
                """.format(
                    DB=DB,
                    inv_id=inv_id,
                    update_cases=" ".join(
                        "WHEN t1.TagLabel = '{taglabel}' THEN '{tagvalue}'".format(
                            taglabel=taglabel,
                            tagvalue=result_dict_custom.get(taglabel, ""),
                        )
                        for taglabel in result_dict_custom.keys()
                    ),
                )
                result_dict = result_dict_custom  # added this line due to custom data

            print("result :", result_dict)
            engine.execute(update_query)
            db.commit()
            return result_dict

        else:
            print("Invalid Document id", inv_id)

    except Exception as e:
        logger.error(f"Error in switch_data: {e}")
        return Response(status_code=500)


async def update_entity(
    u_id: int, inv_id: int, entity_id: int, entitybody_id: int, db: Session
):
    try:

        doc_val = (
            "SELECT * FROM " + DB + ".document where idDocument ='" + str(inv_id) + "';"
        )
        doc_val_df = pd.read_sql(doc_val, SQLALCHEMY_DATABASE_URL)

        entitiId_inv = list(doc_val_df["entityID"])[0]
        documentStatus_ID = list(doc_val_df["documentStatusID"])[0]
        documentsubstatus_ID = list(doc_val_df["documentsubstatusID"])[0]

        id_DocumentModel = list(doc_val_df["documentModelID"])[0]
        doctsgdef_val = (
            "SELECT * FROM "
            + DB
            + ".documenttagdef where TagLabel = 'PurchaseOrder' and idDocumentModel = '"
            + str(id_DocumentModel)
            + "';"
        )
        doctsgdef_val_df = pd.read_sql(doctsgdef_val, SQLALCHEMY_DATABASE_URL)
        id_DocumentTagDef = list(doctsgdef_val_df["idDocumentTagDef"])[0]

        if len(doc_val_df) > 0:

            if int(entitiId_inv) != int(entity_id):

                db.query(model.Document).filter_by(idDocument=inv_id).update(
                    {"entityID": entity_id, "entityBodyID": entitybody_id}
                )

                db.commit()

                # updated entity id------------
                updated_eid = (
                    "select EntityCode from "
                    + DB
                    + ".entity where idEntity="
                    + str(entity_id)
                    + ";"
                )
                updated_eid_df = pd.read_sql(updated_eid, SQLALCHEMY_DATABASE_URL)
                updated_eid_val = list(updated_eid_df["EntityCode"])[0]
                print("updated_eid_val", updated_eid_val)

                # entity id of PO--------
                po_val = (
                    "SELECT PODocumentID FROM "
                    + DB
                    + ".document where idDocument ='"
                    + str(inv_id)
                    + "';"
                )
                po_val_df = pd.read_sql(po_val, SQLALCHEMY_DATABASE_URL)
                po_num = list(po_val_df["PODocumentID"])[0]

                extracted_po_eid = "".join(re.split("[^a-zA-Z]*", po_num))
                extracted_po_number = "".join(re.split("[^0-9]*", po_num))

                if len(extracted_po_eid) >= 3:
                    extracted_po_eid = extracted_po_eid[0:3]
                    print("extracted_po_eid", extracted_po_eid)

                if extracted_po_eid != updated_eid_val:
                    print("updated_eid_val1", updated_eid_val)
                    print("extracted_po_eid1", extracted_po_eid)

                    formatted_po = updated_eid_val + "-PO-" + extracted_po_number

                    db.query(model.Document).filter_by(idDocument=inv_id).update(
                        {"PODocumentID": formatted_po}
                    )

                    db.query(model.DocumentData).filter_by(
                        documentID=inv_id, documentTagDefID=id_DocumentTagDef
                    ).update({"Value": formatted_po, "ErrorDesc": "PO Updated"})

                    db.commit()

                else:
                    db.query(model.DocumentData).filter_by(
                        documentID=inv_id, documentTagDefID=id_DocumentTagDef
                    ).update({"ErrorDesc": "PO Updated"})

                    db.commit()

                # inserting entry into documenthistory table---------------
                created_on = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                doc_history = {
                    "documentID": inv_id,
                    "userID": u_id,
                    "CreatedOn": created_on,
                    "documentdescription": "Entity update Successfull",
                    "documentStatusID": documentStatus_ID,
                    "documentSubStatusID": documentsubstatus_ID,
                }
                doc_history = model.DocumentHistoryLogs(**doc_history)
                db.add(doc_history)
                db.commit()

                # updating model id ----------------
                vendorAccount_ID = list(doc_val_df["vendorAccountID"])[0]
                Prev_documentModel_ID = list(doc_val_df["documentModelID"])[0]

                idvendoraccount_val = (
                    "(SELECT idVendorAccount FROM "
                    + DB
                    + ".vendoraccount where Account = (SELECT Account FROM "
                    + DB
                    + ".vendoraccount where idVendorAccount = "
                    + str(vendorAccount_ID)
                    + ") and entityID = "
                    + str(entity_id)
                    + " and entityBodyID ="
                    + str(entitybody_id)
                    + ")"
                )
                new_docmodelid_val = (
                    "SELECT idDocumentModel FROM "
                    + DB
                    + ".documentmodel where idVendorAccount ="
                    + str(idvendoraccount_val)
                    + ";"
                )
                new_docmodelid_df = pd.read_sql(
                    new_docmodelid_val, SQLALCHEMY_DATABASE_URL
                )
                new_docmodelid = list(new_docmodelid_df["idDocumentModel"])[0]

                if Prev_documentModel_ID != new_docmodelid:
                    print("Prev_documentModel_ID", Prev_documentModel_ID)
                    print("new_docmodelid", new_docmodelid)

                    db.query(model.Document).filter_by(idDocument=inv_id).update(
                        {"documentModelID": new_docmodelid}
                    )

                    # Update Document Tag def ------
                    # Fetching data
                    docdata_val = (
                        "select idDocumentData,documentTagDefID from "
                        + DB
                        + ".documentdata where documentID ='"
                        + str(inv_id)
                        + "';"
                    )
                    docdata_val_df = pd.read_sql(docdata_val, SQLALCHEMY_DATABASE_URL)
                    print(docdata_val_df)

                    new_val = (
                        "select idDocumentTagDef,TagLabel from "
                        + DB
                        + ".documenttagdef where idDocumentModel ='"
                        + str(new_docmodelid)
                        + "';"
                    )
                    new_val_df = pd.read_sql(new_val, SQLALCHEMY_DATABASE_URL)
                    new_val_df.columns = [
                        "idDocumentTagDef_new",
                        "TagLabel",
                    ]  # type: ignore
                    print("new_val_df : ", new_val_df)

                    old_val = (
                        "select idDocumentTagDef,TagLabel from "
                        + DB
                        + ".documenttagdef where idDocumentModel ='"
                        + str(Prev_documentModel_ID)
                        + "';"
                    )
                    old_val_df = pd.read_sql(old_val, SQLALCHEMY_DATABASE_URL)
                    old_val_df.columns = [
                        "idDocumentTagDef_old",
                        "TagLabel",
                    ]  # type: ignore
                    print("old_val_df : ", old_val_df)
                    # hii

                    Difference = set(old_val_df["TagLabel"]) - set(
                        new_val_df["TagLabel"]
                    )
                    print(len(Difference))
                    if len(Difference) > 0:
                        # Append to metadata
                        dochist1 = (
                            "INSERT INTO "
                            + DB
                            + ".documenttagdef (idDocumentModel, TagLabel) values "
                        )
                        for dif in Difference:
                            dochist1 = (
                                dochist1
                                + "("
                                + str(new_docmodelid)
                                + ',"'
                                + str(dif)
                                + '"'
                                + "), "
                            )
                        dochist1 = dochist1[:-2] + ";"
                        engine.execute(dochist1)
                        print(dochist1)

                        # Read DataFrame if any new valves are appended
                        new_val = (
                            "select idDocumentTagDef,TagLabel from "
                            + DB
                            + ".documenttagdef where idDocumentModel ='"
                            + str(new_docmodelid)
                            + "';"
                        )
                        new_val_df = pd.read_sql(new_val, SQLALCHEMY_DATABASE_URL)
                        new_val_df.columns = [
                            "idDocumentTagDef_new",
                            "TagLabel",
                        ]  # type: ignore
                        print("new_val_df : ", new_val_df)

                    # Merge both DataFrame and drop the null values
                    result_df = pd.merge(
                        new_val_df, old_val_df, on="TagLabel", how="outer"
                    )  # 'outer' means outer join - merges all values
                    result_df = result_df.dropna()
                    print("result_df", result_df)

                    update_tagID = {}

                    # Add values to dictionary

                    for row in range(len(result_df)):
                        temp_def = old_val_df[
                            old_val_df["idDocumentTagDef_old"]
                            == result_df["idDocumentTagDef_old"][row]
                        ]
                        update_tagID[list(temp_def["idDocumentTagDef_old"])[0]] = (
                            result_df["idDocumentTagDef_new"][row]
                        )

                    print("update_tagID", update_tagID)

                    # update the document tagdef ID's
                    query = (
                        f"UPDATE {DB}.documentdata SET documentTagDefID = CASE "
                        + " ".join(
                            f"WHEN documentTagDefID = {documentTagDefID} THEN {_new}"
                            for documentTagDefID, _new in update_tagID.items()
                        )
                        + f" ELSE NULL END WHERE documentID = {inv_id};"
                    )
                    engine.execute(query)
                    print(query)

                    # Update Documentlineitemtags------
                    # Fetching data
                    docdataline_val = (
                        "select idDocumentLineItems,lineItemtagID from "
                        + DB
                        + ".documentlineitems where documentID ='"
                        + str(inv_id)
                        + "';"
                    )
                    docdataline_val_df = pd.read_sql(
                        docdataline_val, SQLALCHEMY_DATABASE_URL
                    )
                    print(docdataline_val_df)

                    newline_val = (
                        "select idDocumentLineItemTags,TagName from "
                        + DB
                        + ".documentlineitemtags where idDocumentModel ='"
                        + str(new_docmodelid)
                        + "';"
                    )
                    newline_val_df = pd.read_sql(newline_val, SQLALCHEMY_DATABASE_URL)
                    newline_val_df.columns = [
                        "idDocumentLineItemTags_new",
                        "TagName",
                    ]  # type: ignore
                    print("newline_val_df : ", newline_val_df)

                    oldline_val = (
                        "select idDocumentLineItemTags,TagName from "
                        + DB
                        + ".documentlineitemtags where idDocumentModel ='"
                        + str(Prev_documentModel_ID)
                        + "';"
                    )
                    oldline_val_df = pd.read_sql(oldline_val, SQLALCHEMY_DATABASE_URL)
                    oldline_val_df.columns = [
                        "idDocumentLineItemTags_old",
                        "TagName",
                    ]  # type: ignore
                    print("oldline_val_df : ", oldline_val_df)

                    Difference1 = set(oldline_val_df["TagName"]) - set(
                        newline_val_df["TagName"]
                    )
                    print(len(Difference1))
                    if len(Difference1) > 0:
                        # Append to metadata
                        dochist = (
                            "INSERT INTO "
                            + DB
                            + ".documentlineitemtags (idDocumentModel, TagName) values "
                        )
                        for dif in Difference1:
                            dochist = (
                                dochist
                                + "("
                                + str(new_docmodelid)
                                + ',"'
                                + str(dif)
                                + '"'
                                + "), "
                            )
                        dochist = dochist[:-2] + ";"
                        engine.execute(dochist)
                        print(dochist)

                        # Read DataFrame if any new valves are appended
                        newline_val = (
                            "select idDocumentLineItemTags,TagName from "
                            + DB
                            + ".documentlineitemtags where idDocumentModel ='"
                            + str(new_docmodelid)
                            + "';"
                        )
                        newline_val_df = pd.read_sql(
                            newline_val, SQLALCHEMY_DATABASE_URL
                        )
                        newline_val_df.columns = [
                            "idDocumentLineItemTags_new",
                            "TagName",
                        ]  # type: ignore
                        print("newline_val_df : ", newline_val_df)

                    # Merge both DataFrame and drop the null values
                    resultline_df = pd.merge(
                        newline_val_df, oldline_val_df, on="TagName", how="outer"
                    )  # 'outer' means outer join - merges all values
                    resultline_df = resultline_df.dropna()
                    print("resultline_df", resultline_df)

                    updateline_tagID = {}

                    # Add values to dictionary

                    for row1 in range(len(resultline_df)):
                        templine_def = oldline_val_df[
                            oldline_val_df["idDocumentLineItemTags_old"]
                            == resultline_df["idDocumentLineItemTags_old"][row1]
                        ]
                        updateline_tagID[
                            list(templine_def["idDocumentLineItemTags_old"])[0]
                        ] = resultline_df["idDocumentLineItemTags_new"][row1]

                    print("updateline_tagID", updateline_tagID)

                    # Update the documentlineitems IDs
                    query = (
                        f"UPDATE {DB}.documentlineitems SET lineItemtagID = CASE "
                        + " ".join(
                            f"WHEN lineItemtagID = {_old} " + f"THEN {_new}"
                            for _old, _new in updateline_tagID.items()
                        )
                        + f" ELSE NULL END WHERE documentID = {inv_id};"
                    )
                    engine.execute(query)
                    print(query)

                db.commit()

    except Exception as e:
        logger.error(f"Error in update_entity: {e}")
        return Response(status_code=500)


async def readbatchprocessdetails(u_id: int, db: Session):
    """This function read a service provider account.

    It contains 2 parameter.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        iscust = (
            db.query(model.User.isCustomerUser)
            .filter_by(idUser=u_id, isActive=1)
            .scalar()
        )

        user_access = (
            db.query(model.AccessPermission.permissionDefID)
            .filter_by(userID=u_id)
            .scalar()
        )

        if user_access == 1 or user_access == 15 or user_access == 21:
            sub_status_id = [4, 31, 35, 36, 38, 39, 78]
        else:
            sub_status_id = [4, 31, 32, 35, 36, 38, 39, 40, 78]
        sub_status = case(
            [
                (
                    model.Document.documentDescription
                    == "Invoice exists but in Draft Status",
                    "Manually Created - Draft",
                ),
                (
                    model.Document.documentDescription == "Invoice exists",
                    "Manually Posted",
                ),
                (
                    model.Document.documentDescription.like(
                        "Physical remaining quantity in the unit Box%"
                    ),
                    "Overdelivery - Remaining Qty",
                ),
                (
                    or_(
                        model.Document.documentDescription.like("Number sequence%"),
                        model.Document.documentDescription.like(
                            "Purchase order is no longer confirmed%"
                        ),
                        model.Document.documentDescription.like(
                            "The fiscal period is closed for "
                            + "module Purchase order on the date%"
                        ),
                    ),
                    "PO Not Confirmed - Posting Cancelled",
                ),
                (
                    model.Document.documentDescription.like(
                        "Physical updating quantity in the inventory%"
                    ),
                    "Overdelivery - Updating Qty",
                ),
                (
                    model.Document.documentDescription.like("Overdelivery of line%"),
                    "Overdelivery - Mismatch",
                ),
                (
                    model.Document.documentDescription.like(
                        "Decimal rounding of the physical updating quantity%"
                    ),
                    "Overdelivery - Unit of Measure",
                ),
                (
                    model.Document.documentDescription.like(
                        "%was already used as on date%"
                    ),
                    "Product Receipt Used",
                ),
                (
                    model.Document.documentDescription.like("%Wrong date%"),
                    "Wrong Date Format",
                ),
                (
                    model.Document.documentDescription.like(
                        "%Selected Entity does not match with PO Format!%"
                    ),
                    "Entity Mismatch",
                ),
                (
                    model.Document.documentDescription.like("%PO status is closed!%"),
                    "PO not Open!",
                ),
                (
                    model.Document.documentDescription.like(
                        "Qty/Price Match status failed%"
                    ),
                    "Qty/Price Match status failed",
                ),
            ],
            else_=model.Document.documentDescription,
        ).label("substatus")
        # Main query
        data = (
            db.query(
                model.Document,
                model.DocumentSubStatus,
                model.VendorAccount,
                model.Vendor,
                model.Entity,
                sub_status,
            )
            .options(
                Load(model.Document).load_only(
                    "docheaderID",
                    "CreatedOn",
                    "PODocumentID",
                    "totalAmount",
                    "documentStatusID",
                    "documentsubstatusID",
                    "sender",
                    "documentDescription",
                ),
                Load(model.DocumentSubStatus).load_only("status"),
                Load(model.VendorAccount).load_only("AccountType"),
                Load(model.Vendor).load_only("VendorName"),
                Load(model.Entity).load_only("EntityName"),
            )
            .filter(
                model.Document.documentsubstatusID
                == model.DocumentSubStatus.idDocumentSubstatus,
                model.Document.entityID == model.Entity.idEntity,
                model.Document.vendorAccountID == model.VendorAccount.idVendorAccount,
                model.VendorAccount.vendorID == model.Vendor.idVendor,
                model.Document.documentStatusID == 4,
                model.Document.documentsubstatusID.not_in(list(sub_status_id)),
            )
        )
        if iscust == 1:
            sub_query_ent = (
                db.query(model.UserAccess.EntityID)
                .filter_by(UserID=u_id, isActive=1)
                .distinct()
            )
            data = data.filter(model.Document.entityID.in_(sub_query_ent)).all()
            if data:
                po_document_ids = [doc.PODocumentID for doc, _, _, _, _, _ in data]
                requestor_names = (
                    db.query(model.Document.PODocumentID, model.Document.sender)
                    .filter(
                        model.Document.idDocumentType == 1,
                        model.Document.PODocumentID.in_(po_document_ids),
                    )
                    .all()
                )
                requestor_name_map = {rn[0]: rn[1] for rn in requestor_names}

                for item in data:
                    document, document_sub_status, vendor, vendr_account, entity, _ = (
                        item
                    )
                    document.requestorName = requestor_name_map.get(
                        document.PODocumentID
                    )
        else:
            sub_query_ent = (
                db.query(model.VendorUserAccess.vendorAccountID)
                .filter_by(vendorUserID=u_id, isActive=1)
                .distinct()
            )
            data = data.filter(model.Document.vendorAccountID.in_(sub_query_ent)).all()

        return data
    except Exception as e:
        logger.error(f"Error in readbatchprocessdetails: {e}")
        print(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def send_to_batch_approval(u_id: int, rule_id: int, inv_id: int, db: Session):
    try:
        sub_status_id = (
            db.query(model.DocumentSubStatus.idDocumentSubstatus)
            .filter(model.DocumentSubStatus.status == "Batch Edit")
            .one()
        )
        old_r_id = (
            db.query(model.Document.ruleID)
            .filter(model.Document.idDocument == inv_id)
            .one()
        )
        old_rule = (
            db.query(model.Rule.Name)
            .filter(model.Rule.idDocumentRules == old_r_id[0])
            .one()
        )
        new_rule = (
            db.query(model.Rule.Name)
            .filter(model.Rule.idDocumentRules == rule_id)
            .one()
        )

        db.query(model.Document).filter_by(idDocument=inv_id).update(
            {"documentsubstatusID": sub_status_id[0]}
        )

        inv_up_id = (
            db.query(model.DocumentRuleupdates.idDocumentRulehistorylog)
            .filter_by(documentID=inv_id, type="rule")
            .all()
        )
        if len(inv_up_id) > 0:
            db.query(model.DocumentRuleupdates).filter_by(
                documentID=inv_id, IsActive=1, type="rule"
            ).update({"IsActive": 0})
        if old_r_id[0] != rule_id:
            db.query(model.Document).filter_by(idDocument=inv_id).update(
                {"ruleID": rule_id, "IsRuleUpdated": 1}
            )
            c4 = model.DocumentRuleupdates(
                documentID=inv_id,
                oldrule=old_rule[0],
                newrule=new_rule[0],
                userID=u_id,
                IsActive=1,
                type="rule",
                createdOn=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            )
            db.add(c4)
        db.commit()
        return {"result": "success"}
    except Exception as e:
        logger.error(f"Error in send_to_batch_approval: {e}")
        return Response(status_code=500)


async def send_to_manual_approval(u_id: int, inv_id: int, db: Session):
    try:
        sub_status_id = (
            db.query(model.DocumentSubStatus.idDocumentSubstatus)
            .filter(model.DocumentSubStatus.status == "Manual Check")
            .one()
        )

        db.query(model.Document).filter_by(idDocument=inv_id).update(
            {"documentsubstatusID": sub_status_id[0], "documentStatusID": 2}
        )

        db.commit()
        return {"result": "success"}
    except Exception as e:
        logger.error(f"Error in send_to_manual_approval: {e}")
        return Response(status_code=500)


async def readbatchprocessdetailsAdmin(u_id: int, db: Session):
    """This function read a service provider account.

    It contains 2 parameter.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        sub_query_ent = (
            db.query(model.UserAccess.EntityID)
            .filter_by(UserID=u_id, isActive=1)
            .distinct()
        )
        sub_status = (
            db.query(model.DocumentSubStatus.idDocumentSubstatus)
            .filter(model.DocumentSubStatus.status.in_(["Batch Edit", "Manual Check"]))
            .distinct()
        )
        approval_type = case(
            [
                (model.DocumentSubStatus.idDocumentSubstatus == 4, "Batch Approval"),
                (model.DocumentSubStatus.idDocumentSubstatus == 6, "Manual Approval"),
            ]
        ).label("Approvaltype")

        data = (
            db.query(
                model.Document,
                model.DocumentSubStatus,
                model.Rule,
                model.VendorAccount,
                model.Vendor,
                model.DocumentRuleupdates,
                approval_type,
            )
            .options(
                Load(model.VendorAccount).load_only("AccountType"),
                Load(model.Vendor).load_only("VendorName"),
                Load(model.DocumentRuleupdates).load_only("oldrule", "createdOn"),
            )
            .filter(model.Document.entityID.in_(sub_query_ent))
            .filter(model.Document.documentsubstatusID.in_(sub_status))
            .filter(
                model.Document.documentsubstatusID
                == model.DocumentSubStatus.idDocumentSubstatus
            )
            .filter(model.Document.ruleID == model.Rule.idDocumentRules)
            .filter(
                model.Document.vendorAccountID == model.VendorAccount.idVendorAccount
            )
            .filter(model.VendorAccount.vendorID == model.Vendor.idVendor)
            .join(
                model.DocumentRuleupdates,
                and_(
                    model.DocumentRuleupdates.documentID == model.Document.idDocument,
                    model.DocumentRuleupdates.type == "rule",
                ),
                isouter=True,
            )
            .filter(
                or_(
                    model.Document.IsRuleUpdated == 0,
                    model.DocumentRuleupdates.IsActive == 1,
                )
            )
            .all()
        )

        return data
    except Exception as e:
        logger.error(f"Error in readbatchprocessdetailsAdmin: {e}")
        return Response(status_code=500)


async def send_to_batch_approval_Admin(u_id: int, inv_id: int, db: Session):
    try:
        sub_statusid = (
            db.query(model.Document.documentsubstatusID)
            .filter(model.Document.idDocument == inv_id)
            .scalar()
        )
        if sub_statusid == 7:
            sub_status_idt = 3
        elif sub_statusid == 75:
            sub_status_idt = 75
        else:
            sub_status_idt = 24
        # sub_status_id = (
        #     db.query(model.DocumentSubStatus.idDocumentSubstatus)
        #     .filter(model.DocumentSubStatus.status == "Batch Edit Approved")
        #     .one()
        # )  # TODO: Unused variable

        db.query(model.Document).filter_by(idDocument=inv_id).update(
            {"documentsubstatusID": sub_status_idt, "documentStatusID": 1}
        )
        db.query(model.DocumentRuleupdates).filter_by(
            documentID=inv_id, type="error"
        ).update({"IsActive": 0})
        db.commit()
        return {"result": "success"}
    except Exception as e:
        logger.error(f"Error in send_to_batch_approval_Admin: {e}")
        return Response(status_code=500)


async def send_to_manual_approval_Admin(u_id: int, inv_id: int, db: Session):
    try:
        sub_status_id = (
            db.query(model.DocumentSubStatus.idDocumentSubstatus)
            .filter(model.DocumentSubStatus.status == "Manual Check Approved")
            .one()
        )

        db.query(model.Document).filter_by(idDocument=inv_id).update(
            {"documentsubstatusID": sub_status_id[0], "documentStatusID": 2}
        )

        db.commit()
        return {"result": "success"}
    except Exception as e:
        logger.error(f"Error in send_to_manual_approval_Admin: {e}")
        return Response(status_code=500)


async def readInvokebatchsummary(u_id: int, db: Session):
    """This function read a service provider account.

    It contains 2 parameter.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        sub_query_ent = (
            db.query(model.UserAccess.EntityID)
            .filter_by(UserID=u_id, isActive=1)
            .distinct()
        )
        sub_status_id = (
            db.query(model.DocumentSubStatus.idDocumentSubstatus)
            .filter(model.DocumentSubStatus.status == "Batch Edit Approved")
            .one()
        )
        status_id = (
            db.query(model.DocumentSubStatus.DocumentstatusID)
            .filter(model.DocumentSubStatus.status == "Batch Edit Approved")
            .one()
        )

        data = (
            db.query(
                model.Document,
                model.DocumentSubStatus,
                model.Rule,
                model.VendorAccount,
                model.Vendor,
            )
            .options(
                Load(model.VendorAccount).load_only("AccountType"),
                Load(model.Vendor).load_only("VendorName"),
            )
            .filter(model.Document.entityID.in_(sub_query_ent))
            .filter(
                model.Document.documentsubstatusID
                == model.DocumentSubStatus.idDocumentSubstatus
            )
            .filter(model.Document.ruleID == model.Rule.idDocumentRules)
            .filter(
                model.Document.vendorAccountID == model.VendorAccount.idVendorAccount
            )
            .filter(model.VendorAccount.vendorID == model.Vendor.idVendor)
            .filter(
                model.Document.documentsubstatusID == sub_status_id[0],
                model.Document.documentStatusID == status_id[0],
            )
            .all()
        )

        return data
    except Exception as e:
        logger.error(f"Error in readInvokebatchsummary: {e}")
        return Response(status_code=500)


async def readfinancialapprovalsummary(u_id: int, db: Session):
    """This function read a service provider account.

    It contains 2 parameter.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        sub_query_ent = (
            db.query(model.UserAccess.EntityID)
            .filter_by(UserID=u_id, isActive=1)
            .distinct()
        )
        # sub_status = (
        #     db.query(model.DocumentSubStatus.idDocumentSubstatus)
        #     .filter(
        #         model.DocumentSubStatus.status.in_(
        #             ["System Check", "Batch Edit Approved", "Manual Check Approved"]
        #         )
        #     )
        #     .distinct()
        # )  # TODO: Unused variable
        approval_type = case(
            [
                (model.DocumentSubStatus.idDocumentSubstatus == 5, "Batch Approval"),
                (model.DocumentSubStatus.idDocumentSubstatus == 27, "Batch Approval"),
                (model.DocumentSubStatus.idDocumentSubstatus == 25, "Manual Approval"),
            ]
        ).label("Approvaltype")
        data1 = (
            db.query(
                model.Document,
                model.Rule,
                model.VendorAccount,
                model.Vendor,
                approval_type,
            )
            .options(
                Load(model.VendorAccount).load_only("AccountType"),
                Load(model.Vendor).load_only("VendorName"),
            )
            .filter(model.Document.entityID.in_(sub_query_ent))
            .filter(model.Document.ruleID == model.Rule.idDocumentRules)
            .filter(
                model.Document.vendorAccountID == model.VendorAccount.idVendorAccount
            )
            .filter(model.VendorAccount.vendorID == model.Vendor.idVendor)
            .filter(model.Document.documentStatusID == 2)
            .all()
        )

        return data1
    except Exception as e:
        logger.error(f"Error in readfinancialapprovalsummary: {e}")
        return Response(status_code=500)


async def test_batchdata(u_id: int, db: Session):
    try:
        data = {
            2738459: {
                "map_item": {
                    "467070": {"invo_itm_code": "1", "fuzz_scr": 100, "item_status": 1},
                    "467071": {"invo_itm_code": "2", "fuzz_scr": 100, "item_status": 1},
                },
                "po_grn_data": {
                    "467070": {
                        "qty": {
                            "po_status": 0,
                            "grn_status": 0,
                            "idDocumentLineItems": 259228,
                            "ck_status": 2,
                        },
                        "unit_price": {
                            "po_status": 0,
                            "idDocumentLineItems": 259233,
                            "ck_status": 1,
                        },
                        "item": {
                            "po_status": 1,
                            "grn_status": 1,
                            "idDocumentLineItems": 259226,
                            "ck_status": 2,
                        },
                    },
                    "467071": {
                        "qty": {
                            "po_status": 0,
                            "grn_status": 0,
                            "idDocumentLineItems": 259236,
                            "ck_status": 2,
                        },
                        "unit_price": {
                            "po_status": 0,
                            "idDocumentLineItems": 259241,
                            "ck_status": 1,
                        },
                        "item": {
                            "po_status": 1,
                            "grn_status": 1,
                            "idDocumentLineItems": 259234,
                            "ck_status": 2,
                        },
                    },
                },
                "inline_rule": 2,
            },
            2738466: {
                "map_item": {
                    "467070": {"invo_itm_code": "1", "fuzz_scr": 100, "item_status": 1},
                    "467071": {"invo_itm_code": "2", "fuzz_scr": 100, "item_status": 1},
                },
                "po_grn_data": {
                    "467070": {
                        "qty": {
                            "po_status": 0,
                            "grn_status": 0,
                            "idDocumentLineItems": 259365,
                            "ck_status": 2,
                        },
                        "unit_price": {
                            "po_status": 0,
                            "idDocumentLineItems": 259370,
                            "ck_status": 1,
                        },
                        "item": {
                            "po_status": 1,
                            "grn_status": 1,
                            "idDocumentLineItems": 259363,
                            "ck_status": 2,
                        },
                    },
                    "467071": {
                        "qty": {
                            "po_status": 0,
                            "grn_status": 0,
                            "idDocumentLineItems": 259373,
                            "ck_status": 2,
                        },
                        "unit_price": {
                            "po_status": 0,
                            "idDocumentLineItems": 259378,
                            "ck_status": 1,
                        },
                        "item": {
                            "po_status": 1,
                            "grn_status": 1,
                            "idDocumentLineItems": 259371,
                            "ck_status": 2,
                        },
                    },
                },
                "inline_rule": 2,
            },
            2738467: {
                "map_item": {
                    "467069": {"invo_itm_code": "1", "fuzz_scr": 75, "item_status": 1}
                },
                "po_grn_data": {
                    "467069": {
                        "qty": {
                            "po_status": 1,
                            "grn_status": 1,
                            "idDocumentLineItems": 259381,
                            "ck_status": 2,
                        },
                        "unit_price": {
                            "po_status": 1,
                            "idDocumentLineItems": 259382,
                            "ck_status": 1,
                        },
                        "item": {
                            "po_status": 1,
                            "grn_status": 1,
                            "idDocumentLineItems": 259379,
                            "ck_status": 2,
                        },
                    }
                },
                "inline_rule": 2,
            },
            2738470: {
                "map_item": {
                    "462120": {"invo_itm_code": "1", "fuzz_scr": 74, "item_status": 1}
                },
                "po_grn_data": {
                    "462120": {
                        "qty": {
                            "po_status": 1,
                            "grn_status": 1,
                            "idDocumentLineItems": 259427,
                            "ck_status": 2,
                        },
                        "unit_price": {
                            "po_status": 1,
                            "idDocumentLineItems": 259428,
                            "ck_status": 1,
                        },
                        "item": {
                            "po_status": 1,
                            "grn_status": 1,
                            "idDocumentLineItems": 259426,
                            "ck_status": 2,
                        },
                    }
                },
                "inline_rule": 2,
            },
            2738564: {
                "map_item": {
                    "415086": {"invo_itm_code": "1", "fuzz_scr": 81, "item_status": 1},
                    "415088": {"invo_itm_code": "3", "fuzz_scr": 90, "item_status": 1},
                    "415087": {"invo_itm_code": "2", "fuzz_scr": 74, "item_status": 1},
                },
                "po_grn_data": {
                    "415086": {
                        "qty": {
                            "po_status": 1,
                            "grn_status": 1,
                            "idDocumentLineItems": 262335,
                            "ck_status": 2,
                        },
                        "unit_price": {
                            "po_status": 1,
                            "idDocumentLineItems": 262336,
                            "ck_status": 1,
                        },
                        "item": {
                            "po_status": 1,
                            "grn_status": 1,
                            "idDocumentLineItems": 262334,
                            "ck_status": 2,
                        },
                    },
                    "415088": {
                        "qty": {
                            "po_status": 1,
                            "grn_status": 1,
                            "idDocumentLineItems": 262345,
                            "ck_status": 2,
                        },
                        "unit_price": {
                            "po_status": 1,
                            "idDocumentLineItems": 262346,
                            "ck_status": 1,
                        },
                        "item": {
                            "po_status": 1,
                            "grn_status": 1,
                            "idDocumentLineItems": 262343,
                            "ck_status": 2,
                        },
                    },
                    "415087": {
                        "qty": {
                            "po_status": 1,
                            "grn_status": 1,
                            "idDocumentLineItems": 262340,
                            "ck_status": 2,
                        },
                        "unit_price": {
                            "po_status": 1,
                            "idDocumentLineItems": 262341,
                            "ck_status": 1,
                        },
                        "item": {
                            "po_status": 1,
                            "grn_status": 1,
                            "idDocumentLineItems": 262338,
                            "ck_status": 2,
                        },
                    },
                },
                "inline_rule": 2,
            },
        }

        for key, value in data.items():
            print(key)
            inv_id = key
            data1 = value.get("po_grn_data", {})
            data2 = value.get("map_item", {})
            if isinstance(data2, dict):
                for key, value in data2.items():
                    print(value["fuzz_scr"])
                    db.query(model.DocumentLineItems).filter_by(
                        documentID=inv_id, itemCode=value["invo_itm_code"]
                    ).update({"invoice_itemcode": key, "Fuzzy_scr": value["fuzz_scr"]})
            if isinstance(data1, dict):
                for key, value in data1.items():
                    print(key)
                    data2 = value
                    if isinstance(data2, dict):
                        for key, value in data2.items():
                            print(key, value)
                            if key != "unit_price":
                                if value["po_status"] == 1 and value["grn_status"] == 1:
                                    error = 0
                                    desc = None
                                elif (
                                    value["po_status"] == 1 and value["grn_status"] == 0
                                ):
                                    error = 1
                                    desc = str(key) + " is not matching with GRN"
                                elif (
                                    value["po_status"] == 0 and value["grn_status"] == 1
                                ):
                                    error = 1
                                    desc = str(key) + " is not matching with PO"
                                elif (
                                    value["po_status"] == 0 and value["grn_status"] == 0
                                ):
                                    error = 1
                                    desc = str(key) + " is not matching with PO and GRN"
                            else:
                                if value["po_status"] == 0:
                                    error = 1
                                    desc = str(key) + " is not matching with PO"
                                else:
                                    error = 0
                                    desc = None

                            db.query(model.DocumentLineItems).filter_by(
                                idDocumentLineItems=value["idDocumentLineItems"]
                            ).update(
                                {
                                    "isError": error,
                                    "CK_status": value["ck_status"],
                                    "ErrorDesc": desc,
                                }
                            )

        db.commit()
        return {"result": "success"}
    except Exception as e:
        logger.error(f"Error in test_batchdata: {e}")
        return Response(status_code=500)


############################################


async def loadpodata(u_id: int, inv_id: int, db: Session):
    invdat = (
        db.query(model.Document)
        .options(
            load_only(
                "docPath",
                "supplierAccountID",
                "vendorAccountID",
                "ruleID",
                "PODocumentID",
            )
        )
        .filter_by(idDocument=inv_id)
        .one()
    )
    all_po = (
        db.query(model.Document)
        .options(load_only("PODocumentID"))
        .filter_by(vendorAccountID=invdat.vendorAccountID, idDocumentType=1)
        .all()
    )
    return all_po


async def readlinedatatest(u_id: int, inv_id: int, db: Session):
    try:
        invdat = (
            db.query(model.Document)
            .options(load_only("docPath", "vendorAccountID"))
            .filter_by(idDocument=inv_id)
            .one()
        )
        vendordata = (
            db.query(model.Vendor, model.VendorAccount)
            .options(
                Load(model.Vendor).load_only(
                    "VendorName",
                    "VendorCode",
                    "Email",
                    "Contact",
                    "TradeLicense",
                    "VATLicense",
                    "TLExpiryDate",
                    "VLExpiryDate",
                    "TRNNumber",
                ),
                Load(model.VendorAccount).load_only("AccountType", "Account"),
            )
            .filter(model.VendorAccount.idVendorAccount == invdat.vendorAccountID)
            .join(
                model.VendorAccount,
                model.VendorAccount.vendorID == model.Vendor.idVendor,
                isouter=True,
            )
            .all()
        )
        invoice_header_data = (
            db.query(model.DocumentData, model.DocumentTagDef, model.DocumentUpdates)
            .options(
                Load(model.DocumentTagDef).load_only("TagLabel"),
                Load(model.DocumentUpdates).load_only("OldValue"),
            )
            .join(
                model.DocumentTagDef,
                model.DocumentData.documentTagDefID
                == model.DocumentTagDef.idDocumentTagDef,
            )
            .join(
                model.DocumentUpdates,
                model.DocumentData.idDocumentData
                == model.DocumentUpdates.documentDataID,
                isouter=True,
            )
            .filter(model.DocumentData.documentID == inv_id)
            .all()
        )
        invoice_lines_data = (
            db.query(
                model.DocumentLineItems,
                model.DocumentLineItemTags,
                model.DocumentUpdates,
            )
            .options(
                Load(model.DocumentLineItemTags).load_only("TagName"),
                Load(model.DocumentUpdates).load_only("OldValue"),
            )
            .join(
                model.DocumentLineItemTags,
                model.DocumentLineItems.lineItemtagID
                == model.DocumentLineItemTags.idDocumentLineItemTags,
            )
            .join(
                model.DocumentUpdates,
                model.DocumentLineItems.idDocumentLineItems
                == model.DocumentUpdates.documentLineItemID,
                isouter=True,
            )
            .filter(model.DocumentLineItems.documentID == inv_id)
            .all()
        )
        invoice_header_cleaned = prepare_inv_header_data(invoice_header_data)
        invoice_line_cleaned = prepare_line_data(invoice_lines_data, po_tag_map)
        return {
            "Vendordata": vendordata,
            "headerdata": invoice_header_cleaned,
            "linedata": invoice_line_cleaned,
        }

    except Exception as e:
        logger.error(f"Error in readlinedatatest: {e}")
        print(traceback.format_exc())
        return Response(status_code=500)


# to get file path


async def readinvoicefilepath(u_id: int, inv_id: int, db: Session):
    try:
        content_type = "application/pdf"
        invdat = (
            db.query(model.Document)
            .options(load_only("docPath", "supplierAccountID", "vendorAccountID"))
            .filter_by(idDocument=inv_id)
            .one()
        )

        # check if file path is present and give base64 coded image url
        if invdat.docPath:
            try:
                cust_id = (
                    db.query(model.User.customerID).filter_by(idUser=u_id).scalar()
                )
                fr_data = (
                    db.query(model.FRConfiguration)
                    .options(
                        load_only(
                            "ConnectionString", "ContainerName", "ServiceContainerName"
                        )
                    )
                    .filter_by(idCustomer=cust_id)
                    .one()
                )

                account_url = (
                    f"https://{settings.storage_account_name}.blob.core.windows.net"
                )
                blob_service_client = BlobServiceClient(
                    account_url=account_url, credential=get_credential()
                )

                if invdat.supplierAccountID:
                    blob_client = blob_service_client.get_blob_client(
                        container=fr_data.ServiceContainerName, blob=invdat.docPath
                    )
                if invdat.vendorAccountID:
                    blob_client = blob_service_client.get_blob_client(
                        container=fr_data.ContainerName, blob=invdat.docPath
                    )

                # invdat.docPath = str(list(blob_client.download_blob().readall()))
                try:
                    filetype = os.path.splitext(invdat.docPath)[1].lower()
                    if filetype == ".png":
                        content_type = "image/png"
                    elif filetype == ".jpg" or filetype == ".jpeg":
                        content_type = "image/jpg"
                    else:
                        content_type = "application/pdf"
                except Exception as e:
                    print(f"Error in getting file type: {e}")
                invdat.docPath = base64.b64encode(blob_client.download_blob().readall())

            except Exception:
                invdat.docPath = ""
        return {"filepath": invdat.docPath, "content_type": content_type}

    except Exception as e:
        logger.error(f"Error in readinvoicefilepath: {e}")
        return Response(status_code=500)


# to update po number
async def update_po_number(inv_id: int, po_num: str, db: Session):
    try:

        db.query(model.Document).filter_by(idDocument=inv_id).update(
            {"PODocumentID": po_num}
        )

        db.commit()
        return {"result": "success"}
    except Exception as e:
        logger.error(f"Error in update_po_number: {e}")
        return Response(status_code=500)


#############################
async def get_all_itemcode(inv_id: int, db: Session):
    try:
        po_number = (
            db.query(model.Document.PODocumentID)
            .filter(model.Document.idDocument == inv_id)
            .one()
        )
        po_doc_id = (
            db.query(model.Document.idDocument)
            .filter(
                model.Document.PODocumentID == po_number[0],
                model.Document.idDocumentType == 1,
            )
            .one()
        )

        all_description = (
            db.query(model.DocumentLineItems)
            .options(load_only("Value", "itemCode"))
            .filter_by(documentID=po_doc_id[0], lineItemtagID=5230)
            .all()
        )
        # all_description = db.query(model.AGIPOLine).options(
        #     load_only("Name","LineNumber")).filter_by(PurchId=po_number[0]).all()

        return {"description": all_description}
    except Exception as e:
        logger.error(f"Error in get_all_itemcode: {e}")
        return Response(status_code=500)


# async def get_all_errortypes(db: Session):
#     try:
#         error_type = db.query(model.BatchErrorType).options(load_only("name")).all()
#         return {"description": error_type}
#     except Exception as e:
#         return Response(status_code=500)


# async def update_line_mapping(
#     inv_id: int,
#     inv_itemcode: str,
#     po_itemcode: str,
#     errotypeid: int,
#     vendoraccountID: int,
#     uid: int,
#     db: Session,
# ):
#     try:
#         present_itemcode = (
#             db.query(model.DocumentLineItems.invoice_itemcode)
#             .filter_by(documentID=inv_id)
#             .distinct()
#             .all()
#         )
#         po_itemcode1 = (po_itemcode,)
#         print("po_itemcode1")
#         # for itemusermap table insert
#         model_id = (
#             db.query(model.Document.documentModelID)
#             .filter_by(idDocument=inv_id)
#             .distinct()
#             .one()
#         )
#         descline_id = (
#             db.query(model.DocumentLineItemTags.idDocumentLineItemTags)
#             .filter_by(idDocumentModel=model_id[0], TagName="Description")
#             .distinct()
#             .one()
#         )
#         mapped_invoitem_description = (
#             db.query(model.DocumentLineItems.Value)
#             .filter_by(
#                 documentID=inv_id, itemCode=inv_itemcode, lineItemtagID=descline_id[0]
#             )
#             .distinct()
#             .one()
#         )
#         if po_itemcode1 in present_itemcode:
#             item_code1 = (
#                 db.query(model.DocumentLineItems.itemCode)
#                 .filter_by(documentID=inv_id, invoice_itemcode=po_itemcode)
#                 .distinct()
#                 .one()
#             )
#             inv_item_code1 = (
#                 db.query(model.DocumentLineItems.invoice_itemcode)
#                 .filter_by(documentID=inv_id, itemCode=inv_itemcode)
#                 .distinct()
#                 .one()
#             )
#             db.query(model.DocumentLineItems).filter_by(
#                 documentID=inv_id, itemCode=inv_itemcode
#             ).update({"invoice_itemcode": po_itemcode, "Fuzzy_scr": 0})

#             db.query(model.ItemUserMap).filter_by(
#                 documentID=inv_id, mappedinvoiceitemcode=inv_itemcode
#             ).delete()
#             db.commit()
#             c4 = model.ItemUserMap(
#                 previousitemmetadataid=None,
#                 itemmetadataid=None,
#                 documentID=inv_id,
#                 vendoraccountID=vendoraccountID,
#                 mappedinvoiceitemcode=inv_itemcode,
#                 mappedinvoitemdescription=mapped_invoitem_description[0],
#                 batcherrortype=errotypeid,
#                 UserID=uid,
#                 createdOn=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
#             )
#             db.add(c4)
#             db.query(model.DocumentLineItems).filter_by(
#                 documentID=inv_id, itemCode=item_code1[0]
#             ).update({"invoice_itemcode": inv_item_code1[0]})

#         else:

#             db.query(model.DocumentLineItems).filter_by(
#                 documentID=inv_id, itemCode=inv_itemcode
#             ).update({"invoice_itemcode": po_itemcode, "Fuzzy_scr": 0})
#             db.query(model.ItemUserMap).filter_by(
#                 documentID=inv_id, mappedinvoiceitemcode=inv_itemcode
#             ).delete()
#             db.commit()
#             c4 = model.ItemUserMap(
#                 previousitemmetadataid=None,
#                 itemmetadataid=None,
#                 documentID=inv_id,
#                 vendoraccountID=vendoraccountID,
#                 mappedinvoiceitemcode=inv_itemcode,
#                 mappedinvoitemdescription=mapped_invoitem_description[0],
#                 batcherrortype=errotypeid,
#                 UserID=uid,
#                 createdOn=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
#             )
#             db.add(c4)
#             print("not present")

#         db.commit()
#         return {"result": "Updated"}
#     except Exception as e:
#         print(traceback.format_exc())
#         if "Incorrect string value" in str(e):
#             return {"result": "Updated"}
#         return Response(status_code=500)


# async def get_current_itemmapped(inv_id: int, db: Session):
#     try:
#         po_num = (
#             db.query(model.Document.PODocumentID)
#             .filter(model.Document.idDocument == inv_id)
#             .scalar()
#         )
#         po_id = (
#             db.query(model.Document.idDocument)
#             .filter(
#                 model.Document.PODocumentID == po_num,
#                 model.Document.idDocumentType == 1,
#             )
#             .scalar()
#         )
#         pomodel = (
#             db.query(model.DocumentModel.idDocumentModel)
#             .filter(model.DocumentModel.modelID == "POMid000909")
#             .scalar()
#         )
#         DescriptionTag = (
#             db.query(model.DocumentLineItemTags.idDocumentLineItemTags)
#             .filter(
#                 model.DocumentLineItemTags.TagName == "Name",
#                 model.DocumentLineItemTags.idDocumentModel == pomodel,
#             )
#             .scalar()
#         )
#         item_desc1 = (
#             db.query(model.ItemUserMap)
#             .options(Load(model.ItemUserMap).load_only("mappedinvoiceitemcode"))
#             .filter(model.ItemUserMap.documentID == inv_id)
#             .all()
#         )
#         item_mapping = []
#         for item in item_desc1:
#             obj = {}
#             po_itemcode = (
#                 db.query(model.DocumentLineItems.invoice_itemcode)
#                 .filter_by(documentID=inv_id, itemCode=item.mappedinvoiceitemcode)
#                 .first()
#             )
#             if po_itemcode:
#                 po_line_val = (
#                     db.query(model.DocumentLineItems.Value)
#                     .filter_by(
#                         documentID=po_id,
#                         itemCode=po_itemcode[0],
#                         lineItemtagID=DescriptionTag,
#                     )
#                     .scalar()
#                 )
#                 obj = {
#                     "ItemMetaData": {"description": po_line_val},
#                     "ItemUserMap": {
#                         "mappedinvoiceitemcode": item.mappedinvoiceitemcode
#                     },
#                 }
#                 item_mapping.append(obj)

#         return {"description": item_mapping}
#     except Exception as e:
#         return Response(status_code=500)


# Function to fetch the po lines based on the invoice id provided
async def get_po_line_items(u_id, inv_id, po_number, db):
    try:
        if inv_id is not None:
            po_num = (
                db.query(model.Document.PODocumentID)
                .filter(model.Document.idDocument == inv_id)
                .scalar()
            )
            po_id = (
                db.query(model.Document.idDocument)
                .filter(
                    model.Document.PODocumentID == po_num,
                    model.Document.idDocumentType == 1,
                )
                .scalar()
            )
        else:
            po_id = (
                db.query(model.Document.idDocument)
                .filter(
                    model.Document.PODocumentID == po_number,
                    model.Document.idDocumentType == 1,
                )
                .scalar()
            )
        doc_model_id = (
            db.query(model.Document.documentModelID)
            .filter(model.Document.idDocument == po_id)
            .scalar()
        )
        fields = [
            "LineNumber",
            "ItemId",
            "Name",
            "ProcurementCategory",
            "PurchQty",
            "UnitPrice",
            "DiscAmount",
            "DiscPercent",
            "RemainPurchFinancial",
        ]
        tags_ids = (
            db.query(model.DocumentLineItemTags)
            .options(load_only("idDocumentLineItemTags", "TagName"))
            .filter(
                model.DocumentLineItemTags.TagName.in_(fields),
                model.DocumentLineItemTags.idDocumentModel == doc_model_id,
            )
            .all()
        )
        tags = {}
        for tag in tags_ids:
            tags[tag.idDocumentLineItemTags] = tag.TagName
        dist_item_code = (
            db.query(model.DocumentLineItems.itemCode)
            .filter(model.DocumentLineItems.documentID == po_id)
            .distinct()
        )
        po_lines_dict = []
        for item_code in dist_item_code:
            data = {}
            po_lines = (
                db.query(model.DocumentLineItems)
                .options(load_only("Value", "lineItemtagID"))
                .filter(
                    model.DocumentLineItems.documentID == po_id,
                    model.DocumentLineItems.itemCode == item_code[0],
                )
                .filter(model.DocumentLineItems.lineItemtagID.in_(list(tags.keys())))
                .all()
            )
            for row in po_lines:
                if row.lineItemtagID in tags.keys():
                    data[tags[row.lineItemtagID]] = row.Value
            po_lines_dict.append(data)
        return {"Po_line_details": po_lines_dict}
    except SQLAlchemyError as e:
        logger.error(e)
        traceback.print_exc()
        return Response(status_code=500)
    finally:
        db.close()


# func to get po total
def get_po_total(po_lines):
    po_tot_amt = 0
    try:
        # calculating the total of selected po line
        for line in po_lines:
            if line.Quantity <= line.PurchQty:
                if line.DiscAmount != 0:
                    unitprice = abs(line.UnitPrice - line.DiscAmount)
                elif line.DiscPercent != 0:
                    unitprice = abs(
                        line.UnitPrice - ((line.DiscPercent * line.UnitPrice) / 100)
                    )
                else:
                    unitprice = line.UnitPrice
                print("unit price", unitprice)
                po_tot_amt = po_tot_amt + (line.Quantity * unitprice)
                print("po total", po_tot_amt)
            else:
                po_tot_amt = 0
                return {
                    "po_tot_amt": po_tot_amt,
                    "status": f"Quantity {line.Quantity} greater "
                    + f"than PurchQty {line.PurchQty}for the line {line.LineNumber}",
                }
        return {"po_tot_amt": po_tot_amt, "status": "success"}
    except Exception:
        logger.error(traceback.format_exc())
        po_tot_amt = 0
        return po_tot_amt


# check tags availability for inserting new data
def chk_tag_availability(fields, id_doc_modelid, db):
    try:
        tags_ids = (
            db.query(model.DocumentLineItemTags)
            .options(load_only("TagName", "idDocumentLineItemTags"))
            .filter(model.DocumentLineItemTags.idDocumentModel == id_doc_modelid)
            .filter(model.DocumentLineItemTags.TagName.in_(fields))
            .all()
        )
        tags = {}
        for tag in tags_ids:
            tags[tag.TagName] = tag.idDocumentLineItemTags
        avail_tags_len = 0
        for field in fields:
            if field in tags.keys():
                avail_tags_len = avail_tags_len + 1
        return avail_tags_len, tags
    except Exception:
        logger.error(traceback.format_exc())
        return None


# function to save po lines to invoice lines
async def save_po_lines_to_invoice(u_id, inv_id, po_lines, db):
    global resp
    try:
        po_tot_amt = 0

        # Getting invoice total
        inv_subtotal = get_invoiceTotal(inv_id, db)
        print("invtot", inv_subtotal)
        if inv_subtotal != "":
            inv_subtotal = float(inv_subtotal)
        else:
            error_msg = inv_subtotal
            inv_subtotal = 0

        # calculating the total of selected po line
        data = get_po_total(po_lines)
        po_tot_amt = data["po_tot_amt"]

        # converitng all totals to float values for comparison
        inv_subtotal = float(inv_subtotal)
        po_tot_amt = float(po_tot_amt)

        print(inv_subtotal, po_tot_amt)

        # Getting doc id from invoice
        id_doc_modelid = (
            db.query(model.Document)
            .options(load_only("documentModelID", "PODocumentID"))
            .filter(model.Document.idDocument == inv_id)
            .scalar()
        )

        # Calculating the invoice and Po total difference percentage
        inv_diff_per = abs(((po_tot_amt - inv_subtotal) * 100) / po_tot_amt)

        # if There is 2 percent less saving po line total as Invoice total
        if inv_diff_per != 0 and inv_diff_per < 2:
            sub_tg_id = (
                db.query(model.DocumentTagDef.idDocumentTagDef)
                .filter(
                    model.DocumentTagDef.TagLabel == "SubTotal",
                    model.DocumentTagDef.idDocumentModel
                    == id_doc_modelid.documentModelID,
                )
                .scalar()
            )
            db.query(model.DocumentData).filter(
                model.DocumentData.documentTagDefID == sub_tg_id,
                model.DocumentData.documentID == inv_id,
            ).update({"Value": po_tot_amt})
            db.commit()

        # Checking the if flip po possible
        if inv_diff_per < 2 and data["status"] == "success":
            print("invoice total has difference less than 1 value")
            resp = flip_po_to_invoice(
                po_lines, inv_id, db, u_id, id_doc_modelid.documentModelID
            )
        else:
            print("invoice total do not match with po")
            error_msg = "invoice total does not match with Po"
            return {"error": error_msg}

        # If flip po success storing the doc history
        if resp["result"] == "success":
            doc_status = (
                db.query(model.Document)
                .options(load_only("documentStatusID", "documentsubstatusID"))
                .filter_by(idDocument=inv_id)
                .scalar()
            )
            created_on = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            doc_history = {
                "documentID": inv_id,
                "userID": u_id,
                "CreatedOn": created_on,
            }
            doc_history["documentdescription"] = "Po Flip Successfull"
            doc_history["documentStatusID"] = doc_status.documentStatusID
            doc_history["documentSubStatusID"] = doc_status.documentsubstatusID
            doc_history = model.DocumentHistoryLogs(**doc_history)
            db.add(doc_history)
            db.commit()
            return {"result": "success"}
        else:
            return {"result": "Unsuccessfull"}
    except SQLAlchemyError as e:
        error_msg = e
        traceback.print_exc()
        return Response(status_code=500)
    finally:
        db.close()


# Getting invoice total for the invoice.


def get_invoiceTotal(inv_id, db):
    try:
        id_doc_modelid = (
            db.query(model.Document)
            .options(load_only("documentModelID", "PODocumentID"))
            .filter(model.Document.idDocument == inv_id)
            .scalar()
        )
        tag_id = (
            db.query(model.DocumentTagDef.idDocumentTagDef)
            .filter(
                model.DocumentTagDef.TagLabel == "SubTotal",
                model.DocumentTagDef.idDocumentModel == id_doc_modelid.documentModelID,
            )
            .scalar()
        )
        inv_subtotal = (
            db.query(model.DocumentData.Value)
            .filter(model.DocumentData.documentID == inv_id)
            .filter(model.DocumentData.documentTagDefID == tag_id)
            .scalar()
        )
        if inv_subtotal is None:
            inv_total = float(
                db.query(model.Document.totalAmount)
                .filter(model.Document.idDocument == inv_id)
                .scalar()
            )
            tag_id = (
                db.query(model.DocumentTagDef.idDocumentTagDef)
                .filter(
                    model.DocumentTagDef.TagLabel == "TotalTax",
                    model.DocumentTagDef.idDocumentModel
                    == id_doc_modelid.documentModelID,
                )
                .scalar()
            )
            if tag_id is not None:
                inv_tax = float(
                    db.query(model.DocumentData.Value)
                    .filter(model.DocumentData.documentID == inv_id)
                    .filter(model.DocumentData.documentTagDefID == tag_id)
                    .scalar()
                )
            else:
                inv_tax = 0
            inv_subtotal = inv_total - inv_tax
        return inv_subtotal
    except SQLAlchemyError as e:
        logger.error(e)
        traceback.print_exc()
        print("error fetching invoice total")
        return 0
    finally:
        db.close()


def flip_po_to_invoice(po_lines, inv_id, db, u_id, id_doc_modelid):
    global error_msg
    try:
        if len(po_lines) > 0:
            # checking for feilds availability
            fields = ["Description", "AmountExcTax", "Quantity", "UnitPrice"]
            avail_tags_len, tags = chk_tag_availability(fields, id_doc_modelid, db)
            if avail_tags_len == len(fields):
                try:
                    # lines deletion for inserting selected po lines
                    line_ids = (
                        db.query(model.DocumentLineItems.idDocumentLineItems)
                        .filter(model.DocumentLineItems.documentID == inv_id)
                        .all()
                    )
                    if line_ids is not None:
                        try:
                            for id in line_ids:
                                db.query(model.DocumentUpdates).filter(
                                    model.DocumentUpdates.documentLineItemID == id[0]
                                ).delete()
                                db.commit()
                            db.query(model.DocumentLineItems).filter(
                                model.DocumentLineItems.documentID == inv_id
                            ).delete()
                            db.commit()
                        except Exception:
                            logger.error(traceback.format_exc())

                    # calculating discount from po line
                    # and preparing data for insertion to db
                    inv_line_number = 0
                    for line in po_lines:
                        inv_line_number += 1
                        if line.DiscAmount != 0:
                            tag_id = (
                                db.query(
                                    model.DocumentLineItemTags.idDocumentLineItemTags
                                )
                                .filter(
                                    model.DocumentLineItemTags.TagName == "Discount",
                                    model.DocumentLineItemTags.idDocumentModel
                                    == id_doc_modelid,
                                )
                                .scalar()
                            )
                            unitprice = abs(line.UnitPrice - line.DiscAmount)
                            if tag_id is None:
                                to_insert = {
                                    "idDocumentModel": id_doc_modelid,
                                    "TagName": "Discount",
                                }
                                db.add(model.DocumentLineItemTags(**to_insert))
                                db.commit()
                            data = {
                                "Description": line.Name,
                                "UnitPrice": line.UnitPrice,
                                "Quantity": line.Quantity,
                                "AmountExcTax": round(line.Quantity * unitprice, 2),
                                "Discount": line.DiscAmount,
                            }
                        elif line.DiscPercent != 0:
                            unitprice = abs(
                                line.UnitPrice
                                - ((line.DiscPercent * line.UnitPrice) / 100)
                            )
                            if tag_id is None:
                                to_insert = {
                                    "idDocumentModel": id_doc_modelid,
                                    "TagName": "DiscPercent",
                                }
                                db.add(model.DocumentLineItemTags(**to_insert))
                                db.commit()
                            data = {
                                "Description": line.Name,
                                "UnitPrice": line.UnitPrice,
                                "Quantity": line.Quantity,
                                "AmountExcTax": round(line.Quantity * unitprice, 2),
                                "DiscPercent": line.DiscPercent,
                            }
                        else:
                            unitprice = line.UnitPrice
                            data = {
                                "Description": line.Name,
                                "UnitPrice": line.UnitPrice,
                                "Quantity": line.Quantity,
                                "AmountExcTax": round(line.Quantity * unitprice, 2),
                            }
                        # Getting the tags and tags id for inserting data
                        ln, tags = chk_tag_availability(data.keys(), id_doc_modelid, db)

                        # Calling add line data function to add line by line
                        save_resp = addLineData(
                            data, inv_line_number, db, inv_id, tags, line.LineNumber
                        )
                        if save_resp["result"] != "success":
                            error_msg = "error in saving the po lines"
                            return {"result": error_msg}
                    return {"result": "Success"}
                except Exception:
                    logger.error(traceback.format_exc())
                    return {"result": "Unsuccessfull"}
            else:
                return {"result": "one or more tags missing"}
        else:
            return {"result": "No Po lines selected for PO Flip"}
    except SQLAlchemyError as e:
        logger.error(e)
        return Response(status_code=500)
    finally:
        db.close()


def addLineData(line, itemcode, db, inv_id, tags, po_line_num):
    try:
        created_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        db_data = {}
        for key in line.keys():
            db_data["documentID"] = inv_id
            db_data["lineItemtagID"] = tags[key]
            db_data["Value"] = line[key]
            db_data["itemCode"] = itemcode
            db_data["invoice_itemcode"] = po_line_num
            db_data["IsUpdated"] = 0
            db_data["isError"] = 0
            db_data["UpdatedDate"] = created_date
            db_data["CreatedDate"] = created_date
            try:
                db.add(model.DocumentLineItems(**db_data))
                db.commit()
            except Exception:
                logger.error(traceback.format_exc())
                error_msg = "Failed to save line"
                return {"Status": error_msg}
        return {"result": "success"}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def check_inv_status(u_id, inv_id, db):
    try:
        status_id = (
            db.query(model.Document)
            .options(load_only("documentStatusID", "documentsubstatusID"))
            .filter(model.Document.idDocument == inv_id)
            .scalar()
        )

        status_id = {
            "status": status_id.documentStatusID,
            "substatus": status_id.documentsubstatusID,
        }
        return {"result": status_id}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


# prepare line details to compare
def prepare_line_data(line_data, po_tag_map):
    key_order = [
        "Description",
        "Quantity",
        "UnitPrice",
        "Discount",
        "DiscPercent",
        "AmountExcTax",
    ]
    numberic_labels = [
        "InvoiceTotal",
        "SubTotal",
        "TotalTax",
        "Amount",
        "UnitPrice",
        "Quantity",
        "AmountExcTax",
        "Tax",
        "DiscAmount",
        "DiscPercent",
        "Discount",
    ]
    line_data.sort(
        key=lambda x: x.DocumentLineItems.itemCode
    )  # Sort the data based on itemCode
    line_cleaned = []
    for key, group in groupby(line_data, key=lambda x: x.DocumentLineItems.itemCode):
        obj = {}
        for line in group:
            tag_name = line.DocumentLineItemTags.TagName
            if tag_name in po_tag_map.keys():
                # If it exists, use the mapped key from the po_tag_map
                obj[po_tag_map[tag_name]] = {
                    "idDocumentLineItems": line.DocumentLineItems.idDocumentLineItems,
                    "Value": (
                        cln_amt(line.DocumentLineItems.Value)
                        if po_tag_map[tag_name] in numberic_labels
                        else line.DocumentLineItems.Value
                    ),
                    "itemCode": key,
                    "invoice_itemcode": line.DocumentLineItems.invoice_itemcode,
                    "isError": line.DocumentLineItems.isError,
                    "isUpdated": line.DocumentLineItems.IsUpdated,
                    "ErrorDesc": line.DocumentLineItems.ErrorDesc,
                    "DocumentUpdates": line.DocumentUpdates,
                }
            else:
                # If it doesn't exist in po_tag_map, use the original key
                obj[tag_name] = {
                    "idDocumentLineItems": line.DocumentLineItems.idDocumentLineItems,
                    "Value": (
                        cln_amt(line.DocumentLineItems.Value)
                        if tag_name in numberic_labels
                        else line.DocumentLineItems.Value
                    ),
                    "itemCode": key,
                    "invoice_itemcode": line.DocumentLineItems.invoice_itemcode,
                    "isError": line.DocumentLineItems.isError,
                    "isUpdated": line.DocumentLineItems.IsUpdated,
                    "ErrorDesc": line.DocumentLineItems.ErrorDesc,
                    "DocumentUpdates": line.DocumentUpdates,
                }

        if len(obj.keys()) > 0:
            ordered_obj = {k: obj[k] for k in key_order if k in obj}
            line_cleaned.append(ordered_obj)

    return line_cleaned


# prepare invoice header to compare
def prepare_inv_header_data(header_data):
    invoice_header_cleaned = {}
    for invoice_header in header_data:
        invoice_header_cleaned[invoice_header.DocumentTagDef.TagLabel] = {
            "Value": invoice_header.DocumentData.Value,
            "idDocumentData": invoice_header.DocumentData.idDocumentData,
            "isError": invoice_header.DocumentData.isError,
            "isUpdated": invoice_header.DocumentData.IsUpdated,
            "ErrorDesc": invoice_header.DocumentData.ErrorDesc,
            "DocumentUpdates": invoice_header.DocumentUpdates,
        }
    return invoice_header_cleaned


# clean amount function
def cln_amt(amt):
    amt = str(amt)
    if len(amt) > 0:
        if len(re.findall(r"\d+\,\d+\d+\.\d+", amt)) > 0:
            cl_amt = re.findall(r"\d+\,\d+\d+\.\d+", amt)[0]
            cl_amt = float(cl_amt.replace(",", ""))
        elif len(re.findall(r"\d+\.\d+", amt)) > 0:
            cl_amt = re.findall(r"\d+\.\d+", amt)[0]
            cl_amt = float(cl_amt)
        elif len(re.findall(r"\d+", amt)) > 0:
            cl_amt = re.findall(r"\d+", amt)[0]
            cl_amt = float(cl_amt)
        else:
            cl_amt = amt
    else:
        cl_amt = amt
    return cl_amt

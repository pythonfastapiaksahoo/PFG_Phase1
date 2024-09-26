import io
import os
import sys
import traceback
from datetime import datetime

import model
import pandas as pd
import pytz as tz
from fastapi.responses import Response
from fuzzywuzzy import process
from logger_module import logger
from sqlalchemy import and_, case, exc, func, or_
from sqlalchemy.orm import Load, Session, load_only

sys.path.append("..")

tz_region_name = os.getenv("serina_tz", "Asia/Dubai")
tz_region = tz.timezone(tz_region_name)


async def UpdateVendorERP(VendorUserID, VendorID, UpdateVendor, db):
    """This function updates a Vendor account loaded from ERP.

    It contains 4 parameters.
    :param VendorUserID: It is a function parameters that is of integer
        type, it provides the vendor user Id.
    :param VendorID: It is a function parameters that is of integer
        type, it provides the vendor Id.
    :param UpdateVendor: It is function parameter that is of a Pydantic
        class object, It takes member data for updating of Vendor.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        UpdateVendor = dict(UpdateVendor)
        # have to make it dynamic to void duplicate error
        UpdateVendor["createdBy"] = VendorUserID
        UpdateVendor["UpdatedOn"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        # UpdateVendorDb = model.Vendor(**UpdateVendor)
        for item_key in UpdateVendor.copy():
            # pop out elements that are not having any value
            if not UpdateVendor[item_key]:
                UpdateVendor.pop(item_key)
        db.query(model.Vendor).filter(model.Vendor.idVendor == VendorID).update(
            UpdateVendor
        )
        db.commit()
        return {"result": "updated", "record": UpdateVendor}
    except Exception as e:
        return Response(status_code=500, headers={"Error": "Server error"})
    finally:
        db.close()


async def UpdateVendorAccERP(VendorUserID, VendorAccID, UpdateVendorAcc, db):
    """This function updates a Vendor account.

    It contains 4 parameters.
    :param VendorUserID: It is a function parameters that is of integer
        type, it provides the vendor user Id.
    :param VendorAccID: It is a function parameters that is of integer
        type, it provides the vendor account Id.
    :param UpdateVendorAcc: It is function parameter that is of a
        Pydantic class object, It takes member data for updating of
        Vendor account.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        UpdateVendorAcc = dict(UpdateVendorAcc)
        # iterating through a copy of dict if not error will produce
        for item_key in UpdateVendorAcc.copy():
            if not UpdateVendorAcc[item_key]:
                # pop out elements that are not having any value
                UpdateVendorAcc.pop(item_key)
        UpdateVendorAcc["UpdatedOn"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        db.query(model.VendorAccount).filter(
            model.VendorAccount.idVendorAccount == VendorAccID
        ).update(UpdateVendorAcc)
        db.commit()
        return {"result": "updated", "record": UpdateVendorAcc}
    except Exception as e:
        return Response(
            status_code=500,
            headers={"Error": "Server error", "Desc": "Invalid vendor ID"},
        )
    finally:
        db.close()


async def NewVendor(VendorUserID, NewVendor, db):
    """This function creates a new Vendor.

    It contains 3 parameters.
    :param VendorUserID: It is a function parameters that is of integer
        type, it provides the vendor user Id.
    :param NewVendor: It is function parameter that is of a Pydantic
        class object, It takes member data for creation of new Vendor.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        NewVendor = dict(NewVendor)
        # have to make it dynamic to void duplicate error
        NewVendor["createdBy"] = VendorUserID
        NewVendor["CreatedOn"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        NewVendor["UpdatedOn"] = NewVendor["CreatedOn"]
        NewVendorDb = model.Vendor(**NewVendor)
        db.add(NewVendorDb)
        db.commit()
        res = db.execute(
            "select idVendor from vendor where idvendor = (SELECT LAST_INSERT_ID())"
        )
        result = res.first()
        if result is not None:
            idVendor = result[0]
        else:
            idVendor = 0
        return {"result": "created", "record": NewVendor, "idVendor": idVendor}
    except Exception as e:
        return Response(status_code=500, headers={"Error": "Server error"})
    finally:
        db.close()


async def NewVendorAcc(VendorUserID, VendorID, NewVendorAcc, db):
    """This function creates a new Vendor account.

    It contains 4 parameters.
    :param VendorUserID: It is a function parameters that is of integer
        type, it provides the vendor user Id.
    :param VendorID: It is a function parameters that is of integer
        type, it provides the vendor Id.
    :param NewVendorAcc: It is function parameter that is of a Pydantic
        class object, It takes member data for creation of new Vendor
        Account.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        NewVendorAcc = dict(NewVendorAcc)
        # have to make it dynamic to void duplicate error
        # have to check if corresponds to the user id
        NewVendorAcc["vendorID"] = VendorID
        NewVendorAcc["CreatedOn"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        NewVendorAcc["UpdatedOn"] = NewVendorAcc["CreatedOn"]
        # supplying dict as a keyword args to the Vendor Account Model
        NewVendorAccDb = model.VendorAccount(**NewVendorAcc)
        db.add(NewVendorAccDb)
        db.commit()
        return {"result": "created", "record": NewVendorAcc}
    except Exception as e:
        return Response(status_code=500, headers={"Error": "Server error"})
    finally:
        db.close()


async def NewVendorUser(u_id, ven_user, ven_user_type, db):
    """This function creates a new Vendor user.

    It contains 4 parameters.
    :param VendorUserID: It is a function parameters that is of integer
        type, it provides the vendor user Id.
    :param VendorID: It is a path parameters that is of integer type, it
        provides the vendor Id.
    :param NewVendorUser: It is Body parameter that is of a Pydantic
        class object, It takes member data for creation of new Vendor
        User.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        # permission check function has to be created and checked
        createdon = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        # pop the user access
        n_usr = ven_user.dict()
        n_usr["created_by"] = u_id
        user_access = n_usr.pop("uservendoraccess")
        role_id = n_usr.pop("role_id")
        # make check point for max amount and role later
        # max_amount = n_usr.pop("max_amount")
        # get company id
        c_id = db.query(model.User.customerID).filter_by(idUser=u_id).scalar()
        n_usr = model.User(**n_usr, customerID=c_id, CreatedOn=createdon)
        db.add(n_usr)
        db.flush()
        # get the vendor id from admin user id
        vendor_admin_access = (
            db.query(model.VendorUserAccess)
            .filter_by(vendorUserID=u_id, isActive=1)
            .all()
        )
        # common info
        basedata = {
            "vendorUserID": n_usr.idUser,
            "CreatedBy": u_id,
            "CreatedOn": createdon,
            "UpdatedOn": createdon,
        }
        # saving the access permission
        user_access = []
        for row in vendor_admin_access:
            basedata["vendorID"] = row.vendorID
            basedata["vendorAccountID"] = row.vendorAccountID
            basedata["entityID"] = row.entityID
            user_access.append(basedata.copy())
        user_access = [model.VendorUserAccess(**row) for row in user_access]
        db.add_all(user_access)
        # notifying changes to the db but not committing since next stage it
        # will be committed
        db.flush()
        db.add(
            model.AccessPermission(
                permissionDefID=role_id, userID=n_usr.idUser, CreatedOn=createdon
            )
        )
        db.flush()
        # returning the customer details with custom function from model to
        # avoid all columns
        return n_usr.datadict(), ven_user_type
    except exc.IntegrityError as e:
        print(traceback.print_exc())
        db.rollback()
        return Response(
            status_code=400,
            headers={"ClientError": "one or more values does not exist"},
        )
    except Exception as e:
        print(traceback.print_exc())
        db.rollback()
        return Response(
            status_code=500, headers={"Error": f"{traceback.format_exc()}Server error"}
        )
    finally:
        db.close()


def NewVendorUserInvoiceAccess(VendorUserID, NewVendorInvAccs, db):
    """This function creates a new Vendor invoice access.

    It contains 3 parameters.
    :param VendorUserID: It is a path parameters that is of integer
        type, it provides the vendor user Id.
    :param NewVendorInvAccs: It is Body parameter that is of a Pydantic
        class object, It takes member data for creation of new Vendor
        Invoce Access.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        NewVendorInvAccs = dict(NewVendorInvAccs)
        # have to check if corresponds to the user id
        NewVendorInvAccs["vendorUserID"] = VendorUserID
        NewVendorInvAccs["CreatedOn"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        NewVendorInvAccs["UpdatedOn"] = NewVendorInvAccs["CreatedOn"]
        # supplying dict as a keyword args to the Vendor Invoice Access Model
        NewVendorInvAccsdb = model.VendorInvoiceAccess(**NewVendorInvAccs)
        db.add(NewVendorInvAccsdb)
        db.commit()
        return NewVendorInvAccs
    except Exception as e:
        return Response(status_code=500, headers={"Error": "Server error"})
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
        return db.query(model.Vendor).all()
    except Exception as e:
        return Response(
            status_code=500, headers={"Error": "Server error", "Desc": "Invalid result"}
        )
    finally:
        db.close()


async def readvendorbyuid(u_id, vendor_type, db, off_limit, api_filter):
    """Reads a list of Vendors based on the provided user ID and filtering
    criteria.

    :param u_id: The user ID for which to fetch vendor data.
    :param vendor_type: The type of vendor (e.g., 'PO_Based' or
        'NON_PO_Based').
    :param db: The database session object.
    :param off_limit: A tuple containing (offset, limit) for pagination.
    :param api_filter: A dictionary containing additional filters.
    :return: A list of dictionaries representing the vendor data.
    """
    try:
        # Case statement for onboarding status
        onboarding_status = case(
            (model.DocumentModel.modelStatus.in_((4, 5)), "Onboarded"),
            (model.DocumentModel.modelStatus.in_((2, 3)), "In-Progress"),
            (model.DocumentModel.idDocumentModel.is_(None), "Not-Onboarded"),
            else_="Not-Onboarded",
        ).label("OnboardedStatus")

        # Subquery to get the min priority status for each vendor
        subquery = (
            db.query(
                model.Vendor.idVendor,
                func.min(
                    case(
                        (model.DocumentModel.modelStatus.in_((4, 5)), 1),
                        (model.DocumentModel.modelStatus.in_((2, 3)), 2),
                        (model.DocumentModel.idDocumentModel.is_(None), 3),
                        else_=3,
                    )
                ).label("min_status_priority"),
            )
            .join(
                model.VendorAccount,
                model.Vendor.idVendor == model.VendorAccount.vendorID,
            )
            .outerjoin(
                model.DocumentModel,
                model.DocumentModel.idVendorAccount
                == model.VendorAccount.idVendorAccount,
            )
            .group_by(model.Vendor.idVendor)
            .subquery()
        )

        # Main query
        data = (
            db.query(
                model.Vendor,
                model.Entity,
                model.VendorAccount.idVendorAccount,
                model.DocumentModel.idVendorAccount,
                onboarding_status,
            )
            .options(
                Load(model.Vendor).load_only(
                    "VendorName", "VendorCode", "Email", "Contact", "TRNNumber"
                ),
                Load(model.Entity).load_only("EntityName"),
            )
            .join(model.Entity, model.Vendor.entityID == model.Entity.idEntity)
            .join(
                model.VendorAccount,
                model.Vendor.idVendor == model.VendorAccount.vendorID,
            )
            .outerjoin(
                model.DocumentModel,
                model.DocumentModel.idVendorAccount
                == model.VendorAccount.idVendorAccount,
            )
            .join(
                subquery,
                and_(
                    model.Vendor.idVendor == subquery.c.idVendor,
                    case(
                        (model.DocumentModel.modelStatus.in_((4, 5)), 1),
                        (model.DocumentModel.modelStatus.in_((2, 3)), 2),
                        (model.DocumentModel.idDocumentModel.is_(None), 3),
                        else_=3,
                    )
                    == subquery.c.min_status_priority,
                ),
            )
        )

        # Apply additional filters
        for key, val in api_filter.items():
            if key == "ent_id" and val:
                data = data.filter(model.Entity.idEntity == val)
            if key == "ven_code" and val:
                data = data.filter(
                    or_(
                        model.Vendor.VendorName.ilike(f"%{val}%"),
                        model.Vendor.VendorCode.ilike(f"%{val}%"),
                    )
                )
            if key == "onb_status" and val:
                if val == "In-Progress":
                    data = data.filter(model.DocumentModel.modelStatus.in_((2, 3)))
                elif val == "Onboarded":
                    data = data.filter(model.DocumentModel.modelStatus.in_((4, 5)))
                elif val == "Not-Onboarded":
                    data = data.filter(
                        (
                            model.DocumentModel.idDocumentModel.is_(None)
                            | model.DocumentModel.modelStatus.not_in((2, 3, 4, 5))
                        )
                    )

        # Apply filters for vendor type
        if vendor_type:
            if vendor_type == "PO_Based":
                md_ids = db.query(model.FRMetaData.idInvoiceModel).filter_by(
                    vendorType="PO based"
                )
                ven_acc_ids = db.query(model.VendorAccount.vendorID).filter(
                    model.DocumentModel.idDocumentModel.in_(md_ids)
                )
                data = data.filter(model.Vendor.idVendor.in_(ven_acc_ids))
            elif vendor_type == "NON_PO_Based":
                md_ids = db.query(model.FRMetaData.idInvoiceModel).filter_by(
                    vendorType="Non-PO based"
                )
                ven_acc_ids = db.query(model.VendorAccount.vendorID).filter(
                    model.DocumentModel.idDocumentModel.in_(md_ids)
                )
                data = data.filter(model.Vendor.idVendor.in_(ven_acc_ids))

        # Pagination
        offset, limit = off_limit
        off_val = (offset - 1) * limit
        if off_val < 0:
            return Response(
                status_code=403,
                headers={"ClientError": "Please provide a valid offset value."},
            )

        # Execute query and apply pagination after filtering
        data = data.distinct().limit(limit).offset(off_val).all()

        # Prepare result
        result = []
        for row in data:
            row_dict = {}
            for idx, col in enumerate(row):
                if isinstance(col, model.Vendor):
                    row_dict["Vendor"] = {
                        "idVendor": col.idVendor,
                        "VendorName": col.VendorName,
                        "VendorCode": col.VendorCode,
                        "Email": col.Email,
                        "Contact": col.Contact,
                    }
                elif isinstance(col, model.Entity):
                    row_dict["Entity"] = {
                        "EntityName": col.EntityName,
                        "idEntity": col.idEntity,
                    }
                elif isinstance(col, int):
                    row_dict["idVendorAccount"] = col
                elif isinstance(col, str):
                    row_dict["OnboardedStatus"] = col
                elif col is None:
                    row_dict[f"col{idx}"] = None
            result.append(row_dict)

        return result
    except Exception as e:
        print(traceback.format_exc())
        return Response(
            status_code=500, headers={"Error": "Server error", "Desc": "Invalid result"}
        )
    finally:
        db.close()


async def readpaginatedvendorlist(
    u_id, vendor_type, db, off_limit, api_filter, ven_status
):
    """Reads a list of Vendors based on the provided user ID and filtering
    criteria.

    :param u_id: The user ID for which to fetch vendor data.
    :param vendor_type: The type of vendor (e.g., 'PO_Based' or
        'NON_PO_Based').
    :param db: The database session object.
    :param off_limit: A tuple containing (offset, limit) for pagination.
    :param api_filter: A dictionary containing additional filters.
    :return: A list of dictionaries representing the vendor data.
    """
    try:
        # Case statement for onboarding status
        onboarding_status = case(
            (model.DocumentModel.modelStatus.in_((4, 5)), "Onboarded"),
            (model.DocumentModel.modelStatus.in_((2, 3)), "In-Progress"),
            (model.DocumentModel.idDocumentModel.is_(None), "Not-Onboarded"),
            else_="Not-Onboarded",
        ).label("OnboardedStatus")

        # Subquery to get the min priority status for each vendor
        subquery = (
            db.query(
                model.Vendor.idVendor,
                func.min(
                    case(
                        (model.DocumentModel.modelStatus.in_((4, 5)), 1),
                        (model.DocumentModel.modelStatus.in_((2, 3)), 2),
                        (model.DocumentModel.idDocumentModel.is_(None), 3),
                        else_=3,
                    )
                ).label("min_status_priority"),
            )
            .join(
                model.VendorAccount,
                model.Vendor.idVendor == model.VendorAccount.vendorID,
            )
            .outerjoin(
                model.DocumentModel,
                model.DocumentModel.idVendorAccount
                == model.VendorAccount.idVendorAccount,
            )
            .group_by(model.Vendor.idVendor)
            .subquery()
        )

        # Main query
        data = (
            db.query(
                model.Vendor,
                model.Entity,
                model.VendorAccount.idVendorAccount,
                model.DocumentModel.idVendorAccount,
                onboarding_status,
            )
            .options(
                Load(model.Vendor).load_only(
                    "VendorName", "VendorCode", "vendorType", "Address", "City"
                ),
                Load(model.Entity).load_only("EntityName"),
            )
            .join(model.Entity, model.Vendor.entityID == model.Entity.idEntity)
            .join(
                model.VendorAccount,
                model.Vendor.idVendor == model.VendorAccount.vendorID,
            )
            .outerjoin(
                model.DocumentModel,
                model.DocumentModel.idVendorAccount
                == model.VendorAccount.idVendorAccount,
            )
            .join(
                subquery,
                and_(
                    model.Vendor.idVendor == subquery.c.idVendor,
                    case(
                        (model.DocumentModel.modelStatus.in_((4, 5)), 1),
                        (model.DocumentModel.modelStatus.in_((2, 3)), 2),
                        (model.DocumentModel.idDocumentModel.is_(None), 3),
                        else_=3,
                    )
                    == subquery.c.min_status_priority,
                ),
            )
        )
        # Apply additional filters
        for key, val in api_filter.items():
            if key == "ent_id" and val:
                data = data.filter(model.Entity.idEntity == val)
            if key == "ven_code" and val:
                data = data.filter(
                    or_(
                        model.Vendor.VendorName.ilike(f"%{val}%"),
                        model.Vendor.VendorCode.ilike(f"%{val}%"),
                    )
                )
            if key == "onb_status" and val:
                if val == "In-Progress":
                    data = data.filter(model.DocumentModel.modelStatus.in_((2, 3)))
                elif val == "Onboarded":
                    data = data.filter(model.DocumentModel.modelStatus.in_((4, 5)))
                elif val == "Not-Onboarded":
                    data = data.filter(
                        (
                            model.DocumentModel.idDocumentModel.is_(None)
                            | model.DocumentModel.modelStatus.not_in((2, 3, 4, 5))
                        )
                    )
        # Apply filters for vendor type
        if vendor_type:
            if vendor_type == "PO_Based":
                md_ids = db.query(model.FRMetaData.idInvoiceModel).filter_by(
                    vendorType="PO based"
                )
                ven_acc_ids = db.query(model.VendorAccount.vendorID).filter(
                    model.DocumentModel.idDocumentModel.in_(md_ids)
                )
                data = data.filter(model.Vendor.idVendor.in_(ven_acc_ids))
            elif vendor_type == "NON_PO_Based":
                md_ids = db.query(model.FRMetaData.idInvoiceModel).filter_by(
                    vendorType="Non-PO based"
                )
                ven_acc_ids = db.query(model.VendorAccount.vendorID).filter(
                    model.DocumentModel.idDocumentModel.in_(md_ids)
                )
                data = data.filter(model.Vendor.idVendor.in_(ven_acc_ids))
        if ven_status:
            if ven_status == "A":
                data = data.filter(
                    func.jsonb_extract_path_text(
                        model.Vendor.miscellaneous, "VENDOR_STATUS"
                    )
                    == "A"
                )

            elif ven_status == "I":
                data = data.filter(
                    func.jsonb_extract_path_text(
                        model.Vendor.miscellaneous, "VENDOR_STATUS"
                    )
                    == "I"
                )
        # Total count query (with filters applied)
        total_count = data.distinct(model.Vendor.idVendor).count()
        # Pagination
        offset, limit = off_limit
        off_val = (offset - 1) * limit
        if off_val < 0:
            return Response(
                status_code=403,
                headers={"ClientError": "Please provide a valid offset value."},
            )

        # Execute query and apply pagination after filtering
        data = data.distinct().limit(limit).offset(off_val).all()

        # Prepare result
        result = {"data": [], "total_count": total_count}
        for row in data:
            row_dict = {}
            for idx, col in enumerate(row):
                if isinstance(col, model.Vendor):
                    row_dict["Vendor"] = {
                        "idVendor": col.idVendor,
                        "VendorName": col.VendorName,
                        "VendorCode": col.VendorCode,
                        "vendorType": col.vendorType,
                        "Address": col.Address,
                        "City": col.City,
                    }
                elif isinstance(col, model.Entity):
                    row_dict["Entity"] = {
                        "EntityName": col.EntityName,
                        "idEntity": col.idEntity,
                    }
                elif isinstance(col, int):
                    row_dict["idVendorAccount"] = col
                elif isinstance(col, str):
                    row_dict["OnboardedStatus"] = col
                elif col is None:
                    row_dict[f"col{idx}"] = None
            result["data"].append(row_dict)

        return result
    except Exception as e:
        logger.error(f"line 363 readvendorbyuid crud : {traceback.format_exc()}")
        return Response(
            status_code=500, headers={"Error": "Server error", "Desc": "Invalid result"}
        )
    finally:
        db.close()


async def checkonboarded(u_id, db):
    sub_query1 = (
        db.query(model.UserAccess.EntityID)
        .filter_by(UserID=u_id, isActive=1)
        .distinct()
    )
    ven_id = (
        db.query(model.VendorAccount.vendorID)
        .filter(model.VendorAccount.entityID.in_(sub_query1))
        .distinct()
    )
    data = (
        db.query(model.DocumentModel, model.Vendor, model.Entity, model.VendorAccount)
        .options(
            Load(model.Vendor).load_only(
                "VendorName", "VendorCode", "Email", "Contact"
            ),
            Load(model.Entity).load_only("EntityName"),
            Load(model.VendorAccount).load_only("idVendorAccount", "Account"),
            Load(model.DocumentModel).load_only("idDocumentModel"),
        )
        .filter(
            model.VendorAccount.idVendorAccount == model.DocumentModel.idVendorAccount,
            model.Vendor.entityID == model.Entity.idEntity,
            model.Vendor.idVendor == model.VendorAccount.vendorID,
            model.DocumentModel.modelStatus.in_([2, 3, 4, 5]),
        )
        .filter(model.Vendor.idVendor.in_(ven_id))
    )
    return data.all()


async def read_vendor_account_permission(u_id, db):
    try:
        return (
            db.query(model.Vendor.VendorCode, model.VendorUserAccess.entityID)
            .filter(model.VendorUserAccess.vendorID == model.Vendor.idVendor)
            .filter(model.VendorUserAccess.vendorUserID == u_id)
            .all()
        )
    except Exception as e:
        return Response(
            status_code=500, headers={"Error": "Server error", "Desc": "Invalid result"}
        )
    finally:
        db.close()


async def readvendorbyid(db: Session, v_id: int):
    """This function read a Vendor details.

    It contains 2 parameter.
    :param v_id: It is a function parameters that is of integer type, it
        provides the vendor Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        return (
            db.query(model.Vendor)
            .options(load_only("VendorName", "VendorCode", "Contact", "TRNNumber"))
            .filter(model.Vendor.idVendor == v_id)
            .all()
        )
    except Exception as e:
        return Response(
            status_code=500, headers={"Error": "Server error", "Desc": "Invalid result"}
        )
    finally:
        db.close()


async def readvendoruser(db: Session, v_id: int):
    """This function read a Vendor user.

    It contains 2 parameter.
    :param v_id: It is a function parameters that is of integer type, it
        provides the vendor Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        return (
            db.query(model.VendorUser).filter(model.VendorUser.vendorID == v_id).all()
        )
    except Exception as e:
        return Response(
            status_code=500, headers={"Error": "Server error", "Desc": "Invalid result"}
        )
    finally:
        db.close()


async def readvendoraccount(u_id, ent_id, db):
    """This function read Vendor account details.

    It contains 2 parameter.
    :param u_id: It is a function parameters that is of integer type, it
        provides the vendor user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        # get vendor id from vendor user id
        is_cust_user = (
            db.query(model.User.isCustomerUser).filter_by(idUser=u_id).scalar()
        )
        vendor_account = db.query(model.VendorAccount).options(load_only("Account"))
        if is_cust_user:
            ent_ids = (
                db.query(model.UserAccess.EntityID)
                .filter_by(UserID=u_id, isActive=1)
                .distinct()
            )
            vendor_account = vendor_account.filter(
                model.VendorAccount.entityID.in_(ent_ids)
            )
            if ent_id:
                vendor_account = vendor_account.filter(
                    model.VendorAccount.entityID == ent_id
                )
        else:
            ven_id = (
                db.query(model.VendorUserAccess.vendorID)
                .filter_by(vendorUserID=u_id, isActive=1)
                .distinct()
            )
            vendor_account = vendor_account.filter(
                model.VendorAccount.vendorID.in_(ven_id)
            )
            if ent_id:
                vendor_account = vendor_account.filter(
                    model.VendorAccount.entityID == ent_id
                )
        return {"result": vendor_account.all()}
    except Exception as e:
        return Response(status_code=500, headers={"Error": "Server error"})
    finally:
        db.close()


async def readvendoraccount_uploadpo(db, u_id: int):
    """This function read Vendor account details.

    It contains 2 parameter.
    :param u_id: It is a function parameters that is of integer type, it
        provides the vendor user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        vuen_id = (
            db.query(model.VendorUserAccess.vendorID).filter_by(vendorUserID=u_id).all()
        )
        ent_id = db.query(model.UserAccess.EntityID).filter_by(isActive=1).distinct()

        en_ids = (
            db.query(model.Entity.idEntity)
            .filter(model.Entity.idEntity.in_(ent_id))
            .all()
        )
        en_ids = tuple(ven_id[0] for ven_id in en_ids)
        vuen_id = tuple(ven1_id[0] for ven1_id in vuen_id)

        # return ven_ids

        if en_ids:
            data = db.execute(
                f"SELECT EntityName, (select JSON_ARRAYAGG(JSON_OBJECT('Account',Account,'idVendorAccount', \
                idVendorAccount)) from vendoraccount where entityID = vend.idEntity and vendorID in {vuen_id if len(vuen_id) > 1 else '(' + str(vuen_id[0]) + ')'}) as vendoraccounts FROM \
                Entity as vend where vend.idEntity in {en_ids if len(en_ids) > 1 else '(' + str(en_ids[0]) + ')'}"
            ).fetchall()
            return {"result": data}
        return {"result": None}
    except Exception as e:
        return Response(status_code=500, headers={"Error": "Server error"})
    finally:
        db.close()


async def read_vendor_details(db, vu_id):
    """This function read Vendor details.

    It contains 2 parameter.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :param vu_id: It is a function parameters that is of integer type,
        it provides the vendor user Id.
    :return: It return a result of dictionary type.
    """
    try:
        # get vendor id from user id
        is_active = (
            db.query(model.User.isActive).filter_by(isActive=1, idUser=vu_id).scalar()
        )
        ven_ids = (
            db.query(model.VendorUserAccess.vendorID)
            .filter_by(vendorUserID=vu_id, isActive=1)
            .distinct()
        )
        ven_code = (
            db.query(model.Vendor.VendorCode)
            .filter(model.Vendor.idVendor.in_(ven_ids))
            .distinct()
        )
        if is_active:
            vendordata = (
                db.query(
                    model.Vendor.VendorName,
                    model.Vendor.Address,
                    model.Vendor.City,
                    model.Vendor.Country,
                    model.Vendor.VendorCode,
                    model.Vendor.Desc,
                    model.Vendor.Email,
                    model.Vendor.TradeLicense,
                    model.Vendor.Contact,
                    model.Vendor.Website,
                    model.Vendor.Salutation,
                    model.Vendor.FirstName,
                    model.Vendor.LastName,
                    model.Vendor.Designation,
                    model.Vendor.VATLicense,
                    model.Vendor.TLExpiryDate,
                    model.Vendor.VLExpiryDate,
                    model.Vendor.TRNNumber,
                )
                .filter(model.Vendor.VendorCode.in_(ven_code))
                .distinct()
                .all()
            )
            return {"result": vendordata}
        return {"result": "no data"}
    except Exception as e:
        print(traceback.format_exc())
        return Response(status_code=500, headers={"Error": "Server error"})
    finally:
        db.close()


async def read_vendor_user(db, vu_id):
    """This function read Vendor user details.

    It contains 2 parameter.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :param vu_id: It is a function parameters that is of integer type,
        it provides the vendor user Id.
    :return: It return a result of dictionary type.
    """
    try:
        ctby_id = db.query(model.User.created_by).filter_by(idUser=vu_id).scalar()
        vendor_user_data = (
            db.query(model.AccessPermissionDef, model.User)
            .options(
                Load(model.User).load_only("firstName", "lastName", "isActive"),
                Load(model.AccessPermissionDef).load_only("NameOfRole"),
            )
            .filter(
                model.User.idUser == model.AccessPermission.userID,
                model.AccessPermission.permissionDefID
                == model.AccessPermissionDef.idAccessPermissionDef,
            )
            .filter(model.User.isCustomerUser == 0)
            .filter(model.AccessPermissionDef.idAccessPermissionDef == 11)
            .all()
        )
        # # each user profile data
        if ctby_id:
            vendor_user_data = vendor_user_data.filter(
                model.User.created_by == vu_id
            ).all()
        else:
            vendor_user_data = vendor_user_data.all()
        for row in vendor_user_data:
            ven_id = (
                db.query(model.VendorUserAccess.vendorID)
                .filter_by(vendorUserID=row[1].idUser, isActive=1)
                .limit(1)
                .scalar()
            )
            vendor_name = (
                db.query(model.Vendor.VendorName).filter_by(idVendor=ven_id).scalar()
            )
            setattr(row[1], "vendor_name", vendor_name)
        return vendor_user_data
    except Exception as e:
        return Response(
            status_code=500, headers={"Error": "Server error", "Desc": "Invalid result"}
        )
    finally:
        db.close()


async def readvendorsites(db, u_id, v_id):
    try:
        data = (
            db.query(model.VendorAccount, model.Entity)
            .options(Load(model.Entity).load_only("EntityName"))
            .filter(model.VendorAccount.entityID == model.Entity.idEntity)
            .filter(model.VendorAccount.vendorID == v_id)
            .all()
        )
        return data
    except Exception as e:
        return Response(
            status_code=500, headers={"Error": "Server error", "Desc": "Invalid result"}
        )
    finally:
        db.close()


async def submit_invoice_vendor(vu_id, inv_id, re_upload, uploadtime, bg_task, db):
    try:
        dochist = {}
        doc_status = 1
        doc_sub_status = 1
        # get user name from vu_id
        user_details = (
            db.query(model.User.firstName, model.User.lastName)
            .filter(model.User.idUser == vu_id)
            .first()
        )
        user_name = (
            user_details[0]
            if user_details[0] is not None
            else "" + " " + user_details[1] if user_details[1] is not None else ""
        )

        if re_upload:
            doc_status = 4
            doc_sub_status = 45
        else:
            sub_statusid = (
                db.query(model.Document.documentsubstatusID)
                .filter(model.Document.idDocument == inv_id)
                .scalar()
            )
            header_error = (
                db.query(
                    model.DocumentData.isError,
                    model.DocumentData.IsUpdated,
                    model.DocumentData.ErrorDesc,
                )
                .filter_by(documentID=inv_id, isError=1)
                .all()
            )
            line_error = (
                db.query(
                    model.DocumentLineItems.isError,
                    model.DocumentLineItems.IsUpdated,
                    model.DocumentLineItems.ErrorDesc,
                )
                .filter_by(documentID=inv_id, isError=1)
                .all()
            )
            is_head_corrected = 0
            is_line_corrected = 0
            for header_row in header_error:
                if (
                    header_row.isError
                    and header_row.ErrorDesc
                    and str(header_row.ErrorDesc).lower().find("low") >= 0
                ):
                    pass
                elif header_row.IsUpdated == 0:
                    is_head_corrected = 1
            for line_row in line_error:
                if (
                    line_row.isError
                    and line_row.ErrorDesc
                    and str(line_row.ErrorDesc).lower().find("low") >= 0
                ):
                    pass
                elif line_row.IsUpdated == 0:
                    is_line_corrected = 1
            if (
                is_head_corrected == 1 or is_line_corrected == 1
            ) and sub_statusid != 29:
                doc_status = 4
                doc_sub_status = 29
                c4 = model.DocumentRuleupdates(
                    documentID=inv_id,
                    documentSubStatusID=29,
                    IsActive=1,
                    type="error",
                    createdOn=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                )
                db.add(c4)
                check = (
                    db.query(model.DocumentHistoryLogs)
                    .filter(
                        model.DocumentHistoryLogs.documentID == inv_id,
                        model.DocumentHistoryLogs.documentdescription.like(
                            "%OCR Error Found%"
                        ),
                    )
                    .first()
                )
                if check is None:
                    dochist["documentID"] = inv_id
                    dochist["documentStatusID"] = doc_status
                    dochist["documentSubStatusID"] = doc_sub_status
                    dochist["documentdescription"] = "OCR Error Found"
                    dochist["userID"] = vu_id
                    dochist["CreatedOn"] = datetime.utcnow().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    db.add(model.DocumentHistoryLogs(**dochist))
                    db.commit()

            elif sub_statusid == 33:
                doc_sub_status = 24
            else:
                doc_sub_status = 3
                db.query(model.DocumentRuleupdates).filter_by(
                    documentID=inv_id, type="error"
                ).update({"IsActive": 0})
                check = (
                    db.query(model.DocumentHistoryLogs)
                    .filter(
                        model.DocumentHistoryLogs.documentID == inv_id,
                        model.DocumentHistoryLogs.documentdescription.like(
                            "%OCR Error Corrected%"
                        ),
                    )
                    .first()
                )
                if check is None:
                    dochist["documentID"] = inv_id
                    # dochist["documentStatusID"] = doc_status
                    dochist["documentSubStatusID"] = doc_sub_status
                    dochist["documentdescription"] = (
                        f"OCR Error Corrected by {user_name}"
                    )
                    dochist["userID"] = vu_id
                    dochist["CreatedOn"] = datetime.utcnow().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    db.add(model.DocumentHistoryLogs(**dochist))
                    db.commit()
        # if line_error:
        #     doc_status = 4
        db.query(model.Document).filter_by(idDocument=inv_id).update(
            {
                "UpdatedOn": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "documentStatusID": doc_status,
                "documentsubstatusID": doc_sub_status,
                "uploadtime": uploadtime,
            }
        )
        check = (
            db.query(model.DocumentHistoryLogs)
            .filter(
                model.DocumentHistoryLogs.documentID == inv_id,
                model.DocumentHistoryLogs.documentdescription.like(
                    "%Invoice submitted by%"
                ),
            )
            .first()
        )
        if check is None:
            dochist["documentID"] = inv_id
            dochist["documentStatusID"] = doc_status
            # dochist["documentsubstatusID"] = doc_sub_status
            dochist["documentdescription"] = f"Invoice submitted by {user_name}"
            dochist["userID"] = vu_id
            dochist["CreatedOn"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            db.add(model.DocumentHistoryLogs(**dochist))
            db.commit()

        return {"result": "submitted"}
    except Exception as e:
        return Response(
            status_code=500, headers={"Error": "Server error", "Desc": "Invalid result"}
        )
    finally:
        db.close()


async def read_vendor_name_codes(db, u_id, off_limit, ven_name):
    try:
        offset, limit = off_limit
        off_val = (offset - 1) * limit
        if off_val < 0:
            return Response(
                status_code=403,
                headers={"ClientError": "please provide right offset value"},
            )
        is_cutomer_user = (
            db.query(model.User.isCustomerUser)
            .filter(model.User.idUser == u_id)
            .scalar()
        )
        vendor_data = db.query(model.Vendor.VendorName, model.Vendor.VendorCode)
        if is_cutomer_user:
            sub_query = (
                db.query(model.UserAccess.EntityID)
                .filter_by(UserID=u_id, isActive=1)
                .distinct()
            )
            vendor_data = vendor_data.filter(model.Vendor.entityID.in_(sub_query))
        else:
            sub_query = (
                db.query(model.VendorUserAccess.vendorID)
                .filter_by(UserID=u_id, isActive=1)
                .distinct()
            )
            vendor_data = vendor_data.filter(model.Vendor.idVendor.in_(sub_query))
        if ven_name:
            vendor_data = vendor_data.filter(
                model.Vendor.VendorName.like(f"%{ven_name}%")
            )
        if limit:
            vendor_data = vendor_data.limit(limit)
        if off_val:
            vendor_data = vendor_data.offset(off_val)
        return vendor_data.distinct().all()
    except Exception as e:
        return Response(
            status_code=500, headers={"Error": "Server error", "Desc": "Invalid result"}
        )
    finally:
        db.close()


async def getVendor_name_list_match(u_id, ven_name, ven_name_search, off_limit, db):
    offset, limit = off_limit
    if offset < 0:
        return Response(
            status_code=403,
            headers={"ClientError": "please provide right offset value"},
        )
    try:
        ent_ids = (
            db.query(model.UserAccess.EntityID)
            .filter_by(UserID=u_id, isActive=1)
            .distinct()
        )
        vendor = (
            db.query(model.Vendor.VendorName, model.Vendor.VendorCode)
            .filter(model.Vendor.entityID.in_(ent_ids))
            .distinct()
        )
        if limit:
            vendor = vendor.limit(limit)
        if offset:
            offset = (offset - 1) * limit
            vendor = vendor.offset(offset)
        if ven_name_search is None:
            choices = []
            vendor = vendor.all()
            for row in vendor:
                choices.append(row.VendorName)
            data = process.extract(ven_name, choices, limit=10)
            data = [row[0] for row in data if row[1] > 80]
            choices.clear()
            for row in vendor:
                if row.VendorName in data:
                    choices.append(row)
        else:
            choices = vendor.filter(
                model.Vendor.VendorName.like(f"%{ven_name_search}%")
            ).all()
        return {"vendorlist": choices}
    except Exception as e:
        print(e)
        return Response(status_code=500, headers={"Error": "Server error"})
    finally:
        db.close()


async def get_vendor_onbaord_status(u_id, ven_code, db):
    try:
        ven_acc_ids = db.query(model.VendorAccount.idVendorAccount).filter(
            model.VendorAccount.Account == ven_code
        )
        is_enabled = (
            db.query(model.DocumentModel.idDocumentModel)
            .filter(model.DocumentModel.modelStatus.between(4, 5))
            .filter(model.DocumentModel.idVendorAccount.in_(ven_acc_ids))
            .all()
        )
        if len(is_enabled):
            return {"result": {"vendor_status": True}}
        else:
            return {"result": {"vendor_status": False}}
    except Exception as e:
        print(e)
        return Response(status_code=500, headers={"Error": "Server error"})
    finally:
        db.close()


# download api for vendor status
async def downloadvendorbyuid(u_id, vendor_type, db, api_filter):
    """This function read a Vendor.

    It contains 1 parameter.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        # case statement for onboarding status
        onboarding_status = case(
            [
                (model.DocumentModel.modelStatus.in_((2, 3, 4, 5)), "Onboarded"),
                (model.DocumentModel.idDocumentModel.is_(None), "Not-Onboarded"),
            ],
            else_="Not-Onboarded",
        ).label("OnboardedStatus")
        sub_query1 = (
            db.query(model.UserAccess.EntityID)
            .filter_by(UserID=u_id, isActive=1)
            .distinct()
        )
        ven_id = (
            db.query(model.VendorAccount.vendorID)
            .filter(model.VendorAccount.entityID.in_(sub_query1))
            .distinct()
        )
        data = (
            db.query(
                model.Vendor.VendorName,
                model.Entity.EntityName,
                model.VendorAccount.Account,
                model.DocumentModel.modelName,
                onboarding_status,
            )
            .filter(model.Vendor.entityID == model.Entity.idEntity)
            .filter(model.Vendor.idVendor == model.VendorAccount.vendorID)
            .join(
                model.DocumentModel,
                model.DocumentModel.idVendorAccount
                == model.VendorAccount.idVendorAccount,
                isouter=True,
            )
            .filter(model.Vendor.idVendor.in_(ven_id))
        )
        if vendor_type and vendor_type == "PO_Based":
            md_ids = db.query(model.FRMetaData.idInvoiceModel).filter_by(
                vendorType="PO based"
            )
            ven_acc_ids = db.query(model.VendorAccount.vendorID).filter(
                model.DocumentModel.idDocumentModel.in_(md_ids)
            )
            data = data.filter(model.Vendor.idVendor.in_(ven_acc_ids))
        if vendor_type and vendor_type == "NON_PO_Based":
            md_ids = db.query(model.FRMetaData.idInvoiceModel).filter_by(
                vendorType="Non-PO based"
            )
            ven_acc_ids = db.query(model.VendorAccount.vendorID).filter(
                model.DocumentModel.idDocumentModel.in_(md_ids)
            )
            data = data.filter(model.Vendor.idVendor.in_(ven_acc_ids))
        for key, val in api_filter.items():
            if key == "ent_id" and val:
                data = data.filter(model.Entity.idEntity == val)
            if key == "ven_code" and val:
                data = data.filter(model.Vendor.VendorName.like(f"%{val}%"))
            if key == "onb_status" and val:
                if val == "Onboarded":
                    data = data.filter(
                        model.DocumentModel.modelStatus.in_((2, 3, 4, 5))
                    )
                if val == "Not-Onboarded":
                    data = data.filter(
                        (
                            model.DocumentModel.idDocumentModel.is_(None)
                            | model.DocumentModel.modelStatus.not_in((2, 3, 4, 5))
                        )
                    )
        onboarded_status_dataframe = pd.read_sql(data.statement, data.session.bind)
        onboarded_file = io.BytesIO()
        onboarded_status_dataframe.to_excel(
            onboarded_file, sheet_name="Vendor Onboarded Status", index=False
        )
        onboarded_file.seek(0)
        return onboarded_file
        # print(onboarded_status_dataframe.info)
    except Exception as e:
        print(traceback.format_exc())
        return Response(
            status_code=500, headers={"Error": "Server error", "Desc": "Invalid result"}
        )
    finally:
        db.close()

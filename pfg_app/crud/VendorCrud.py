import os
import re
import traceback

import pytz as tz
from fastapi.responses import Response
from sqlalchemy import and_, case, func, or_
from sqlalchemy.orm import Load

import pfg_app.model as model
from pfg_app.logger_module import logger

tz_region_name = os.getenv("serina_tz", "Asia/Dubai")
tz_region = tz.timezone(tz_region_name)


async def readvendorname(u_id, db):
    """This function read list of VendorNames.

    It contains 2 parameter.
    :param u_id: The user ID for which to fetch vendor data.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: It return a result of dictionary type.
    """
    try:
        # Query to get vendor names and filter by VENDOR_STATUS
        query = db.query(model.Vendor.VendorName).filter(
            func.jsonb_extract_path_text(model.Vendor.miscellaneous, "VENDOR_STATUS")
            == "A"
        )
        data = query.all()
        return data

    except Exception:
        logger.error(traceback.format_exc())
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

        # Function to normalize strings by removing non-alphanumeric
        # characters and converting to lowercase
        def normalize_string(input_str):
            return func.lower(func.regexp_replace(input_str, r"[^a-zA-Z0-9]", "", "g"))

        # Apply additional filters
        for key, val in api_filter.items():
            if key == "ent_id" and val:
                data = data.filter(model.Entity.idEntity == val)
            if key == "ven_code" and val:
                # Normalize the user input filter
                normalized_filter = re.sub(r"[^a-zA-Z0-9]", "", val.lower())

                # Create a pattern for the search with wildcards
                pattern = f"%{normalized_filter}%"
                data = data.filter(
                    or_(
                        normalize_string(model.Vendor.VendorName).ilike(pattern),
                        normalize_string(model.Vendor.VendorCode).ilike(pattern),
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
        # Filter by vendor status
        if ven_status:
            if ven_status in ["A", "I"]:
                data = data.filter(
                    func.jsonb_extract_path_text(
                        model.Vendor.miscellaneous, "VENDOR_STATUS"
                    )
                    == ven_status
                )
            else:
                return {"error": f"Invalid vendor status: {ven_status}"}
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
    except Exception:
        logger.error(traceback.format_exc())
        return Response(
            status_code=500, headers={"Error": "Server error", "Desc": "Invalid result"}
        )
    finally:
        db.close()


async def readvendorlist(u_id, vendor_type, db, api_filter, ven_status):
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

        # Function to normalize strings by removing non-alphanumeric
        # characters and converting to lowercase
        def normalize_string(input_str):
            return func.lower(func.regexp_replace(input_str, r"[^a-zA-Z0-9]", "", "g"))

        # Apply additional filters
        for key, val in api_filter.items():
            if key == "ent_id" and val:
                data = data.filter(model.Entity.idEntity == val)
            if key == "ven_code" and val:
                # Normalize the user input filter
                normalized_filter = re.sub(r"[^a-zA-Z0-9]", "", val.lower())

                # Create a pattern for the search with wildcards
                pattern = f"%{normalized_filter}%"
                data = data.filter(
                    or_(
                        normalize_string(model.Vendor.VendorName).ilike(pattern),
                        normalize_string(model.Vendor.VendorCode).ilike(pattern),
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
        # Filter by vendor status
        if ven_status:
            if ven_status in ["A", "I"]:
                data = data.filter(
                    func.jsonb_extract_path_text(
                        model.Vendor.miscellaneous, "VENDOR_STATUS"
                    )
                    == ven_status
                )
            else:
                return {"error": f"Invalid vendor status: {ven_status}"}
        # Total count query (with filters applied)
        total_count = data.distinct(model.Vendor.idVendor).count()

        # Execute query and apply pagination after filtering
        # Execute query and fetch all data
        data = data.distinct().all()

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
    except Exception:
        logger.error(traceback.format_exc())
        return Response(
            status_code=500, headers={"Error": "Server error", "Desc": "Invalid result"}
        )
    finally:
        db.close()

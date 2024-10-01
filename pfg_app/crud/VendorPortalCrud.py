import os
import traceback
from datetime import datetime

import pytz as tz
from fastapi.responses import Response

import pfg_app.model as model
from pfg_app.logger_module import logger

tz_region_name = os.getenv("serina_tz", "Asia/Dubai")
tz_region = tz.timezone(tz_region_name)


async def read_po_numbers(u_id, vendorAccountID, ent_id, db):
    """Function reads PO numbers from Invoice table for a given Vendor Account.

    - vendorAccountID: Unique identifier for a vendor account
    - db: db session variable
    """
    try:
        # subquery to get access vendor id from db
        # sub_query = db.query(model.VendorAccount.idVendorAccount).filter(
        #     model.VendorAccount.vendorID == vendorID).distinct()

        return (
            db.query(model.Document.PODocumentID, model.Document.idDocument)
            .filter(
                model.Document.vendorAccountID == vendorAccountID,
                model.Document.idDocumentType == 1,
                model.Document.entityID == ent_id,
                model.Document.documentStatusID == 12,
            )
            .all()
        )
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def add_label(invoicemodelID, labelDef, db):
    """Function to add label to a PO.

    - invoice_ID: invoice identifier
    - labelDef: definition for the new label
    - db: db session variable
    """
    try:
        # current time
        createdTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        # to dict
        labelDef = dict(labelDef)
        labelDef["CreatedOn"] = createdTime
        labelDef["UpdatedOn"] = createdTime
        labelDef["idDocumentModel"] = invoicemodelID
        tagsDB = model.DocumentTagDef(**labelDef)
        db.add(tagsDB)
        db.commit()
        return {"result": "Updated", "records": labelDef}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


async def add_linetemtag(invoicemodelID, lineitemDef, db):
    """Function to add a new lineitem tag.

    - invoice_ID: invoice identifier
    - lineitemDef: definition for the new lineitem
    - db: db session variable
    """
    try:
        # to dict
        lineitemDef = dict(lineitemDef)
        lineitemDef["idDocumentModel"] = invoicemodelID
        tagsDB = model.DocumentLineItemTags(**lineitemDef)
        db.add(tagsDB)
        db.commit()
        return {"result": "Updated", "records": lineitemDef}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()


def get_tagdef_id(tagelabel):
    """Function to get tag label ID."""

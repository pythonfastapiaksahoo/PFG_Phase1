import traceback
from datetime import datetime, timedelta

import pytz as tz
from fastapi.responses import Response
from sqlalchemy import and_, case, func
from sqlalchemy.orm import load_only

try:
    import model as models
except ImportError:
    from .. import model as models

import os
import sys

sys.path.append("..")
tz_region_name = os.getenv("serina_tz", "Asia/Dubai")
tz_region = tz.timezone(tz_region_name)


async def read_galadhari_summary(u_id, ftdate, sp_id, fentity, db):
    # summary only for accessible entity accounts
    entity_list = (
        db.query(models.Entity.idEntity)
        .filter(models.UserAccess.isActive == 1)
        .filter(models.UserAccess.EntityID == models.Entity.idEntity)
        .filter(models.UserAccess.UserID == u_id)
    )
    # select current month date
    if not ftdate:
        ftdate = datetime.utcnow() - timedelta(days=5)
        ftdate = ftdate.strftime("%Y-%m")
    # case statement for ocr status
    ocr_status = case(
        [
            (models.Document.documentStatusID.in_((2, 3, 7)), "OCR SUCCESS"),
            (models.Document.documentStatusID.in_((4, 5, 6)), "OCR FAILED"),
        ],
        else_="Unknown",
    ).label("ocr_status")
    # case statement for voucher status
    voucher_status = case(
        [(models.Document.documentStatusID == 7, "ERP UPDATED")], else_="ERP PENDING"
    ).label("voucher_status")
    try:
        # to get active accounts, date filter cannot be applied for this
        active_accounts = (
            db.query(models.ServiceAccount.idServiceAccount)
            .filter_by(isActive=1)
            .filter(models.ServiceAccount.entityID.in_(entity_list))
        )
        # total pending, voucher credation status
        total_pending = (
            db.query(models.Document.idDocument)
            .filter(
                models.Document.documentStatusID.in_((2, 3, 4, 5, 6)),
                models.Document.supplierAccountID.isnot(None),
            )
            .filter(models.Document.CreatedOn.ilike(f"%{ftdate}%"))
            .filter(models.Document.entityID.in_(entity_list))
        )
        # total failed, count does not change even if OCR is corrected manually
        total_failed = (
            db.query(models.Document.idDocument)
            .filter(
                models.Document.documentStatusID.in_((4, 5, 6)),
                models.Document.supplierAccountID.isnot(None),
            )
            .filter(models.Document.CreatedOn.ilike(f"%{ftdate}%"))
            .filter(models.Document.entityID.in_(entity_list))
            .distinct()
        )
        # total processed is the count of voucher created
        total_processed = (
            db.query(models.Document.idDocument)
            .filter(
                models.Document.documentStatusID == 7,
                models.Document.supplierAccountID.isnot(None),
            )
            .filter(models.Document.CreatedOn.ilike(f"%{ftdate}%"))
            .filter(models.Document.entityID.in_(entity_list))
            .distinct()
        )
        # total documents pushed to the system
        total_downloaded = (
            db.query(models.Document.idDocument)
            .filter(models.Document.supplierAccountID.isnot(None))
            .filter(models.Document.CreatedOn.ilike(f"%{ftdate}%"))
            .filter(models.Document.entityID.in_(entity_list))
        )
        # Table data with drilled down count
        drill_down_data = (
            db.query(
                models.ServiceProvider.ServiceProviderName,
                func.count(models.ServiceProvider.ServiceProviderName).label("Count"),
                ocr_status,
                models.Entity.EntityName,
                voucher_status,
            )
            .filter(models.Document.supplierAccountID.isnot(None))
            .filter(models.ServiceProvider.entityID == models.Entity.idEntity)
            .filter(
                models.Document.supplierAccountID
                == models.ServiceAccount.idServiceAccount
            )
            .filter(
                models.ServiceAccount.serviceProviderID
                == models.ServiceProvider.idServiceProvider
            )
            .group_by(models.Entity.idEntity)
            .group_by(models.ServiceProvider.ServiceProviderName)
            .group_by(ocr_status)
            .group_by(voucher_status)
            .filter(models.Document.CreatedOn.ilike(f"%{ftdate}%"))
            .filter(models.Document.entityID.in_(entity_list))
        )

        # entity filter
        if fentity:
            entity_id = (
                db.query(models.Entity.idEntity)
                .filter(models.UserAccess.EntityID == models.Entity.idEntity)
                .filter(models.Entity.idEntity == fentity)
                .filter(models.UserAccess.UserID == u_id)
                .distinct()
                .scalar()
            )
            if entity_id:
                active_accounts = active_accounts.filter(
                    models.ServiceAccount.entityID == fentity
                )
                total_pending = total_pending.filter(
                    models.Document.entityID == fentity
                )
                total_failed = total_failed.filter(models.Document.entityID == fentity)
                total_processed = total_processed.filter(
                    models.Document.entityID == fentity
                )
                total_downloaded = total_downloaded.filter(
                    models.Document.entityID == fentity
                )
                drill_down_data = drill_down_data.filter(
                    models.Document.entityID == fentity
                )
            else:
                return Response(status_code=400)

        # service provider filter
        if sp_id:
            active_accounts = active_accounts.filter(
                models.ServiceAccount.serviceProviderID == sp_id
            )
            sub_query = db.query(models.ServiceAccount.idServiceAccount).filter(
                models.ServiceAccount.serviceProviderID == sp_id
            )
            total_pending = total_pending.filter(
                models.Document.supplierAccountID.in_(sub_query)
            )
            total_failed = total_failed.filter(
                models.Document.supplierAccountID.in_(sub_query)
            )
            total_downloaded = total_downloaded.filter(
                models.Document.supplierAccountID.in_(sub_query)
            )
            total_processed = total_processed.filter(
                models.Document.supplierAccountID.in_(sub_query)
            )
            drill_down_data = drill_down_data.filter(
                models.Document.supplierAccountID.in_(sub_query)
            )
        # getting count , using len cos that will be faster
        active_count = len(active_accounts.all())
        tot_pen = len(total_pending.all())
        tot_fail = len(total_failed.all())
        tot_dwn = len(total_downloaded.all())
        tot_dpd = len(total_processed.all())
        return {
            "result": {
                "active_accounts": active_count,
                "total_pending": tot_pen,
                "total_failed": tot_fail,
                "total_downloaded": tot_dwn,
                "total_processed": tot_dpd,
                "drill_down_data": drill_down_data.all(),
            }
        }
    except Exception as e:
        traceback.print_exc()
        return Response(status_code=500)
    finally:
        db.close()


async def read_pages_summary(u_id, ftdate, endate, entity, vendor, sp, db):
    # summary only for accessible entity accounts
    try:

        entity_list = (
            db.query(models.Entity.idEntity)
            .filter(models.UserAccess.isActive == 1)
            .filter(models.UserAccess.EntityID == models.Entity.idEntity)
            .filter(models.UserAccess.UserID == u_id)
        )
        if entity:
            entity_list = [entity]
        date_specified = False
        if ftdate:
            date_specified = True
        if endate:
            date_specified = True
        if date_specified:
            vendor_docs = (
                db.query(
                    func.sum(models.Document.documentTotalPages).label("TotalPages"),
                    func.count(models.Document.documentTotalPages).label(
                        "TotalInvoices"
                    ),
                    models.Entity.EntityName,
                    models.Vendor.VendorName,
                    models.DocumentStatus.status,
                )
                .filter(models.Document.entityID.in_(entity_list))
                .filter(models.Entity.idEntity == models.Document.entityID)
                .filter(
                    models.Document.vendorAccountID
                    == models.VendorAccount.idVendorAccount
                )
                .filter(models.VendorAccount.vendorID == models.Vendor.idVendor)
                .filter(models.Document.documentTotalPages.isnot(None))
                .filter(
                    models.Document.documentStatusID
                    == models.DocumentStatus.idDocumentstatus
                )
                .filter(
                    models.Document.documentStatusID != 0,
                    models.Document.vendorAccountID.isnot(None),
                    models.Document.idDocumentType == 3,
                )
                .filter(
                    and_(
                        func.date(models.Document.CreatedOn) >= ftdate,
                        func.date(models.Document.CreatedOn) <= endate,
                    )
                )
                .group_by(
                    models.Document.entityID,
                    models.Document.vendorAccountID,
                    models.DocumentStatus.status,
                )
            )
        else:
            vendor_docs = (
                db.query(
                    func.sum(models.Document.documentTotalPages).label("TotalPages"),
                    func.count(models.Document.documentTotalPages).label(
                        "TotalInvoices"
                    ),
                    models.Entity.EntityName,
                    models.Vendor.VendorName,
                    models.DocumentStatus.status,
                )
                .filter(models.Document.entityID.in_(entity_list))
                .filter(models.Entity.idEntity == models.Document.entityID)
                .filter(
                    models.Document.vendorAccountID
                    == models.VendorAccount.idVendorAccount
                )
                .filter(models.VendorAccount.vendorID == models.Vendor.idVendor)
                .filter(models.Document.documentTotalPages.isnot(None))
                .filter(
                    models.Document.documentStatusID
                    == models.DocumentStatus.idDocumentstatus
                )
                .filter(
                    models.Document.documentStatusID != 0,
                    models.Document.vendorAccountID.isnot(None),
                    models.Document.idDocumentType == 3,
                )
                .group_by(
                    models.Document.entityID,
                    models.Document.vendorAccountID,
                    models.DocumentStatus.status,
                )
            )

        vendor_data = vendor_docs.all()
        if vendor:
            vendor_data = vendor_docs.filter(models.Vendor.VendorName == vendor).all()
        vendorTotalPages = sum(i["TotalPages"] for i in vendor_data)
        vendorTotalInvoices = sum(i["TotalInvoices"] for i in vendor_data)
        if date_specified:
            supplier_docs = (
                db.query(
                    func.sum(models.Document.documentTotalPages).label("TotalPages"),
                    func.count(models.Document.documentTotalPages).label(
                        "TotalInvoices"
                    ),
                    models.Entity.EntityName,
                    models.ServiceProvider.ServiceProviderName,
                    models.DocumentStatus.status,
                )
                .filter(models.Document.entityID.in_(entity_list))
                .filter(models.Entity.idEntity == models.Document.entityID)
                .filter(
                    models.Document.supplierAccountID
                    == models.ServiceAccount.idServiceAccount
                )
                .filter(
                    models.ServiceAccount.serviceProviderID
                    == models.ServiceProvider.idServiceProvider
                )
                .filter(models.Document.documentTotalPages.isnot(None))
                .filter(
                    models.Document.documentStatusID
                    == models.DocumentStatus.idDocumentstatus
                )
                .filter(
                    models.Document.documentStatusID != 0,
                    models.Document.supplierAccountID.isnot(None),
                    models.Document.idDocumentType == 3,
                )
                .filter(
                    and_(
                        func.date(models.Document.CreatedOn) >= ftdate,
                        func.date(models.Document.CreatedOn) <= endate,
                    )
                )
                .group_by(
                    models.Document.entityID,
                    models.Document.supplierAccountID,
                    models.DocumentStatus.status,
                )
            )
        else:
            supplier_docs = (
                db.query(
                    func.sum(models.Document.documentTotalPages).label("TotalPages"),
                    func.count(models.Document.documentTotalPages).label(
                        "TotalInvoices"
                    ),
                    models.Entity.EntityName,
                    models.ServiceProvider.ServiceProviderName,
                    models.DocumentStatus.status,
                )
                .filter(models.Document.entityID.in_(entity_list))
                .filter(models.Entity.idEntity == models.Document.entityID)
                .filter(
                    models.Document.supplierAccountID
                    == models.ServiceAccount.idServiceAccount
                )
                .filter(
                    models.ServiceAccount.serviceProviderID
                    == models.ServiceProvider.idServiceProvider
                )
                .filter(models.Document.documentTotalPages.isnot(None))
                .filter(
                    models.Document.documentStatusID
                    == models.DocumentStatus.idDocumentstatus
                )
                .filter(
                    models.Document.documentStatusID != 0,
                    models.Document.supplierAccountID.isnot(None),
                    models.Document.idDocumentType == 3,
                )
                .group_by(
                    models.Document.entityID,
                    models.Document.supplierAccountID,
                    models.DocumentStatus.status,
                )
            )

        supplier_data = supplier_docs.all()
        if sp:
            supplier_data = supplier_docs.filter(
                models.ServiceProvider.ServiceProviderName == sp
            ).all()
        supplierTotalPages = sum(i["TotalPages"] for i in supplier_data)
        supplierTotalInvoices = sum(i["TotalInvoices"] for i in supplier_data)

        return {
            "supplier_data": {
                "data": supplier_data,
                "summary": {
                    "TotalPages": supplierTotalPages,
                    "TotalInvoices": supplierTotalInvoices,
                },
            },
            "vendor_data": {
                "data": vendor_data,
                "summary": {
                    "TotalPages": vendorTotalPages,
                    "TotalInvoices": vendorTotalInvoices,
                },
            },
        }
    except Exception as e:
        return {"error": str(e)}
    finally:
        db.close()


async def read_entity_filter(u_id, db):
    try:
        is_customer_user = (
            db.query(models.User.isCustomerUser).filter_by(idUser=u_id).scalar()
        )
        if is_customer_user:
            entity_list = (
                db.query(models.Entity)
                .options(load_only("EntityName"))
                .filter(models.UserAccess.isActive == 1)
                .filter(models.UserAccess.EntityID == models.Entity.idEntity)
                .filter(models.UserAccess.UserID == u_id)
                .all()
            )
        else:
            entity_list = (
                db.query(models.Entity)
                .options(load_only("EntityName"))
                .filter(models.VendorUserAccess.isActive == 1)
                .filter(models.VendorUserAccess.entityID == models.Entity.idEntity)
                .filter(models.VendorUserAccess.vendorUserID == u_id)
                .all()
            )
        return {"result": entity_list}
    except Exception as e:
        return Response(status_code=500)
    finally:
        db.close()


async def read_service_filter(u_id, db):
    try:
        service_list = (
            db.query(models.ServiceProvider)
            .options(load_only(models.ServiceProvider.ServiceProviderName))
            .all()
        )
        return {"result": service_list}
    except Exception as e:
        return Response(status_code=500)
    finally:
        db.close()

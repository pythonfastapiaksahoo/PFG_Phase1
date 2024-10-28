import base64
import datetime
import os
import traceback

import requests
from azure.storage.blob import BlobServiceClient
from fastapi import HTTPException, Response
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import load_only

import pfg_app.model as model
from pfg_app import settings
from pfg_app.core.utils import get_credential
from pfg_app.crud.InvoiceCrud import update_docHistory
from pfg_app.logger_module import logger
from pfg_app.schemas.pfgtriggerSchema import InvoiceVoucherSchema


async def getDepartmentMaster(db):

    try:
        return db.query(model.PFGDepartment).all()
    except Exception:
        logger.error(f"Error: { traceback.format_exc()}")
        return Response(status_code=500)
    finally:
        db.close()


async def getStoreMaster(db):

    try:
        return db.query(model.PFGStore).all()
    except Exception:
        logger.error(f"Error: { traceback.format_exc()}")
        return Response(status_code=500)
    finally:
        db.close()


async def getAccountMaster(db):

    try:
        return db.query(model.PFGAccount).all()
    except Exception:
        logger.error(f"Error: { traceback.format_exc()}")
        return Response(status_code=500)
    finally:
        db.close()


async def getVendorMaster(db):
    try:
        vendors = db.query(model.PFGVendor).all()

        vendor_list = []
        for vendor in vendors:
            vendor_data = {
                "SETID": vendor.SETID,
                "VENDOR_ID": vendor.VENDOR_ID,
                "NAME1": vendor.NAME1,
                "NAME2": vendor.NAME2.strip(),  # Strip to handle empty spaces
                "VENDOR_CLASS": vendor.VENDOR_CLASS,
                "VENDOR_STATUS": vendor.VENDOR_STATUS,
                "DEFAULT_LOC": vendor.DEFAULT_LOC,
                "VENDOR_LOC": vendor.VENDOR_LOC if vendor.VENDOR_LOC else [],
                "VENDOR_ADDR": vendor.VENDOR_ADDR if vendor.VENDOR_ADDR else [],
            }
            vendor_list.append(vendor_data)

        return vendor_list

    except Exception:
        logger.error(f"Error: { traceback.format_exc()}")
        return Response(status_code=500)

    finally:
        db.close()


async def getProjectMaster(db):

    try:
        return db.query(model.PFGProject).all()
    except Exception:
        logger.error(f"Error: { traceback.format_exc()}")
        return Response(status_code=500)
    finally:
        db.close()


async def getProjectActivityMaster(db):

    try:
        return db.query(model.PFGProjectActivity).all()
    except Exception:
        logger.error(f"Error: { traceback.format_exc()}")
        return Response(status_code=500)
    finally:
        db.close()


async def getReceiptMaster(db):

    try:
        return db.query(model.PFGReceipt).all()
    except Exception:
        logger.error(f"Error: { traceback.format_exc()}")
        return Response(status_code=500)
    finally:
        db.close()


async def updateDepartmentMaster(Departmentdata, db):

    try:
        response = []
        # Validate required fields
        for data in Departmentdata:
            if not all([data.SETID, data.DEPTID, data.EFFDT]):
                raise HTTPException(
                    status_code=400, detail="required fields missing!!!"
                )

            department_data = data.dict()

            # Find existing department record
            existing_department = (
                db.query(model.PFGDepartment)
                .filter(
                    model.PFGDepartment.SETID == data.SETID,
                    model.PFGDepartment.DEPTID == data.DEPTID,
                    model.PFGDepartment.EFFDT == data.EFFDT,
                )
                .first()
            )

            if existing_department:
                # Update existing department record
                for key, value in department_data.items():
                    setattr(existing_department, key, value)
                db.commit()
                db.refresh(existing_department)
                response.append(existing_department)

            else:
                # Insert new department record
                new_department = model.PFGDepartment(**department_data)
                db.add(new_department)
                db.commit()
                db.refresh(new_department)
                response.append(new_department)

        await SyncDepartmentMaster(db, Departmentdata)
        return {"result": "Updated", "records": response}
    except SQLAlchemyError:
        logger.error(f"Error: { traceback.format_exc()}")
        db.rollback()
        raise HTTPException(
            status_code=500, detail="An error occurred while processing the request."
        )

    finally:
        db.close()


async def updateStoreMaster(Storedata, db):
    try:
        response = []
        # Validate required fields
        for data in Storedata:
            if not all([data.SETID, data.STORE, data.EFFDT]):
                raise HTTPException(
                    status_code=400, detail="required fields missing!!!"
                )

            store_data = data.dict()

            # Find existing department record
            existing_store = (
                db.query(model.PFGStore)
                .filter(
                    model.PFGStore.SETID == data.SETID,
                    model.PFGStore.STORE == data.STORE,
                    model.PFGStore.EFFDT == data.EFFDT,
                )
                .first()
            )

            if existing_store:
                # Update existing department record
                for key, value in store_data.items():
                    setattr(existing_store, key, value)
                db.commit()
                db.refresh(existing_store)
                response.append(existing_store)

            else:
                # Insert new department record
                new_store = model.PFGStore(**store_data)
                db.add(new_store)
                db.commit()
                db.refresh(new_store)
                response.append(new_store)

        return {"result": "Updated", "records": response}
    except SQLAlchemyError:
        logger.error(f"Error: { traceback.format_exc()}")
        db.rollback()
        raise HTTPException(
            status_code=500, detail="An error occurred while processing the request."
        )

    finally:
        db.close()


async def updateVendorMaster(vendordata, db):
    try:
        # print("vendordata: ", vendordata)
        # Initialize a set to store processed vendor IDs to avoid duplicates
        processed_vendor_ids = set()
        for data in vendordata:
            # Validate required fields in the main vendor data
            if not data.SETID:
                raise HTTPException(
                    status_code=400, detail="SETID is missing in vendor JSON body."
                )

            # Validate required fields in VENDOR_LOC
            if data.VENDOR_LOC:
                for loc in data.VENDOR_LOC:
                    if not all(
                        [
                            loc.get("SETID"),
                            loc.get("VENDOR_ID"),
                            loc.get("VENDOR_LOC"),
                            loc.get("EFFDT"),
                        ]
                    ):
                        raise HTTPException(
                            status_code=400,
                            detail="Required fields missing in VENDOR_LOC.",
                        )

                    if loc["SETID"] != data.SETID or loc["VENDOR_ID"] != data.VENDOR_ID:
                        raise HTTPException(
                            status_code=400,
                            detail="SETID and VENDOR_ID must match "
                            + "between vendor_data and VENDOR_LOC.",
                        )

            # Validate required fields in VENDOR_ADDR
            if data.VENDOR_ADDR:
                for addr in data.VENDOR_ADDR:
                    if not all(
                        [
                            addr.get("SETID"),
                            addr.get("VENDOR_ID"),
                            addr.get("ADDRESS_SEQ_NUM"),
                        ]
                    ):
                        raise HTTPException(
                            status_code=400,
                            detail="Required fields missing in VENDOR_ADDR.",
                        )

                    if (
                        addr["SETID"] != data.SETID
                        or addr["VENDOR_ID"] != data.VENDOR_ID
                    ):
                        raise HTTPException(
                            status_code=400,
                            detail="SETID and VENDOR_ID must match "
                            + "between vendor_data and VENDOR_ADDR.",
                        )

            # Convert Pydantic model to dict and handle nested objects
            vendor_data = data.dict()
            # print("vendor_data_dict :",vendor_data )
            # Extract and remove VENDOR_LOC and VENDOR_ADDR for separate processing
            vendor_loc_data = vendor_data.pop("VENDOR_LOC", [])
            vendor_addr_data = vendor_data.pop("VENDOR_ADDR", [])

            # print("vendor_loc_data: ",vendor_loc_data)
            # print("vendor_addr_data: ", vendor_addr_data)
            # Check if the vendor already exists in the database
            existing_vendor = (
                db.query(model.PFGVendor)
                .filter(
                    model.PFGVendor.SETID == vendor_data["SETID"],
                    model.PFGVendor.VENDOR_ID == vendor_data["VENDOR_ID"],
                )
                .first()
            )

            if existing_vendor:
                # Update the existing vendor record
                for key, value in vendor_data.items():
                    setattr(existing_vendor, key, value)

                # Process VENDOR_LOC
                existing_vendor_loc = (
                    existing_vendor.VENDOR_LOC if existing_vendor.VENDOR_LOC else []
                )
                print(f"Initial existing_vendor_loc: {existing_vendor_loc}")

                for loc in vendor_loc_data:
                    existing_loc = next(
                        (
                            line
                            for line in existing_vendor_loc
                            if line["VENDOR_LOC"] == loc["VENDOR_LOC"]
                            and line["EFFDT"] == loc["EFFDT"]
                        ),
                        None,
                    )

                    if existing_loc:
                        # Update the existing location
                        print(f"Updating existing location: {existing_loc} with {loc}")
                        for key, value in loc.items():
                            existing_loc[key] = value
                    else:
                        # Append loc to existing_vendor.VENDOR_LOC
                        # if it doesn't already exist
                        print(f"Appending new location: {loc}")
                        existing_vendor_loc.append(loc)

                existing_vendor.VENDOR_LOC = existing_vendor_loc

                # Process VENDOR_ADDR
                existing_vendor_addr = (
                    existing_vendor.VENDOR_ADDR if existing_vendor.VENDOR_ADDR else []
                )
                # print(f"Initial existing_vendor_addr: {existing_vendor_addr}")
                for addr in vendor_addr_data:
                    existing_addr = next(
                        (
                            a
                            for a in existing_vendor_addr
                            if a["ADDRESS_SEQ_NUM"] == addr["ADDRESS_SEQ_NUM"]
                        ),
                        None,
                    )

                    if existing_addr:
                        # Update the existing address
                        for key, value in addr.items():
                            existing_addr[key] = value
                    else:
                        # Append addr to existing_vendor.VENDOR_ADDR
                        #  if it doesn't already exist
                        existing_vendor_addr.append(addr)
                # print(f"Final existing_vendor_addr: {existing_vendor.VENDOR_ADDR}")

                db.query(model.PFGVendor).filter(
                    model.PFGVendor.SETID == vendor_data["SETID"],
                    model.PFGVendor.VENDOR_ID == vendor_data["VENDOR_ID"],
                ).update(
                    {
                        "VENDOR_LOC": existing_vendor.VENDOR_LOC,
                        "VENDOR_ADDR": existing_vendor.VENDOR_ADDR,
                    }
                )

                print(
                    "Updating VENDOR_LOC and VENDOR_ADDR "
                    + f"for existing vendor_id: {existing_vendor.VENDOR_ID}"
                )
                db.commit()
                db.refresh(existing_vendor)
            else:
                # Insert a new vendor record
                new_vendor = model.PFGVendor(**vendor_data)
                new_vendor.VENDOR_LOC = vendor_loc_data
                new_vendor.VENDOR_ADDR = vendor_addr_data
                print("Inserting a new vendor record: ", new_vendor)
                db.add(new_vendor)
                db.commit()
                db.refresh(new_vendor)

            # Add the VENDOR_ID to the processed set
            processed_vendor_ids.add(data.VENDOR_ID)

        # After the loop, fetch all updated/inserted vendor records
        processed_vendordata = (
            db.query(model.PFGVendor)
            .filter(model.PFGVendor.VENDOR_ID.in_(processed_vendor_ids))
            .all()
        )

        # Call SyncVendorMaster with the processed vendor data
        await SyncVendorMaster(db, processed_vendordata)
        return {"result": "Updated", "records": len(vendordata)}

    except SQLAlchemyError as e:
        print("error:", e)
        db.rollback()
        raise HTTPException(
            status_code=500, detail="An error occurred while processing the request."
        )

    finally:
        db.close()


# Update Account Master data
async def updateAccountMaster(Accountdata, db):

    try:
        response = []
        # Validate required fields
        for data in Accountdata:
            if not all([data.SETID, data.ACCOUNT, data.EFFDT]):
                raise HTTPException(
                    status_code=400, detail="required fields missing!!!"
                )

            account_data = data.dict()

            # Find existing department record
            existing_account = (
                db.query(model.PFGAccount)
                .filter(
                    model.PFGAccount.SETID == data.SETID,
                    model.PFGAccount.ACCOUNT == data.ACCOUNT,
                    model.PFGAccount.EFFDT == data.EFFDT,
                )
                .first()
            )

            if existing_account:
                # Update existing department record
                for key, value in account_data.items():
                    setattr(existing_account, key, value)
                db.commit()
                db.refresh(existing_account)
                response.append(existing_account)

            else:
                # Insert new department record
                new_account = model.PFGAccount(**account_data)
                db.add(new_account)
                db.commit()
                db.refresh(new_account)
                response.append(new_account)

        return {"result": "Updated", "records": response}
    except SQLAlchemyError:
        logger.error(f"Error: { traceback.format_exc()}")
        db.rollback()
        raise HTTPException(
            status_code=500, detail="An error occurred while processing the request."
        )

    finally:
        db.close()


async def updateProjectMaster(Projectdata, db):

    try:
        response = []
        # Validate required fields
        for data in Projectdata:
            if not all([data.BUSINESS_UNIT, data.PROJECT_ID]):
                raise HTTPException(
                    status_code=400, detail="required fields missing!!!"
                )

            project_data = data.dict()

            # Find existing department record
            existing_project = (
                db.query(model.PFGProject)
                .filter(
                    model.PFGProject.BUSINESS_UNIT == data.BUSINESS_UNIT,
                    model.PFGProject.PROJECT_ID == data.PROJECT_ID,
                )
                .first()
            )

            if existing_project:
                # Update existing department record
                for key, value in project_data.items():
                    setattr(existing_project, key, value)
                db.commit()
                db.refresh(existing_project)
                response.append(existing_project)

            else:
                # Insert new department record
                new_project = model.PFGProject(**project_data)
                db.add(new_project)
                db.commit()
                db.refresh(new_project)
                response.append(new_project)

        return {"result": "Updated", "records": response}
    except SQLAlchemyError:
        logger.error(f"Error: { traceback.format_exc()}")
        db.rollback()
        raise HTTPException(
            status_code=500, detail="An error occurred while processing the request."
        )

    finally:
        db.close()


async def updateProjectActivityMaster(ProjectActivitydata, db):

    try:
        response = []
        # Validate required fields
        for data in ProjectActivitydata:
            if not all([data.BUSINESS_UNIT, data.PROJECT_ID, data.ACTIVITY_ID]):
                raise HTTPException(
                    status_code=400, detail="required fields missing!!!"
                )

            project_activity_data = data.dict()

            # Find existing department record
            existing_project_activity = (
                db.query(model.PFGProjectActivity)
                .filter(
                    model.PFGProjectActivity.BUSINESS_UNIT == data.BUSINESS_UNIT,
                    model.PFGProjectActivity.PROJECT_ID == data.PROJECT_ID,
                    model.PFGProjectActivity.ACTIVITY_ID == data.ACTIVITY_ID,
                )
                .first()
            )

            if existing_project_activity:
                # Update existing department record
                for key, value in project_activity_data.items():
                    setattr(existing_project_activity, key, value)
                db.commit()
                db.refresh(existing_project_activity)
                response.append(existing_project_activity)

            else:
                # Insert new department record
                new_project_activity = model.PFGProjectActivity(**project_activity_data)
                db.add(new_project_activity)
                db.commit()
                db.refresh(new_project_activity)
                response.append(new_project_activity)

        return {"result": "Updated", "records": response}
    except SQLAlchemyError:
        logger.error(f"Error: { traceback.format_exc()}")
        db.rollback()
        raise HTTPException(
            status_code=500, detail="An error occurred while processing the request."
        )

    finally:
        db.close()


async def updateReceiptMaster(Receiptdata, db):
    try:
        for data in Receiptdata:
            # Validate required fields
            if not all([data.BUSINESS_UNIT, data.RECEIVER_ID]):
                raise HTTPException(
                    status_code=400, detail="Required fields missing in RECV_HDR!"
                )

            if data.RECV_LN_DISTRIB:
                loc = data.RECV_LN_DISTRIB
                if not all(
                    [
                        loc.BUSINESS_UNIT,
                        loc.RECEIVER_ID,
                        loc.RECV_LN_NBR,
                        loc.RECV_SHIP_SEQ_NBR,
                        loc.DISTRIB_LN_NUM,
                    ]
                ):
                    raise HTTPException(
                        status_code=400,
                        detail="Required fields missing in RECV_LN_DISTRIB!",
                    )

                if (
                    loc.BUSINESS_UNIT != data.BUSINESS_UNIT
                    or loc.RECEIVER_ID != data.RECEIVER_ID
                ):
                    raise HTTPException(
                        status_code=400,
                        detail="BUSINESS_UNIT and RECEIVER_ID"
                        + " must match between RECV_HDR and RECV_LN_DISTRIB.",
                    )

            # Flatten the combined PFGReceipt and RECV_LN_DISTRIB into one dictionary
            receipt_data = data.dict()
            distrib_data = data.RECV_LN_DISTRIB.dict() if data.RECV_LN_DISTRIB else {}

            # Merge RECV_LN_DISTRIB into receipt_data for a flattened structure
            receipt_data.update(distrib_data)

            # Find existing record by unique combination of
            # BUSINESS_UNIT, RECEIVER_ID,
            # RECV_LN_NBR, RECV_SHIP_SEQ_NBR, DISTRIB_LN_NUM
            existing_receipt = (
                db.query(model.PFGReceipt)
                .filter(
                    model.PFGReceipt.BUSINESS_UNIT == data.BUSINESS_UNIT,
                    model.PFGReceipt.RECEIVER_ID == data.RECEIVER_ID,
                    model.PFGReceipt.RECV_LN_NBR == data.RECV_LN_DISTRIB.RECV_LN_NBR,
                    model.PFGReceipt.RECV_SHIP_SEQ_NBR
                    == data.RECV_LN_DISTRIB.RECV_SHIP_SEQ_NBR,
                    model.PFGReceipt.DISTRIB_LINE_NUM
                    == data.RECV_LN_DISTRIB.DISTRIB_LN_NUM,
                )
                .first()
            )

            if existing_receipt:
                # Update existing record
                for key, value in receipt_data.items():
                    if hasattr(existing_receipt, key):  # Ensure the key exists in model
                        setattr(existing_receipt, key, value)
                db.commit()
                db.refresh(existing_receipt)

            else:
                # Insert new record
                valid_data = {
                    key: value
                    for key, value in receipt_data.items()
                    if hasattr(model.PFGReceipt, key)
                }
                new_receipt = model.PFGReceipt(**valid_data)
                db.add(new_receipt)
                db.commit()
                db.refresh(new_receipt)

        return {"result": "Receipt Master Data Updated"}

    except Exception:
        logger.error(f"Error: { traceback.format_exc()}")
        db.rollback()
        raise HTTPException(
            status_code=500, detail="An error occurred while processing the request."
        )

    finally:
        db.close()


async def SyncDepartmentMaster(db, Departmentdata):
    try:
        to_update = []
        to_insert = []
        for row in Departmentdata:
            # Check if the Department already exists in the
            # Department table based on DEPTID
            # (which corresponds to Department_ID)
            existing_department = (
                db.query(model.Department)
                .filter(model.Department.DEPTID == row.DEPTID)
                .first()
            )

            if existing_department:
                # Update existing vendor record
                existing_department.SETID = row.SETID
                existing_department.EFFDT = row.EFFDT
                existing_department.EFF_STATUS = row.EFF_STATUS
                existing_department.DESCR = row.DESCR
                existing_department.DESCRSHORT = row.DESCRSHORT
                existing_department.UpdatedOn = datetime.datetime.now()
                to_update.append(existing_department)

            else:
                # Insert new vendor record
                new_department = model.Department(
                    SETID=row.SETID,
                    DEPTID=row.DEPTID,
                    EFFDT=row.EFFDT,
                    EFF_STATUS=row.EFF_STATUS,
                    DESCR=row.DESCR,
                    DESCRSHORT=row.DESCRSHORT,
                    entityID=1,
                    entityBodyID=1,
                    CreatedOn=datetime.datetime.now(),
                )
                to_insert.append(new_department)
        if to_update:
            db.bulk_save_objects(to_update)
        if to_insert:
            db.bulk_save_objects(to_insert)

        db.commit()

        return {"result": "Synchronization completed"}

    except SQLAlchemyError:
        logger.error(f"Error: { traceback.format_exc()}")
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail="An error occurred while processing the synchronization.",
        )

    finally:
        db.close()


async def SyncVendorMaster(db, vendordata):
    try:
        # Get the maximum value of idVendor from the Vendor3 table
        latest_vendor_id = db.query(func.max(model.Vendor.idVendor)).scalar() or 0
        print("latest_vendor_id: ", latest_vendor_id)
        vendor_id = latest_vendor_id + 1

        new_vendors = []
        new_vendor_accounts = []
        update_vendors = []

        for row in vendordata:
            row_data = {
                "SETID": row.SETID,
                "VENDOR_ID": row.VENDOR_ID,
                "NAME1": row.NAME1,
                "NAME2": row.NAME2,
                "VENDOR_CLASS": row.VENDOR_CLASS,
                "VENDOR_STATUS": row.VENDOR_STATUS,
                "DEFAULT_LOC": row.DEFAULT_LOC,
                "VENDOR_LOC": row.VENDOR_LOC if row.VENDOR_LOC else [],
                "VENDOR_ADDR": row.VENDOR_ADDR if row.VENDOR_ADDR else [],
            }

            # # Concatenate NAME1 and NAME2
            # full_vendor_name = f"{row.NAME1.strip()} {row.NAME2.strip()}".strip()

            # Check if the vendor already exists in the
            # Vendor table based on VendorCode (which corresponds to VENDOR_ID)
            existing_vendor = (
                db.query(model.Vendor)
                .filter(model.Vendor.VendorCode == row.VENDOR_ID)
                .first()
            )

            # Assuming you're only working with
            # the first address in the VENDOR_ADDR list (adjust as necessary)
            primary_address = row.VENDOR_ADDR[0] if row.VENDOR_ADDR else {}

            # Concatenate the ADDRESS1, ADDRESS2, ADDRESS3, and ADDRESS4 fields
            full_address = " ".join(
                [
                    primary_address.get("ADDRESS1", "").strip(),
                    primary_address.get("ADDRESS2", "").strip(),
                    primary_address.get("ADDRESS3", "").strip(),
                    primary_address.get("ADDRESS4", "").strip(),
                ]
            ).strip()

            # Extract the CITY field
            city = primary_address.get("CITY", "").strip()

            if existing_vendor:
                # Update existing vendor record
                existing_vendor.VendorName = row.NAME1
                existing_vendor.vendorType = row.VENDOR_CLASS
                existing_vendor.Address = full_address
                existing_vendor.City = city
                existing_vendor.UpdatedOn = datetime.datetime.now()
                existing_vendor.miscellaneous = row_data
                update_vendors.append(existing_vendor)

            else:
                # Prepare new vendor record
                new_vendor = model.Vendor(
                    idVendor=vendor_id,
                    VendorName=row.NAME1,
                    VendorCode=row.VENDOR_ID,
                    vendorType=row.VENDOR_CLASS,
                    Address=full_address,
                    City=city,
                    createdBy=1,
                    entityID=1,
                    miscellaneous=row_data,
                    CreatedOn=datetime.datetime.now(),
                )
                new_vendors.append(new_vendor)

                # Prepare vendor account detail
                vendor_account = model.VendorAccount(
                    idVendorAccount=vendor_id,
                    vendorID=vendor_id,
                    Account=row.VENDOR_ID,
                    entityID=1,
                    entityBodyID=1,
                )
                new_vendor_accounts.append(vendor_account)

                # Increment idVendor for the next insertion
                vendor_id += 1

        # Bulk save or update in batches
        if new_vendors:
            db.bulk_save_objects(new_vendors)
        if new_vendor_accounts:
            db.bulk_save_objects(new_vendor_accounts)
        if update_vendors:
            db.bulk_save_objects(update_vendors)

        # Commit once after processing all data
        db.commit()

        return {"result": "Vendor table updation completed"}

    except SQLAlchemyError:
        logger.error(f"error: { traceback.format_exc()}")
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail="An error occurred while processing the synchronization.",
        )

    finally:
        db.close()


# CRUD function to process the invoice voucher and send it to peoplesoft
def processInvoiceVoucher(doc_id, db):
    try:
        # Fetch the invoice details from the voucherdata table
        voucherdata = (
            db.query(model.VoucherData)
            .filter(model.VoucherData.documentID == doc_id)
            .scalar()
        )
        if not voucherdata:
            raise HTTPException(status_code=404, detail="Voucherdata not found")

        # Call the function to get the base64 file and content type
        try:
            file_data = read_invoice_file_voucher(doc_id, db)
            if file_data and "result" in file_data:
                base64file = file_data["result"]["filepath"]

                # If filepath is a bytes object, decode it
                if isinstance(base64file, bytes):
                    base64file = base64file.decode("utf-8")
            else:
                base64file = "Error retrieving file: No result found in file data."
        except Exception as e:
            # Catch any error from the read_invoice_file
            # function and use the error message
            base64file = f"Error retrieving file: {str(e)}"

        # Continue processing the file
        # print(f"Filepath (Base64 Encoded or Error): {base64file}")

        request_payload = {
            "RequestBody": [
                {
                    "OF_VCHR_IMPORT_STG": [
                        {
                            "VCHR_HDR_STG": [
                                {
                                    "BUSINESS_UNIT": "MERCH",
                                    "VOUCHER_STYLE": "REG",
                                    "INVOICE_ID": voucherdata.Invoice_Id or "",
                                    "INVOICE_DT": voucherdata.Invoice_Dt or "",
                                    "VENDOR_SETID": voucherdata.Vendor_Setid or "",
                                    "VENDOR_ID": voucherdata.Vendor_ID or "",
                                    "ORIGIN": "IDP",
                                    "ACCOUNTING_DT": "",
                                    "VOUCHER_ID_RELATED": " ",
                                    "GROSS_AMT": (
                                        voucherdata.Gross_Amt
                                        if voucherdata.Gross_Amt
                                        else 0
                                    ),
                                    "SALETX_AMT": 0,
                                    "FREIGHT_AMT": 0,
                                    "MISC_AMT": 0,
                                    "PYMNT_TERMS_CD": "",
                                    "TXN_CURRENCY_CD": "",
                                    "VAT_ENTRD_AMT": 0,
                                    "VCHR_LINE_STG": [
                                        {
                                            "BUSINESS_UNIT": "MERCH",
                                            "VOUCHER_LINE_NUM": (
                                                voucherdata.Voucher_Line_num
                                                if voucherdata.Voucher_Line_num
                                                else 1
                                            ),
                                            "DESCR": " ",
                                            "MERCHANDISE_AMT": (
                                                voucherdata.Merchandise_Amt
                                                if voucherdata.Merchandise_Amt
                                                else 0
                                            ),
                                            "QTY_VCHR": 1,
                                            "UNIT_OF_MEASURE": "",
                                            "UNIT_PRICE": 0,
                                            "VAT_APPLICABILITY": "",
                                            "BUSINESS_UNIT_RECV": "OFGDS",
                                            "RECEIVER_ID": voucherdata.receiver_id
                                            or "",
                                            "RECV_LN_NBR": 1,
                                            "SHIPTO_ID": "",
                                            "VCHR_DIST_STG": [
                                                {
                                                    "BUSINESS_UNIT": "MERCH",
                                                    "VOUCHER_LINE_NUM": (
                                                        voucherdata.Voucher_Line_num
                                                        if voucherdata.Voucher_Line_num
                                                        else 1
                                                    ),
                                                    "DISTRIB_LINE_NUM": (
                                                        voucherdata.Distrib_Line_num
                                                        if voucherdata.Distrib_Line_num
                                                        else 1
                                                    ),
                                                    "BUSINESS_UNIT_GL": "OFG01",
                                                    "ACCOUNT": voucherdata.Account
                                                    or "",
                                                    "DEPTID": voucherdata.Deptid or "",
                                                    "OPERATING_UNIT": voucherdata.storenumber  # noqa: E501
                                                    or "",
                                                    "MERCHANDISE_AMT": (
                                                        voucherdata.Merchandise_Amt
                                                        if voucherdata.Merchandise_Amt
                                                        else 0
                                                    ),
                                                    "BUSINESS_UNIT_PC": " ",
                                                    "PROJECT_ID": " ",
                                                    "ACTIVITY_ID": " ",
                                                }
                                            ],
                                        }
                                    ],
                                }
                            ],
                            "INV_METADATA_STG": [
                                {
                                    "BUSINESS_UNIT": "MERCH",
                                    "INVOICE_ID": voucherdata.Invoice_Id,
                                    "INVOICE_DT": voucherdata.Invoice_Dt,
                                    "VENDOR_SETID": voucherdata.Vendor_Setid,
                                    "VENDOR_ID": voucherdata.Vendor_ID,
                                    "IMAGE_NBR": 1,
                                    "FILE_NAME": voucherdata.File_Name,
                                    "base64file": base64file,
                                }
                            ],
                        }
                    ]
                }
            ]
        }
        # print(request_payload)
        # Make a POST request to the external API endpoint
        api_url = settings.erp_invoice_import_endpoint
        headers = {"Content-Type": "application/json"}
        username = settings.erp_user
        password = settings.erp_password
        responsedata = {}
        try:
            # Make the POST request with basic authentication
            response = requests.post(
                api_url,
                json=request_payload,
                headers=headers,
                auth=(username, password),
                timeout=60,  # Set a timeout of 60 seconds
            )
            response.raise_for_status()
            # Raises an HTTPError if the response was unsuccessful
            # Log full response details
            print("Response Status: ", response.status_code)
            print("Response Headers: ", response.headers)
            print("Response Content: ", response.content.decode())  # Full content

            # Check for success
            if response.status_code == 200:

                responsedata = {"message": "Success", "data": response.json()}

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {traceback.format_exc()}")
            print("Response content:", response.content.decode())
            responsedata = {"message": str(e), "data": response.json()}

    except Exception:
        responsedata = {
            "message": "InternalError",
            "data": {"Http Response": "500", "Status": "Fail"},
        }
        logger.error(f"Error: { traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing invoice voucher: {str(traceback.format_exc())}",
        )

    return responsedata


def updateInvoiceStatus(doc_id, db):
    try:
        voucherdata = (
            db.query(model.VoucherData)
            .filter(model.VoucherData.documentID == doc_id)
            .scalar()
        )
        if not voucherdata:
            raise HTTPException(status_code=404, detail="Voucherdata not found")
        invoice_status_payload = {
            "RequestBody": {
                "INV_STAT_RQST": {
                    "BUSINESS_UNIT": voucherdata.Business_unit,
                    "INVOICE_ID": voucherdata.Invoice_Id,
                    "INVOICE_DT": voucherdata.Invoice_Dt,
                    "VENDOR_SETID": voucherdata.Vendor_Setid,
                    "VENDOR_ID": voucherdata.Vendor_ID,
                }
            }
        }
        # Make a POST request to the external API endpoint
        api_url = settings.erp_invoice_status_endpoint
        headers = {"Content-Type": "application/json"}
        username = settings.erp_user
        password = settings.erp_password

        response = (
            None  # Initialize response to avoid 'referenced before assignment' error
        )

        try:
            # Make the POST request with basic authentication
            response = requests.post(
                api_url,
                json=invoice_status_payload,
                headers=headers,
                auth=(username, password),
                timeout=60,  # Set a timeout of 60 seconds
            )
            response.raise_for_status()
            # Raises an HTTPError if the response was unsuccessful
            print("Response Status: ", response.status_code)
            # Check for success
            if response.status_code == 200:

                invoice_data = response.json()  # Parse the response JSON data
                entry_status = invoice_data.get(
                    "ENTRY_STATUS"
                )  # Get the ENTRY_STATUS field
                # voucher_id = invoice_data.get("VOUCHER_ID")  # TODO: Unused variable
                # Set the documentstatusid based on the ENTRY_STATUS value

                if entry_status == "NF":
                    documentstatusid = 30
                elif entry_status == "QCK":
                    documentstatusid = 27
                elif entry_status == "P":
                    documentstatusid = 29
                elif entry_status == "R":
                    documentstatusid = 28
                else:
                    documentstatusid = None  # Default if ENTRY_STATUS is not recognized

                # Now update the documentstatusid in the document table
                if documentstatusid is not None:
                    # # Assuming 'doc_id'
                    # is the identifier of the document you want to update
                    db.query(model.Document).filter(
                        model.Document.documentStatusID == doc_id
                    ).update({model.Document.documentStatusID: documentstatusid})
                    db.commit()  # Commit the transaction to save the changes
                    print("DocumentStatusID: ", documentstatusid)
                invoice_status = {"message": "Success", "data": response.json()}
            else:
                # Return a meaningful message if the status code is not 200
                invoice_status = {
                    "message": "Failed",
                    "status_code": response.status_code,
                    "details": response.content.decode(),
                }
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {traceback.format_exc()}")
            if response:
                return {
                    "message": "HTTP error occurred",
                    "status_code": response.status_code,
                    "details": response.content.decode(),
                }
            else:
                return {
                    "message": "HTTP error occurred, no response",
                    "details": str(e),
                }

    except Exception:
        logger.error(f"Error: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing invoice voucher: {str(traceback.format_exc())}",
        )
    return invoice_status


def read_invoice_file_voucher(inv_id, db):
    try:
        content_type = "application/pdf"
        # max_size = 5 * 1024 * 1024  # 5 MB in bytes

        # getting invoice data for later operation
        invdat = (
            db.query(model.Document)
            .options(load_only("docPath", "vendorAccountID"))
            .filter_by(idDocument=inv_id)
            .one()
        )

        # check if file path is present and give base64 coded image url
        if invdat.docPath:
            try:
                # Get the Blob service client
                fr_data = (
                    db.query(model.FRConfiguration)
                    .options(
                        load_only(
                            "ConnectionString", "ContainerName", "ServiceContainerName"
                        )
                    )
                    .filter_by(idCustomer=1)
                    .one()
                )
                account_url = (
                    f"https://{settings.storage_account_name}.blob.core.windows.net"
                )
                blob_service_client = BlobServiceClient(
                    account_url=account_url, credential=get_credential()
                )

                if invdat.vendorAccountID:
                    blob_client = blob_service_client.get_blob_client(
                        container=fr_data.ContainerName, blob=invdat.docPath
                    )

                # Get file properties to check the size
                # properties = blob_client.get_blob_properties()
                # file_size = properties.size

                # # Check if the file size is larger than 5 MB
                # if file_size > max_size:
                #     return {
                #         "result": "File size is more than 5MB",
                #         "file_size": f"{file_size / (1024 * 1024):.2f} MB",
                #     }

                # If the file size is within the limit, proceed to read and encode
                filetype = os.path.splitext(invdat.docPath)[1].lower()
                if filetype == ".png":
                    content_type = "image/png"
                elif filetype == ".jpg" or filetype == ".jpeg":
                    content_type = "image/jpg"
                else:
                    content_type = "application/pdf"

                # Download and encode the file as base64
                file_data = blob_client.download_blob().readall()
                invdat.docPath = base64.b64encode(file_data)

            except Exception:
                invdat.docPath = ""

        return {"result": {"filepath": invdat.docPath, "content_type": content_type}}

    except Exception:
        logger.error(f"Error reading invoice file: {traceback.format_exc()}")
        return Response(status_code=500, headers={"codeError": "Server Error"})
    finally:
        db.close()


def newbulkupdateInvoiceStatus(db):
    try:
        userID = 1
        # db = next(get_db())
        # Batch size for processing
        batch_size = 100  # Define a reasonable batch size

        # Fetch all document IDs with status id 7 (Sent to Peoplesoft) in batches
        doc_query = db.query(model.Document.idDocument).filter(
            model.Document.documentStatusID == 7
        )

        total_docs = doc_query.count()  # Total number of documents to process
        logger.info(f"Total documents to process: {total_docs}")

        # API credentials
        api_url = settings.erp_invoice_status_endpoint
        headers = {"Content-Type": "application/json"}
        auth = (settings.erp_user, settings.erp_password)

        # Process in batches
        for start in range(0, total_docs, batch_size):
            doc_ids = doc_query.offset(start).limit(batch_size).all()

            # Fetch voucher data for each document in the batch
            voucher_data_list = (
                db.query(model.VoucherData)
                .filter(
                    model.VoucherData.documentID.in_([doc_id[0] for doc_id in doc_ids])
                )
                .all()
            )

            # Prepare payloads and make API requests
            updates = []
            for voucherdata, doc_id in zip(voucher_data_list, doc_ids):
                dmsg = None  # Initialize dmsg to ensure it's defined
                # Prepare the payload for the API request
                invoice_status_payload = {
                    "RequestBody": {
                        "INV_STAT_RQST": {
                            "BUSINESS_UNIT": "MERCH",
                            "INVOICE_ID": voucherdata.Invoice_Id,
                            "INVOICE_DT": voucherdata.Invoice_Dt,
                            "VENDOR_SETID": voucherdata.Vendor_Setid,
                            "VENDOR_ID": voucherdata.Vendor_ID,
                        }
                    }
                }

                try:
                    # Make a POST request to the external API
                    response = requests.post(
                        api_url,
                        json=invoice_status_payload,
                        headers=headers,
                        auth=auth,
                        timeout=60,  # Set a timeout of 60 seconds
                    )
                    response.raise_for_status()  # Raise an exception for HTTP errors
                    logger.info(response.json())
                    # Process the response if the status code is 200
                    if response.status_code == 200:
                        invoice_data = response.json()
                        entry_status = invoice_data.get("ENTRY_STATUS")
                        voucher_id = invoice_data.get("VOUCHER_ID")

                        # Determine the new document status based on ENTRY_STATUS
                        documentstatusid = None
                        if entry_status == "STG":
                            documentstatusid = 7
                            docsubstatusid = 43
                            # Skip updating if entry_status is "STG"
                            # because the status is already 7
                            # continue
                        elif entry_status == "QCK":
                            documentstatusid = 27
                            docsubstatusid = 114
                            dmsg = InvoiceVoucherSchema.QUICK_INVOICE
                        elif entry_status == "R":
                            documentstatusid = 28
                            docsubstatusid = 115
                            dmsg = InvoiceVoucherSchema.RECYCLED_INVOICE
                        elif entry_status == "P":
                            documentstatusid = 29
                            docsubstatusid = 116
                            dmsg = InvoiceVoucherSchema.VOUCHER_CREATED
                        elif entry_status == "NF":
                            documentstatusid = 30
                            docsubstatusid = 117
                            dmsg = InvoiceVoucherSchema.VOUCHER_NOT_FOUND

                        # If there's a valid document status update,
                        # add it to the bulk update list
                        if documentstatusid:
                            updates.append(
                                {
                                    "idDocument": doc_id[0],
                                    "documentStatusID": documentstatusid,
                                    "documentsubstatusID": docsubstatusid,
                                    "voucher_id": voucher_id,
                                }
                            )

                except requests.exceptions.RequestException as e:
                    # Log the error and skip this document,
                    # but don't interrupt the batch
                    logger.error(f"Error for doc_id {doc_id[0]}: {str(e)}")

            try:
                # Perform bulk database update for the batch
                if updates:
                    db.bulk_update_mappings(model.Document, updates)
                    db.commit()  # Commit the changes for this batch

                logger.info(f"Processed batch {start} to {start + batch_size}")
            except Exception:
                logger.error(f"Error: {traceback.format_exc()}")

            try:

                update_docHistory(doc_id[0], userID, documentstatusid, dmsg, db)
            except Exception as err:
                dmsg = InvoiceVoucherSchema.FAILURE_COMMON.format_message(err)
                logger.error(f"Error while update dochistlog: {traceback.format_exc()}")

        return {"message": "Bulk update completed successfully"}

    except Exception as e:
        logger.error(f"Error: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500, detail=f"Error updating invoice status: {str(e)}"
        )


def bulkProcessVoucherData(db):
    try:
        userID = 1
        # db = next(get_db())
        # Batch size for processing
        batch_size = 100  # Define a reasonable batch size
        # Fetch all document IDs with status id 7 (Sent to Peoplesoft) in batches
        doc_query = db.query(model.Document.idDocument).filter(
            model.Document.documentStatusID == 7
        )

        total_docs = doc_query.count()  # Total number of documents to process
        logger.info(f"Total documents to process: {total_docs}")
        # Process in batches
        for start in range(0, total_docs, batch_size):
            doc_ids = doc_query.offset(start).limit(batch_size).all()
        for (docID,) in doc_ids:
            try:
                resp = processInvoiceVoucher(docID, db)
                try:
                    if "data" in resp:
                        if "Http Response" in resp["data"]:
                            RespCode = resp["data"]["Http Response"]
                            if resp["data"]["Http Response"].isdigit():
                                RespCodeInt = int(RespCode)
                                if RespCodeInt == 201:
                                    dmsg = (
                                        InvoiceVoucherSchema.SUCCESS_STAGED  # noqa: E501
                                    )
                                    docStatus = 7
                                    docSubStatus = 43

                                elif RespCodeInt == 400:
                                    dmsg = (
                                        InvoiceVoucherSchema.FAILURE_IICS  # noqa: E501
                                    )
                                    docStatus = 21
                                    docSubStatus = 108

                                elif RespCodeInt == 406:
                                    dmsg = (
                                        InvoiceVoucherSchema.FAILURE_INVOICE  # noqa: E501
                                    )
                                    docStatus = 21
                                    docSubStatus = 109

                                elif RespCodeInt == 422:
                                    dmsg = (
                                        InvoiceVoucherSchema.FAILURE_PEOPLESOFT  # noqa: E501
                                    )
                                    docStatus = 21
                                    docSubStatus = 110

                                elif RespCodeInt == 424:
                                    dmsg = (
                                        InvoiceVoucherSchema.FAILURE_FILE_ATTACHMENT  # noqa: E501
                                    )
                                    docStatus = 21
                                    docSubStatus = 111

                                elif RespCodeInt == 500:
                                    dmsg = (
                                        InvoiceVoucherSchema.INTERNAL_SERVER_ERROR  # noqa: E501
                                    )
                                    docStatus = 21
                                    docSubStatus = 53
                                else:
                                    dmsg = (
                                        InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                                    )
                                    docStatus = 21
                                    docSubStatus = 112
                            else:
                                dmsg = (
                                    InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                                )
                                docStatus = 21
                                docSubStatus = 112
                        else:
                            dmsg = (
                                InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                            )
                            docStatus = 21
                            docSubStatus = 112
                    else:
                        dmsg = (
                            InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                        )
                        docStatus = 21
                        docSubStatus = 112
                except Exception as err:
                    logger.info(f"PopleSoftResponseError: {err}")
                    dmsg = InvoiceVoucherSchema.FAILURE_COMMON.format_message(  # noqa: E501
                        err
                    )
                    docStatus = 21
                    docSubStatus = 112

                try:
                    logger.info(f"Updating the document status for doc_id:{docID}")
                    db.query(model.Document).filter(
                        model.Document.idDocument == docID
                    ).update(
                        {
                            model.Document.documentStatusID: docStatus,
                            model.Document.documentsubstatusID: docSubStatus,  # noqa: E501
                        }
                    )
                    db.commit()
                except Exception as err:
                    logger.info(f"ErrorUpdatingPostingData: {err}")
                try:
                    # userID = 1
                    update_docHistory(docID, userID, docStatus, dmsg, db)
                except Exception as e:
                    logger.error(f"pfg_sync 501: {str(e)}")
            except Exception as e:
                print(
                    "Error in ProcessInvoiceVoucher fun(): ",
                    traceback.format_exc(),
                )
                logger.info(f"PopleSoftResponseError: {e}")
                dmsg = InvoiceVoucherSchema.FAILURE_COMMON.format_message(e)
                docStatus = 21
                docSubStatus = 112

                try:
                    db.query(model.Document).filter(
                        model.Document.idDocument == docID
                    ).update(
                        {
                            model.Document.documentStatusID: docStatus,
                            model.Document.documentsubstatusID: docSubStatus,  # noqa: E501
                        }
                    )
                    db.commit()
                except Exception as err:
                    logger.info(f"ErrorUpdatingPostingData 156: {err}")
                try:
                    documentstatus = 21
                    update_docHistory(docID, userID, documentstatus, dmsg, db)
                except Exception as e:
                    logger.error(f"ErrorUpdatingDocHistory 163: {str(e)}")
    except Exception as e:
        logger.error(f"Error in schedule IDP to Peoplesoft : {str(e)}")

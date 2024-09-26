import datetime
import traceback

import model
import requests
from core.config import settings
from fastapi import HTTPException, Response
from schemas.ERPIntegrationSchema import InvoiceResponseItem
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError


async def getDepartmentMaster(db):

    try:
        return db.query(model.PFGDepartment).all()
    except Exception as e:
        return Response(status_code=500, content=str(e))
    finally:
        db.close()


async def getStoreMaster(db):

    try:
        return db.query(model.PFGStore).all()
    except Exception as e:
        return Response(status_code=500, content=str(e))
    finally:
        db.close()


async def getAccountMaster(db):

    try:
        return db.query(model.PFGAccount).all()
    except Exception as e:
        return Response(status_code=500, content=str(e))
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

    except Exception as e:
        return Response(status_code=500, content=str(e))

    finally:
        db.close()


async def getProjectMaster(db):

    try:
        return db.query(model.PFGProject).all()
    except Exception as e:
        return Response(status_code=500, content=str(e))
    finally:
        db.close()


async def getProjectActivityMaster(db):

    try:
        return db.query(model.PFGProjectActivity).all()
    except Exception as e:
        return Response(status_code=500, content=str(e))
    finally:
        db.close()


async def getReceiptMaster(db):

    try:
        return db.query(model.PFGReceipt).all()
    except Exception as e:
        return Response(status_code=500, content=str(e))
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
    except SQLAlchemyError as e:
        # applicationlogging.logs_to_table_storage('updateDepartmentMaster API', traceback.format_exc(),1)
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
    except SQLAlchemyError as e:
        print("error : ", e)
        # applicationlogging.logs_to_table_storage('updateStoreMaster API', traceback.format_exc(),1)
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
                            detail="SETID and VENDOR_ID must match between vendor_data and VENDOR_LOC.",
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
                            detail="SETID and VENDOR_ID must match between vendor_data and VENDOR_ADDR.",
                        )

            # Convert Pydantic model to dict and handle nested objects
            vendor_data = data.dict()
            # print("vendor_data_dict :",vendor_data )
            # Extract and remove VENDOR_LOC and VENDOR_ADDR for separate
            # processing
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
                        # Append loc to existing_vendor.VENDOR_LOC if it
                        # doesn't already exist
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
                        # Append addr to existing_vendor.VENDOR_ADDR if it
                        # doesn't already exist
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
                    f"Updating VENDOR_LOC and VENDOR_ADDR for existing vendor_id: {existing_vendor.VENDOR_ID}"
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
    except SQLAlchemyError as e:
        # applicationlogging.logs_to_table_storage('updateAccountMaster API', traceback.format_exc(), 1)
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
    except SQLAlchemyError as e:
        # applicationlogging.logs_to_table_storage('updateProjectMaster API', traceback.format_exc(),1)
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
    except SQLAlchemyError as e:
        print("Error: ", traceback.format_exc())
        # applicationlogging.logs_to_table_storage('updateProjectActivityMaster API', traceback.format_exc(),1)
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
                        detail="BUSINESS_UNIT and RECEIVER_ID must match between RECV_HDR and RECV_LN_DISTRIB.",
                    )

            # Flatten the combined PFGReceipt and RECV_LN_DISTRIB into one
            # dictionary
            receipt_data = data.dict()
            distrib_data = data.RECV_LN_DISTRIB.dict() if data.RECV_LN_DISTRIB else {}

            # Merge RECV_LN_DISTRIB into receipt_data for a flattened structure
            receipt_data.update(distrib_data)

            # Find existing record by unique combination of BUSINESS_UNIT,
            # RECEIVER_ID, RECV_LN_NBR, RECV_SHIP_SEQ_NBR, DISTRIB_LN_NUM
            existing_receipt = (
                db.query(model.PFGReceipt)
                .filter(
                    model.PFGReceipt.BUSINESS_UNIT == data.BUSINESS_UNIT,
                    model.PFGReceipt.RECEIVER_ID == data.RECEIVER_ID,
                    model.PFGReceipt.RECV_LN_NBR == data.RECV_LN_DISTRIB.RECV_LN_NBR,
                    model.PFGReceipt.RECV_SHIP_SEQ_NBR
                    == data.RECV_LN_DISTRIB.RECV_SHIP_SEQ_NBR,
                    model.PFGReceipt.DISTRIB_LN_NUM
                    == data.RECV_LN_DISTRIB.DISTRIB_LN_NUM,
                )
                .first()
            )

            if existing_receipt:
                # Update existing record
                for key, value in receipt_data.items():
                    setattr(existing_receipt, key, value)
                db.commit()
                db.refresh(existing_receipt)

            else:
                # Insert new record
                new_receipt = model.PFGReceipt(**receipt_data)
                db.add(new_receipt)
                db.commit()
                db.refresh(new_receipt)

        return {"result": "Receipt Master Data Updated"}

    except Exception as e:
        print("Error: ", traceback.format_exc())
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while processing the request.{e}",
        )

    finally:
        db.close()


async def SyncDepartmentMaster(db, Departmentdata):
    try:
        to_update = []
        to_insert = []
        for row in Departmentdata:
            # Check if the Department already exists in the Department table
            # based on DEPTID (which corresponds to Department_ID)
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

    except SQLAlchemyError as e:
        print("error:", traceback.format_exc())
        # applicationlogging.logs_to_table_storage('SyncDepartmentMaster API', traceback.format_exc(), 1)
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

            # Check if the vendor already exists in the Vendor table based on
            # VendorCode (which corresponds to VENDOR_ID)
            existing_vendor = (
                db.query(model.Vendor)
                .filter(model.Vendor.VendorCode == row.VENDOR_ID)
                .first()
            )

            # Assuming you're only working with the first address in the
            # VENDOR_ADDR list (adjust as necessary)
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

    except SQLAlchemyError as e:
        print("error:", traceback.format_exc())
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail="An error occurred while processing the synchronization.",
        )

    finally:
        db.close()


def updateInvoiceStatus(request_data):
    try:
        # Extract INV_STAT_RQST from the RequestBody
        req = request_data.RequestBody.INV_STAT_RQST  # Access through RequestBody

        # Simulating invoice processing and generating response data
        request_data = InvoiceResponseItem(
            BUSINESS_UNIT=req.BUSINESS_UNIT,
            INVOICE_ID=req.INVOICE_ID,
            INVOICE_DT=req.INVOICE_DT,
            VENDOR_SETID=req.VENDOR_SETID,
            VENDOR_ID=req.VENDOR_ID,
        )
        # Convert response_data (InvoiceResponseItem) to a dictionary
        request_data_dict = request_data.dict()  # This makes it JSON serializable
        # Make a POST request to the external API endpoint
        api_url = settings.erp_invoice_status_endpoint
        headers = {"Content-Type": "application/json"}
        username = settings.erp_user
        password = settings.erp_password

        try:
            # Make the POST request with basic authentication
            response = requests.post(
                api_url,
                json=request_data_dict,
                headers=headers,
                auth=(username, password),
            )
            response.raise_for_status()  # Raises an HTTPError if the response was unsuccessful
            print("Response Status: ", response.status_code)
            # Check for success
            if response.status_code == 200:
                return {"message": "Success", "data": response.json()}
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {traceback.format_exc()}")
            print("Response content:", response.content.decode())
        except requests.exceptions.RequestException as e:
            print(
                f"Error occurred while sending voucher data to the API: {traceback.format_exc()}"
            )
        except Exception as e:
            print(
                f"Unexpected error while sending voucher data to the API: {traceback.format_exc()}"
            )
    except Exception as e:
        print("Error: ", traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"Error processing invoice voucher: {str(traceback.format_exc())}",
        )
    return response.json()


# # Helper function to read the file and convert it to base64
# def convert_file_to_base64(file_path: str) -> str:
#     try:
#         with open(file_path, "rb") as file:
#             base64_encoded = base64.b64encode(file.read()).decode("utf-8")
#         return base64_encoded
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error reading file: {str(e)}")


# CRUD function to process the invoice voucher
def processInvoiceVoucher(db):
    try:
        # # Fetch the document details from the Document table having status sent to ERP
        # documents = db.query(model.Document.idDocument).filter(model.Document.documentStatusID == 2).all()

        # if not document:
        #     raise HTTPException(status_code=404, detail="No documents found with status id 2 >> Processing Document")

        # for document in documents:
        voucherdata = (
            db.query(model.VoucherData)
            .filter(model.VoucherData.documentID == 422)
            .scalar()
        )
        if not voucherdata:
            raise HTTPException(status_code=404, detail="Voucherdata not found")
        # # Convert the file to base64
        # base64file = convert_file_to_base64(file_path)

        request_payload = {
            "RequestBody": [
                {
                    "OF_VCHR_IMPORT_STG": [
                        {
                            "VCHR_HDR_STG": [
                                {
                                    "BUSINESS_UNIT": voucherdata.Business_unit or "",
                                    "VOUCHER_STYLE": "REG",
                                    "INVOICE_ID": voucherdata.Invoice_Id or "",
                                    "INVOICE_DT": voucherdata.Invoice_Dt or "",
                                    "VENDOR_SETID": voucherdata.Vendor_Setid or "",
                                    "VENDOR_ID": voucherdata.Vendor_ID or "",
                                    "ORIGIN": "IDP",
                                    "ACCOUNTING_DT": "2024-09-10",
                                    "VOUCHER_ID_RELATED": " ",
                                    "GROSS_AMT": (
                                        voucherdata.Gross_Amt
                                        if voucherdata.Gross_Amt
                                        else 0
                                    ),
                                    "SALETX_AMT": 0,
                                    "FREIGHT_AMT": 0,
                                    "MISC_AMT": 0,
                                    "PYMNT_TERMS_CD": "N30",
                                    "TXN_CURRENCY_CD": "CAD",
                                    "VAT_ENTRD_AMT": 81.320,
                                    "VCHR_LINE_STG": [
                                        {
                                            "BUSINESS_UNIT": voucherdata.Business_unit
                                            or "",
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
                                            "UNIT_OF_MEASURE": "EA",
                                            "UNIT_PRICE": 1870.86,
                                            "VAT_APPLICABILITY": "T",
                                            "BUSINESS_UNIT_RECV": "OFGDS",
                                            "RECEIVER_ID": "141942045",
                                            "RECV_LN_NBR": 1,
                                            "SHIPTO_ID": "5540",
                                            "VCHR_DIST_STG": [
                                                {
                                                    "BUSINESS_UNIT": voucherdata.Business_unit
                                                    or "",
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
                                                    "OPERATING_UNIT": "5540",
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
                                    "BUSINESS_UNIT": voucherdata.Business_unit,
                                    "INVOICE_ID": voucherdata.Invoice_Id,
                                    "INVOICE_DT": voucherdata.Invoice_Dt,
                                    "VENDOR_SETID": voucherdata.Vendor_Setid,
                                    "VENDOR_ID": voucherdata.Vendor_ID,
                                    "IMAGE_NBR": 1,
                                    "FILE_NAME": voucherdata.File_Name,
                                    "base64file": "+MDYxCiUlRU9GCg==",
                                }
                            ],
                        }
                    ]
                }
            ]
        }
        print(request_payload)
        # Make a POST request to the external API endpoint
        api_url = settings.erp_invoice_import_endpoint
        headers = {"Content-Type": "application/json"}
        username = settings.erp_user
        password = settings.erp_password

        try:
            # Make the POST request with basic authentication
            response = requests.post(
                api_url,
                json=request_payload,
                headers=headers,
                auth=(username, password),
            )
            response.raise_for_status()  # Raises an HTTPError if the response was unsuccessful
            # Log full response details
            print("Response Status: ", response.status_code)
            print("Response Headers: ", response.headers)
            print("Response Content: ", response.content.decode())  # Full content
            # Check for success
            if response.status_code == 200:
                return {"message": "Success", "data": response.json()}
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {traceback.format_exc()}")
            print("Response content:", response.content.decode())
        except requests.exceptions.RequestException as e:
            print(
                f"Error occurred while sending voucher data to the API: {traceback.format_exc()}"
            )
        except Exception as e:
            print(
                f"Unexpected error while sending voucher data to the API: {traceback.format_exc()}"
            )
    except Exception as e:
        print("Error: ", traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"Error processing invoice voucher: {str(traceback.format_exc())}",
        )

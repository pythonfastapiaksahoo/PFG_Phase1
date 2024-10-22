import re
import traceback

from fastapi import Response

import pfg_app.model as model
from pfg_app.core.azure_fr import get_fr_data
from pfg_app.FROps.preprocessing import fr_preprocessing
from pfg_app.logger_module import logger
from pfg_app.session.session import get_db

# from sqlalchemy.dialects.postgresql import insert


def getMetaData(vendorAccountID, db):
    try:
        metadata = (
            db.query(model.FRMetaData)
            .join(
                model.DocumentModel,
                model.FRMetaData.idInvoiceModel == model.DocumentModel.idDocumentModel,
            )
            .filter(model.DocumentModel.idVendorAccount == vendorAccountID)
            .first()
        )
        return metadata
    except Exception:
        logger.error(f"{traceback.format_exc()}")
        return None


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
        db.rollback()
        return Response(
            status_code=500, headers={"DB Error": "Failed to get OCR parameters"}
        )


def date_cnv(doc_date, date_format):
    # clean date and convert to "yyyy-mm-dd"

    get_date = {
        "jan": "01",
        "feb": "02",
        "mar": "03",
        "apr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "aug": "08",
        "sep": "09",
        "oct": "10",
        "nov": "11",
        "dec": "12",
    }

    date_status = 0
    req_date = doc_date

    try:
        # Handling various date formats using regex
        if date_format in [
            "mm.dd.yyyy",
            "mm-dd-yyyy",
            "mm/dd/yyyy",
            "mm dd yyyy",
            "mm.dd.yy",
            "mm/dd/yy",
            "mmm-dd-yyyy",
            "mmm dd yyyy",
        ]:
            doc_dt_slt = re.findall(r"\d+", doc_date)
            if len(doc_dt_slt) == 3:
                mm, dd, yy = doc_dt_slt
                dd = dd.zfill(2)
                yy = "20" + yy if len(yy) == 2 else yy
                mm = mm.zfill(2)
                req_date = f"{yy}-{mm}-{dd}"
                date_status = 1
            # Handle cases where the month is abbreviated (e.g., "jan")
            elif len(doc_dt_slt) == 2 and doc_date[:3].lower() in get_date:
                dd, yy = doc_dt_slt
                mm = get_date[doc_date[:3].lower()]
                dd = dd.zfill(2)
                yy = "20" + yy if len(yy) == 2 else yy
                req_date = f"{yy}-{mm}-{dd}"
                date_status = 1

        elif date_format in [
            "dd-mm-yy",
            "dd.mm.yy",
            "dd.mm.yyyy",
            "dd-mm-yyyy",
            "dd mm yyyy",
            "dd/mmm/yyyy",
            "dd/mmm/yy",
        ]:
            doc_dt_slt = re.findall(r"\d+", doc_date)
            if len(doc_dt_slt) == 3:
                dd, mm, yy = doc_dt_slt
                dd = dd.zfill(2)
                yy = "20" + yy if len(yy) == 2 else yy
                mm = mm.zfill(2)
                req_date = f"{yy}-{mm}-{dd}"
                date_status = 1
            # Handle cases with abbreviated months
            elif len(doc_dt_slt) == 2 and any(
                month in doc_date.lower() for month in get_date
            ):
                dd, yy = doc_dt_slt
                for month in get_date:
                    if month in doc_date.lower():
                        mm = get_date[month]
                        break
                dd = dd.zfill(2)
                yy = "20" + yy if len(yy) == 2 else yy
                req_date = f"{yy}-{mm}-{dd}"
                date_status = 1

        elif date_format in ["yyyy mm dd", "yyyy.mm.dd", "yyyy/mm/dd"]:
            doc_dt_slt = re.findall(r"\d+", doc_date)
            if len(doc_dt_slt) == 3:
                yy, mm, dd = doc_dt_slt
                dd = dd.zfill(2)
                mm = mm.zfill(2)
                req_date = f"{yy}-{mm}-{dd}"
                date_status = 1

    except Exception:
        date_status = 0
        req_date = doc_date

    return req_date, date_status


def clean_amount(amount_str):
    try:
        # Extract digits and periods, also handle commas
        cleaned_amount = re.findall(r"[\d,.]+", amount_str)
        if cleaned_amount:
            # Remove commas and convert to float
            cleaned_amount_str = cleaned_amount[0].replace(",", "")
            return round(float(cleaned_amount_str), 2)
    except Exception:
        return None
    return 0.0


def getModelData(vendorAccountID, db):
    try:
        modelDetails = []
        modelData = (
            db.query(model.DocumentModel)
            .filter(model.DocumentModel.idVendorAccount == vendorAccountID)
            .order_by(model.DocumentModel.UpdatedOn)
            .all()
        )
        # print("modelData line 403: ", modelData)
        reqModel = None
        for m in modelData:
            if m.modelID is not None and m.modelID != "":
                reqModel = m
                modelDetails.append(
                    {"IdDocumentModel": m.idDocumentModel, "modelName": m.modelName}
                )
        return reqModel, modelDetails
    except Exception:
        return None


def customModelCall(docID):
    # Custom Model Call for unidentified invoices:
    custcall_status = 1
    try:
        accepted_file_type = "application/pdf"
        file_size_accepted = 100
        db = next(get_db())
        customerID = 1

        docTab = (
            db.query(model.Document).filter(model.Document.idDocument == docID)
        ).first()

        file_path = docTab.docPath
        spltFileName = docTab.docPath
        entityID = docTab.entityID
        vendorAccountID = docTab.vendorAccountID
        InvoModelId = docTab.documentModelID

        configs = getOcrParameters(customerID, db)
        metadata = getMetaData(vendorAccountID, db)
        modelData, modelDetails = getModelData(vendorAccountID, db)

        # entityBodyID = 1
        file_size_accepted = 100
        accepted_file_type = metadata.InvoiceFormat.split(",")
        # date_format = metadata.DateFormat
        endpoint = configs.Endpoint
        DateFormat = metadata.DateFormat
        # mandatoryheadertags = configs.mandatoryheadertags
        # mandatorylinetags = configs.mandatorylinetags
        inv_model_id = modelData.modelID
        API_version = configs.ApiVersion

        filename = spltFileName.split("/")[-1]

        destination_container_name = configs.ContainerName
        API_version = configs.ApiVersion
        model_type = "custom"

        # preprocess the file and get binary data
        fr_preprocessing_status, fr_preprocessing_msg, input_data, ui_status = (
            fr_preprocessing(
                vendorAccountID,
                entityID,
                file_path,
                accepted_file_type,
                file_size_accepted,
                filename,
                spltFileName,
                destination_container_name,
                db,
            )
        )

        # DI call with trained model
        cst_model_status, cst_model_msg, cst_data, cst_status, isComposed, template = (
            get_fr_data(
                input_data,
                API_version,
                endpoint,
                model_type,
                inv_model_id,
            )
        )

        documenttagdef = (
            db.query(model.DocumentTagDef)
            .filter(model.DocumentTagDef.idDocumentModel == InvoModelId)
            .all()
        )

        hdr_tags = {}
        for hdrTags in documenttagdef:
            hdr_tags[hdrTags.TagLabel] = hdrTags.idDocumentTagDef

        documentlineitemsTag = (
            db.query(model.DocumentLineItemTags)
            .filter(model.DocumentLineItemTags.idDocumentModel == InvoModelId)
            .all()
        )

        line_tags = {}
        for LineTags in documentlineitemsTag:
            line_tags[LineTags.TagName] = LineTags.idDocumentLineItemTags

        docTg = {}
        rows = (
            db.query(model.DocumentData)
            .filter(model.DocumentData.documentID == docID)
            .all()
        )

        for row in rows:
            docTg[row.documentTagDefID] = row.Value
        docTg.keys()

        docLineTg = {}
        linerows = (
            db.query(model.DocumentLineItems)
            .filter(model.DocumentLineItems.documentID == docID)
            .all()
        )

        for Linerow in linerows:
            docLineTg[Linerow.lineItemtagID] = Linerow.Value
        docLineTg.keys()

        confThreshold = 90
        # custHdr = {}
        custHdr_data = []
        custHdrDt_update = []
        custHdrDt_insert = []
        custHdrDt = {}
        for hdr in cst_data[0]["documents"][0]["fields"]:
            print(hdr)
            tmp_rw = []
            tmp_rw.append(hdr)
            if "value_type" in cst_data[0]["documents"][0]["fields"][hdr]:
                if "value" or "content" in cst_data[0]["documents"][0]["fields"][hdr]:
                    if (
                        cst_data[0]["documents"][0]["fields"][hdr]["value_type"]
                        == "string"
                    ):
                        if "confidence" in cst_data[0]["documents"][0]["fields"][hdr]:
                            try:
                                cust_conf = round(
                                    float(
                                        cst_data[0]["documents"][0]["fields"][hdr][
                                            "confidence"
                                        ]
                                    )
                                    * 100,
                                    2,
                                )
                            except Exception:
                                cust_conf = 0.0
                        else:
                            cust_conf = 0.0

                        if cust_conf < confThreshold:
                            errorDesc = "Low confidence: " + str(cust_conf)
                            iserror = 1
                        else:
                            errorDesc = "Custom confidence: " + str(cust_conf)
                            iserror = 0

                        # tmp_rw.append(hdr)
                        hdr_rw = cst_data[0]["documents"][0]["fields"][hdr]
                        if "value" in hdr_rw and hdr_rw["value"] is not None:

                            val = cst_data[0]["documents"][0]["fields"][hdr]["value"]
                        elif "content" in hdr_rw and hdr_rw["content"] is not None:

                            val = cst_data[0]["documents"][0]["fields"][hdr]["content"]
                        else:
                            val = "NA"
                            iserror = 0
                            errorDesc = "The specified value could not be retrieved."
                        if hdr == "InvoiceDate":
                            val, status = date_cnv(val, DateFormat)
                            if status == 0:
                                iserror = 1
                                errorDesc = "Invalid Date Format"
                            else:
                                iserror = 0
                                errorDesc = "NA"
                        if hdr in [
                            "SubTotal",
                            "TotalTax",
                            "GST",
                            "PST",
                            "HST",
                            "LitterDeposit",
                        ]:
                            clnAnt = clean_amount(val)
                            if clnAnt is not None:
                                val = str(clnAnt)
                            else:
                                val = "NA"
                                iserror = 1
                                errorDesc = "Invalid amount"
                            # val = str(val)

                        tmp_rw.append(val)

                        tmp_rw.append(iserror)
                        tmp_rw.append(errorDesc)
                        if hdr in hdr_tags:
                            if hdr_tags[hdr] in docTg.keys():
                                custHdrDt_update.append(
                                    {
                                        "documentID": docID,
                                        "documentTagDefID": hdr_tags[hdr],
                                        "Value": val,
                                        "IsUpdated": 0,
                                        "isError": iserror,
                                        "ErrorDesc": errorDesc,
                                    }
                                )

                            else:
                                custHdrDt_insert.append(
                                    {
                                        "documentID": docID,
                                        "documentTagDefID": hdr_tags[hdr],
                                        "Value": val,
                                        "IsUpdated": 0,
                                        "isError": iserror,
                                        "ErrorDesc": errorDesc,
                                    }
                                )
                            custHdr_data.append(custHdrDt)

        for entry in custHdrDt_update:
            record_to_update = (
                db.query(model.DocumentData)
                .filter(
                    model.DocumentData.documentTagDefID == entry["documentTagDefID"],
                    model.DocumentData.documentID == docID,  # Ensure
                )
                .first()
            )

            if record_to_update:
                # print(
                #     f"Found record to update: {record_to_update.documentTagDefID}
                # for documentID {record_to_update.documentID}"
                # )

                record_to_update.Value = entry["Value"]
                record_to_update.IsUpdated = entry["IsUpdated"]
                record_to_update.isError = entry["isError"]
                record_to_update.ErrorDesc = entry["ErrorDesc"]

                # print(
                #     f"Updated record: {record_to_update.documentTagDefID},
                #  new value: {record_to_update.Value}"
                # )
            # else:
            #     logger.info(
            #         f"No record found for documentTagDefID
            # {entry['documentTagDefID']} and documentID {entry['documentID']}"
            #     )

        try:
            db.commit()
            # print("Transaction committed successfully.")
        except Exception:
            custcall_status = 0
            # print(f"Error committing transaction: {e}")
            db.rollback()  # Roll back the transaction if there's an error

        for entry in custHdrDt_insert:

            new_record = model.DocumentData(
                documentID=entry["documentID"],
                documentTagDefID=entry["documentTagDefID"],
                Value=entry["Value"],
                IsUpdated=entry["IsUpdated"],
                isError=entry["isError"],
                ErrorDesc=entry["ErrorDesc"],
            )

            db.add(new_record)

        try:
            db.commit()
            print("New records inserted successfully.")
        except Exception as e:
            custcall_status = 0
            print(f"Error inserting records: {e}")
            db.rollback()

        rw = 1
        tab_data = {}
        fr_rw = []
        # custLine_update = []
        custLine_insert = []

        # table data postprocessing:
        try:
            if "tab_1" in cst_data[0]["documents"][0]["fields"]:
                if "value" in cst_data[0]["documents"][0]["fields"]["tab_1"]:

                    for tb_rw in cst_data[0]["documents"][0]["fields"]["tab_1"][
                        "value"
                    ]:

                        iserror = 0
                        errorDesc = ""

                        if ("value_type" in tb_rw) and ("value" in tb_rw):
                            if tb_rw["value_type"] == "dictionary":
                                rw_val = []
                                for ky in tb_rw["value"]:

                                    if (
                                        "value" in tb_rw["value"][ky]
                                        and tb_rw["value"][ky]["value"] is not None
                                    ):
                                        val = tb_rw["value"][ky]["value"]
                                    elif (
                                        "content" in tb_rw["value"][ky]
                                        and tb_rw["value"][ky]["content"] is not None
                                    ):
                                        val = tb_rw["value"][ky]["value"]
                                    else:
                                        val = "NA"
                                        iserror = 1
                                        errorDesc = "The specified value could not\
                                              be retrieved."
                                    fr_rw.append(ky)
                                    fr_rw.append(val)
                                    fr_rw.append(iserror)
                                    fr_rw.append(errorDesc)
                                    rw_val.append(fr_rw)
                                    fr_rw = []
                                tab_data[rw] = rw_val
                                rw_val = []
                                rw = rw + 1

        except Exception:
            custcall_status = 0
            logger.error(f"{traceback.format_exc()}")

        # Delete the records from the DocumentLineItems table
        try:
            db.rollback()
            db.query(model.DocumentLineItems).filter(
                model.DocumentLineItems.documentID == docID,
            ).delete()

            try:
                db.commit()
                print("Records deleted successfully.")
            except Exception:
                custcall_status = 0
                db.rollback()  # Roll back if there is an error
        except Exception:
            custcall_status = 0
            db.rollback()

        # table data to insert
        try:
            for tb_rw in tab_data:
                print(tb_rw)
                custLine_insert = []
                for itm in tab_data[tb_rw]:
                    if itm[0] in line_tags.keys():
                        custLine_insert.append(
                            {
                                "documentID": docID,
                                "lineItemtagID": line_tags[itm[0]],
                                "Value": itm[1],
                                "IsUpdated": 0,
                                "isError": itm[2],
                                "ErrorDesc": itm[3],
                                "itemCode": tb_rw,
                                "invoice_itemcode": tb_rw,
                            }
                        )
                # #insert row by row:
                for Line_entry in custLine_insert:
                    print(Line_entry)
                    new_line = model.DocumentLineItems(
                        documentID=Line_entry["documentID"],
                        lineItemtagID=Line_entry["lineItemtagID"],
                        Value=Line_entry["Value"],
                        IsUpdated=Line_entry["IsUpdated"],
                        isError=Line_entry["isError"],
                        ErrorDesc=Line_entry["ErrorDesc"],
                        itemCode=Line_entry["itemCode"],
                        invoice_itemcode=Line_entry["invoice_itemcode"],
                    )

                    db.add(new_line)

                try:
                    db.commit()
                    print("New records inserted successfully.")
                except Exception as e:
                    print(f"Error inserting records: {e}")
                    db.rollback()

        except Exception:
            custcall_status = 0
            db.rollback()
            logger.error(f"{traceback.format_exc()}")
    except Exception:
        custcall_status = 0
        db.rollback()
        logger.error(f"{traceback.format_exc()}")

    if custcall_status == 1:
        documentstatus = 4
        documentSubstatus = 26
    else:
        documentstatus = 4
        documentSubstatus = 7

    try:
        db.query(model.Document).filter(model.Document.idDocument == docID).update(
            {
                model.Document.documentStatusID: documentstatus,  # noqa: E501
                model.Document.documentsubstatusID: documentSubstatus,  # noqa: E501
            }
        )
        db.commit()
    except Exception:
        logger.error(f"{traceback.format_exc()}")

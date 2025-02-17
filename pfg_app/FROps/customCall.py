#cust_call
import re
import traceback

from fastapi import Response

from pfg_app.crud.InvoiceCrud import update_docHistory
import pfg_app.model as model
from pfg_app import settings
from pfg_app.core.azure_fr import get_fr_data
from pfg_app.FROps.preprocessing import fr_preprocessing
from pfg_app.logger_module import logger
from pfg_app.session.session import get_db
from sqlalchemy import func

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
        logger.debug(traceback.format_exc())
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
        logger.debug(traceback.format_exc())
        return None
    return 0.0


def getModelData(vendorAccountID, db):
    try:
        modelDetails = []
        modelData = (
            db.query(model.DocumentModel)
            .filter(model.DocumentModel.idVendorAccount == vendorAccountID)
            .filter(model.DocumentModel.is_active == 1)
            .order_by(model.DocumentModel.UpdatedOn)
            .all()
        )
        # modelData = (
        #     db.query(model.DocumentModel)
        #     .filter(model.DocumentModel.idVendorAccount == vendorAccountID)
        #     .order_by(model.DocumentModel.UpdatedOn)
        #     .all()
        # )
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
        logger.debug(traceback.format_exc())
        return None


def customModelCall(docID,userID,db):
    status = 0
    try:
        # Custom Model Call for unidentified invoices:
        custcall_status = 1
        invoDate = ""
        invo_id = ""
        invo_total = "0"
        
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
            accepted_file_type = "application/pdf"
            # date_format = metadata.DateFormat
            endpoint = settings.form_recognizer_endpoint
            DateFormat = metadata.DateFormat
            # mandatoryheadertags = configs.mandatoryheadertags
            # mandatorylinetags = configs.mandatorylinetags
            inv_model_id = modelData.modelID
            # API_version = configs.ApiVersion

            filename = spltFileName.split("/")[-1]

            destination_container_name = configs.ContainerName
            API_version = settings.api_version
            model_type = "custom"
            try:

            # Fetch the corresponding idDocumentData first
                document_data = db.query(model.DocumentData).filter(
                    model.DocumentData.documentID == docID
                ).first()

                if document_data:

                    # Delete related records in DocumentUpdates
                    db.query(model.DocumentUpdates).filter(
                        model.DocumentUpdates.documentDataID == document_data.idDocumentData
                    ).delete()

                    # Now delete the record in DocumentData
                    db.query(model.DocumentData).filter(
                        model.DocumentData.idDocumentData == document_data.idDocumentData
                    ).delete()

                    # Commit the transaction
                    db.commit()


            except Exception:
                logger.error(traceback.format_exc())
                custcall_status = 0
                db.rollback()

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
                                    logger.debug(traceback.format_exc())
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
                                invoDate = val
                                if status == 0:
                                    iserror = 1
                                    errorDesc = "Invalid Date Format"
                                else:
                                    iserror = 0
                                    errorDesc = "NA"
                            
                            if hdr =="InvoiceId":
                                invo_id = val
                            if hdr in [
                                "InvoiceTotal",
                                "SubTotal",
                                "GST",
                                "HST",
                                "PST",
                                "HST",
                                "TotalTax",
                                "LitterDeposit",
                                "BottleDeposit",
                                "Discount",
                                "FreightCharges",
                                "Fuel surcharge",
                                "Credit_Card_Surcharge",
                                "Deposit",
                                "EcoFees",
                                "EnviroFees",
                                "OtherCharges",
                                "Other Credit Charges",
                                "ShipmentCharges",
                                "TotalDiscount",
                                "Usage Charges",
                            ]:
                                clnAnt = clean_amount(val)
                                if hdr=="InvoiceTotal":
                                    if clnAnt is not None:
                                        invo_total = str(clnAnt)
                                    else:
                                        invo_total = "0"
                                    invo_total = clnAnt
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

                    record_to_update.Value = entry["Value"]
                    record_to_update.IsUpdated = entry["IsUpdated"]
                    record_to_update.isError = entry["isError"]
                    record_to_update.ErrorDesc = entry["ErrorDesc"]

            try:
                db.commit()
                # print("Transaction committed successfully.")
            except Exception:
                logger.error(traceback.format_exc())
                custcall_status = 0
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
            except Exception:
                logger.error(traceback.format_exc())
                custcall_status = 0
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

                db.commit()

            except Exception:
                logger.error(traceback.format_exc())
                custcall_status = 0
                db.rollback()

            
            # do mandatory check on tags: 
            existing_tags = (
                db.query(model.DocumentTagDef.TagLabel)
                .filter(
                    model.DocumentTagDef.idDocumentModel == InvoModelId,
                    model.DocumentTagDef.TagLabel.in_(
                        ["Credit Identifier", "SubTotal", "GST"]
                    ),
                )
                .all()
            )

            # Extract existing tag labels from the result
            existing_tag_labels = {tag.TagLabel for tag in existing_tags}

            # Prepare missing tags
            missing_tags = []
            if "Credit Identifier" not in existing_tag_labels:
                missing_tags.append(
                    model.DocumentTagDef(
                        idDocumentModel=InvoModelId,
                        TagLabel="Credit Identifier",
                        CreatedOn=func.now(),
                    )
                )

            # if "SubTotal" not in existing_tag_labels:
            #     missing_tags.append(
            #         model.DocumentTagDef(
            #             idDocumentModel=InvoModelId,
            #             TagLabel="SubTotal",
            #             CreatedOn=func.now(),
            #         )
            #     )

            if "GST" not in existing_tag_labels:
                missing_tags.append(
                    model.DocumentTagDef(
                        idDocumentModel=InvoModelId,
                        TagLabel="GST",
                        CreatedOn=func.now(),
                    )
                )

            # Add missing tags if any
            if missing_tags:
                db.add_all(missing_tags)
                db.commit()

            # check for missing values in the invoice data:
            DocDtHdr = (
                db.query(model.DocumentData, model.DocumentTagDef)
                .join(
                    model.DocumentTagDef,
                    model.DocumentData.documentTagDefID
                    == model.DocumentTagDef.idDocumentTagDef,
                )
                .filter(model.DocumentTagDef.idDocumentModel == InvoModelId)
                .filter(model.DocumentData.documentID == docID)
                .all()
            )

            docHdrDt = {}
            tagNames = {}

            for document_data, document_tag_def in DocDtHdr:
                docHdrDt[document_tag_def.TagLabel] = document_data.Value
                tagNames[document_tag_def.TagLabel] = document_tag_def.idDocumentTagDef
            logger.info(f"customcall docHdrDt: {docHdrDt}")
            logger.info(f"customcall tagNames: {tagNames}")
            custHdrDt_insert_missing = []
            # if "SubTotal" not in docHdrDt:

            #     try:
            #         if "GST" in docHdrDt:
            #             subtotal = clean_amount(docHdrDt["GST"]) - clean_amount(invo_total)

            #         else:
            #             subtotal = invo_total 
            #         custHdrDt_insert_missing.append(
            #                                 {
            #                                     "documentID": docID,
            #                                     "documentTagDefID": hdr_tags["SubTotal"],
            #                                     "Value": subtotal,
            #                                     "IsUpdated": 0,
            #                                     "isError": 0,
            #                                     "ErrorDesc": "Defaulting to invoice total",
            #                                 }
            #                             )
            #     except Exception:
            #         logger.error(f"{traceback.format_exc()}")

            if "Credit Identifier" not in docHdrDt:
                try:
                    custHdrDt_insert_missing.append(
                                            {
                                                "documentID": docID,
                                                "documentTagDefID": hdr_tags["Credit Identifier"],
                                                "Value": "Invoice Document",
                                                "IsUpdated": 0,
                                                "isError": 0,
                                                "ErrorDesc": "Defaulting to Invoice Document",
                                            }
                                        )
                except Exception:
                    logger.error(f"{traceback.format_exc()}")

            if "GST" not in docHdrDt:
                try:
                    custHdrDt_insert_missing.append(
                                            {
                                                "documentID": docID,
                                                "documentTagDefID": hdr_tags["GST"],
                                                "Value": 0,
                                                "IsUpdated": 0,
                                                "isError": 0,
                                                "ErrorDesc": "Defaulting to 0",
                                            }
                                        )
                except Exception:
                    logger.error(f"{traceback.format_exc()}")

            # add missing values to the invoice data:
            logger.info(f"custHdrDt_insert_missing: {custHdrDt_insert_missing}")
            for entry in custHdrDt_insert_missing:
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
            except Exception as err:
                logger.debug(f"ErrorUpdatingPostingData: {err}")
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

                    db.commit()
            except Exception:
                custcall_status = 0
                db.rollback()
                logger.error(f"{traceback.format_exc()}")
        except Exception:
            custcall_status = 0
            db.rollback()
            logger.error(f"{traceback.format_exc()}")

        if custcall_status == 1:
            status = 1
            documentstatus = 4
            documentSubstatus = 26
            try:
                custModelCall_msg =  "Custom Model Call done"
                update_docHistory(
                    docID, userID, documentstatus,custModelCall_msg , db, documentSubstatus
                )
            except Exception:
                logger.debug(traceback.format_exc())
        else:
            documentstatus = 4
            documentSubstatus = 7

        try:
        #     invoDate = ""
        # invo_id = ""
            db.query(model.Document).filter(model.Document.idDocument == docID).update(
                {
                    model.Document.documentStatusID: documentstatus,  # noqa: E501
                    model.Document.documentsubstatusID: documentSubstatus,  # noqa: E501
                    model.Document.documentDate: invoDate,
                    model.Document.docheaderID: invo_id,
                    model.Document.totalAmount: invo_total,
                }
            )
            db.commit()

        except Exception:
            logger.error(f"{traceback.format_exc()}")

        
    except Exception as err:
        logger.error(f"{traceback.format_exc()}")
    return status
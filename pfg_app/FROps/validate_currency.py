from sqlalchemy import func
from sqlalchemy.orm import Session
from pfg_app.logger_module import logger
import pfg_app.model as model

# StampDataList = [
#     {
#         "StampFound": "Yes",
#         "MarkedDept": "Inventory",
#         "Confirmation": "138130054",
#         "ReceivingDate": "April 17 2024",
#         "Receiver": "108271",
#         "Department": "Restaurant",
#         "Store Number": "4553",
#         "VendorName": "1241567 BC LTD DBA FRESHBOX",  # 1241567 B.C. LTD DBA FRESHBOX
#         "Currency": "CAD",
#     }
# ]


def validate_currency(doc_id, currency, db: Session):
    # db = next(get_db())
    if not currency:
        logger.error("Currency is missing or empty in the OpenAI result. Treating it as NULL.")  # noqa: E501
        currency = ""  # Treat empty string as NULL

    # Get the vendor account ID associated with the document
    va_id = (
        db.query(model.Document.vendorAccountID)
        .filter(model.Document.idDocument == doc_id)
        .scalar()
    )

    # Get the vendor ID using the vendor account ID
    vendor_code = (
        db.query(model.VendorAccount.Account)
        .filter(model.VendorAccount.idVendorAccount == va_id)
        .scalar()
    )

    # Query to fetch the vendor and extract the CURRENCY_CD from the miscellaneous field
    actual_currency = (
        db.query(
            func.jsonb_extract_path_text(
                model.Vendor.miscellaneous, "VENDOR_LOC", "0", "CURRENCY_CD"
            )
        )
        .filter(model.Vendor.VendorCode == vendor_code)
        .scalar()
    )
    # Determine if currencies match or not
    # currencies_match = actual_currency == currency

    currencies_match = (
        actual_currency != "" and actual_currency == currency
    )
    # Get documentModelID for the given document
    doc_model_id = (
        db.query(model.Document.documentModelID)
        .filter(model.Document.idDocument == doc_id)
        .scalar()
    )

    # Fetch or create 'Currency' tag definition
    currency_tag_def = (
        db.query(model.DocumentTagDef)
        .filter(
            model.DocumentTagDef.idDocumentModel == doc_model_id,
            model.DocumentTagDef.TagLabel == "Currency",
        )
        .first()
    )

    if not currency_tag_def:
        currency_tag_def = model.DocumentTagDef(
            idDocumentModel=doc_model_id,
            TagLabel="Currency",
            CreatedOn=func.now(),
        )
        db.add(currency_tag_def)
        db.commit()  # Commit to get the ID of the newly inserted DocumentTagDef

    # Check if the corresponding entry in DocumentData exists
    document_data = (
        db.query(model.DocumentData)
        .filter(
            model.DocumentData.documentID == doc_id,
            model.DocumentData.documentTagDefID == currency_tag_def.idDocumentTagDef,
        )
        .first()
    )

    # Set the correct values based on currency match
    if document_data:
        document_data.Value = actual_currency if currencies_match else currency
        document_data.isError = 0 if currencies_match else 1
        document_data.ErrorDesc = (
            "Currency Matching" if currencies_match else "Currency Not Matching"
        )  # noqa: E501
    else:
        # Insert new DocumentData entry if it does not exist
        document_data = model.DocumentData(
            documentID=doc_id,
            documentTagDefID=currency_tag_def.idDocumentTagDef,
            Value=actual_currency if currencies_match else currency,
            isError=0 if currencies_match else 1,
            ErrorDesc=(
                "Currency Matching" if currencies_match else "Currency Not Matching"
            ),  # noqa: E501
            CreatedOn=func.now(),
        )
        db.add(document_data)

    # Commit the changes for DocumentData
    db.commit()

    if currencies_match:
        # Update the Units field in FRMetaData table
        fr_metadata = (
            db.query(model.FRMetaData)
            .filter(model.FRMetaData.idInvoiceModel == doc_model_id)
            .first()
        )
        if fr_metadata and fr_metadata.Units != actual_currency:
            fr_metadata.Units = actual_currency
        db.commit()

    # Return True if currencies match, otherwise return False
    return currencies_match

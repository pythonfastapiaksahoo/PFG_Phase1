from enum import Enum


class InvoiceVoucherSchema(str, Enum):
    """This class defines the schema for the Invoice Voucher data."""

    SUCCESS_STAGED = "Invoice Staged"
    QUICK_INVOICE = "Quick Invoice"
    RECYCLED_INVOICE = "Recycled Invoice"
    VOUCHER_CREATED = "Voucher Created"
    VOUCHER_NOT_FOUND = "Voucher Not Found"
    VOUCHER_CANCELLED = "Voucher Deleted"
    VOUCHER_SCHEDULED = "Voucher Scheduled"
    VOUCHER_COMPLETED = "Voucher Completed"
    VOUCHER_DEFAULTED = "Voucher Defaulted"
    VOUCHER_EDITED = "Voucher Edited"
    VOUCHER_REVIEWED = "Voucher Reviewed"
    VOUCHER_MODIFIED = "Voucher Modified"
    VOUCHER_OPEN = "Voucher Open"
    VOUCHER_TEMPLATE = "Voucher Template"
    FAILURE_IICS = "Failure: Data Error - IICS could not process the message"
    FAILURE_INVOICE = "Failure: Data Error - Invoice could not be staged"
    FAILURE_PEOPLESOFT = "Failure: PeopleSoft could not parse the json message"
    FAILURE_FILE_ATTACHMENT = "Failure: File Attachment could not loaded to File Server"
    INTERNAL_SERVER_ERROR = (
        "Internal Server Error - Could not connect to IICS or to PeopleSoft"
    )
    FAILURE_RESPONSE_UNDEFINED = "Failure: Response Undefined"
    SUCCESS_POSTED_IN_IRCS = "Success: Invoice Submitted for Batch"
    FAILURE_POST_IN_IRCS = "Failure: Error - IICS could not process the message"
    FAILURE_POST_IN_PEOPLESOFT = (
        "Failure: Error - PeopleSoft could not parse the json message"
    )
    FAILURE_POST_IN_FILE_ATTACHMENT = (
        "Failure: Error - File Attachment could not loaded to File Server"
    )
    FAILURE_POST_IN_INTERNAL_SERVER_ERROR = "Failure: Error - Internal Server Error \
        - Could not connect to IICS\
              or to PeopleSoft"
    FAILURE_POST_IN_RESPONSE_UNDEFINED = "Failure: Error - Response Undefined"

    FAILURE_COMMON = "Failure: PopleSoftResponseError "

    def format_message(self, message):
        return self.value.format(message)

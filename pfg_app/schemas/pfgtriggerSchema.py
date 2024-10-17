from enum import Enum


class InvoiceVoucherSchema(str, Enum):
    """This class defines the schema for the Invoice Voucher data."""

    SUCCESS_STAGED = "Success: Invoice Staged"
    QUICK_INVOICE = "Success: Quick Invoice"
    RECYCLED_INVOICE = "Success: Recycled Invoice"
    VOUCHER_CREATED = "Success: Voucher Created"
    VOUCHER_NOT_FOUND = "Failure: Voucher Not Found"
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

    FAILURE_COMMON = "Failure: Error - "

    def format_message(self, message):
        return self.value.format(message)

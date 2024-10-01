from typing import List, Literal, Optional

from pydantic import BaseModel, Field

# class PFGVendorAddr(BaseModel):
#     SETID: str = Field(max_length=5)
#     VENDOR_ID: str = Field(max_length=10)
#     ADDRESS_SEQ_NUM: int = Field(ge=1, le=999)
# # Address sequence number must be a positive integer
#     EFFDT: Optional[str] = Field(max_length=10)
#     EFF_STATUS: Optional[Literal['A', 'I']]
#     ADDRESS1: Optional[str] = Field(None, max_length=55)
#     ADDRESS2: Optional[str] = Field(None, max_length=55)
#     ADDRESS3: Optional[str] = Field(None, max_length=55)
#     ADDRESS4: Optional[str] = Field(None, max_length=55)
#     CITY: Optional[str] = Field(None, max_length=20)
#     STATE: Optional[str] = Field(None, max_length=6)
#     POSTAL: Optional[str] = Field(None, max_length=12)
#     COUNTRY: Optional[str] = Field(None, max_length=3)

# class PFGVendorLoc(BaseModel):
#     SETID: str = Field(max_length=5)
#     VENDOR_ID: str = Field(max_length=10)
#     VENDOR_LOC: str = Field(max_length=10)
#     EFFDT: str = Field(max_length=10)
#     EFF_STATUS: Optional[Literal['A', 'I']]
#     CURRENCY_CD: Optional[str] = Field(max_length=3)


class PFGVendor(BaseModel):
    SETID: str = Field(max_length=5)
    VENDOR_ID: Optional[str] = Field(max_length=10)
    NAME1: Optional[str] = Field(max_length=40)
    NAME2: Optional[str] = Field(max_length=40)
    VENDOR_CLASS: Optional[
        Literal[
            "0",
            "A",
            "B",
            "C",
            "D",
            "E",
            "F",
            "G",
            "H",
            "I",
            "J",
            "K",
            "L",
            "M",
            "N",
            "O",
            "P",
            "Q",
            "R",
            "S",
            "T",
            "U",
            "V",
            "W",
            "X",
            "Y",
            "Z",
        ]
    ]
    VENDOR_STATUS: Optional[Literal["A", "D", "E", "I", "X"]]
    DEFAULT_LOC: Optional[str] = Field(max_length=10)
    VENDOR_LOC: Optional[List[dict]] = None
    VENDOR_ADDR: Optional[List[dict]] = None


class PFGStore(BaseModel):
    SETID: str = Field(max_length=5)
    STORE: str = Field(max_length=10)
    EFFDT: str = Field(max_length=10)
    EFF_STATUS: Optional[Literal["A", "I"]]
    DESCR: Optional[str] = Field(max_length=50)
    DESCRSHORT: Optional[str] = Field(max_length=55)
    ADDRESS1: Optional[str] = Field(max_length=55)
    ADDRESS2: Optional[str] = Field(max_length=55)
    ADDRESS3: Optional[str] = Field(max_length=55)
    ADDRESS4: Optional[str] = Field(max_length=55)
    CITY: Optional[str] = Field(max_length=20)
    STATE: Optional[str] = Field(max_length=6)
    POSTAL: Optional[str] = Field(max_length=12)
    COUNTRY: Optional[str] = Field(max_length=3)


class PFGDepartment(BaseModel):
    SETID: str = Field(max_length=5)
    DEPTID: str = Field(max_length=10)
    EFFDT: str = Field(max_length=10)
    EFF_STATUS: Optional[Literal["A", "I"]]
    DESCR: Optional[str] = Field(max_length=50)
    DESCRSHORT: Optional[str] = Field(max_length=10)


class PFGAccount(BaseModel):
    SETID: str = Field(max_length=5)
    ACCOUNT: str = Field(max_length=10)
    EFFDT: str = Field(max_length=10)
    EFF_STATUS: Optional[Literal["A", "I"]]
    DESCR: Optional[str] = Field(max_length=30)
    DESCRSHORT: Optional[str] = Field(max_length=10)


class PFGProject(BaseModel):
    BUSINESS_UNIT: str = Field(max_length=5)
    PROJECT_ID: str = Field(max_length=15)
    EFF_STATUS: Optional[Literal["A", "I"]]
    DESCR: Optional[str] = Field(max_length=30)
    START_DT: Optional[str] = Field(max_length=10)
    END_DT: Optional[str] = Field(max_length=10)


class PFGProjectActivity(BaseModel):
    BUSINESS_UNIT: str = Field(max_length=5)
    PROJECT_ID: str = Field(max_length=15)
    ACTIVITY_ID: str = Field(max_length=15)
    EFF_STATUS: Optional[Literal["A", "I"]]
    DESCR: Optional[str] = Field(max_length=30)


# class PFGReceipt(BaseModel):
#     BUSINESS_UNIT: str = Field(max_length=5)
#     RECEIVER_ID: str = Field(max_length=15)
#     RECV_LN_NBR: int
#     RECV_SHIP_SEQ_NBR: int
#     DISTRIB_LN_NUM: int
#     MERCHANDISE_AMT: Optional[float]
#     ACCOUNT: Optional[str] = Field(max_length=10)
#     DEPTID : Optional[str] = Field(max_length=10)
#     LOCATION: Optional[str] = Field(max_length=10)


class RECV_LN_DISTRIB(BaseModel):
    BUSINESS_UNIT: str = Field(max_length=5)
    RECEIVER_ID: str = Field(max_length=10)
    RECV_LN_NBR: int
    RECV_SHIP_SEQ_NBR: int
    DISTRIB_LN_NUM: int
    MERCHANDISE_AMT: Optional[float]
    ACCOUNT: Optional[str] = Field(max_length=10)
    DEPTID: Optional[str] = Field(max_length=10)
    LOCATION: Optional[str] = Field(max_length=10)


class PFGReceipt(BaseModel):
    BUSINESS_UNIT: str = Field(max_length=5)
    RECEIVER_ID: str = Field(max_length=10)
    # BILL_OF_LADING: Optional[str] = Field(max_length=30)
    INVOICE_ID: Optional[str] = Field(max_length=30)
    RECEIPT_DT: Optional[str] = Field(format="date")
    SHIPTO_ID: Optional[str] = Field(max_length=10)
    VENDOR_SETID: Optional[str] = Field(max_length=5)
    VENDOR_ID: Optional[str] = Field(max_length=10)
    RECV_STATUS: Optional[str] = Field(
        max_length=1, enum=["C", "H", "M", "O", "P", "R", "X"]
    )
    RECV_LN_DISTRIB: Optional[RECV_LN_DISTRIB]


# Define the payload structure using Pydantic
class InvoiceRequest(BaseModel):
    BUSINESS_UNIT: str
    INVOICE_ID: str
    INVOICE_DT: str
    VENDOR_SETID: str
    VENDOR_ID: str


class RequestBody(BaseModel):
    INV_STAT_RQST: InvoiceRequest


class RequestPayload(BaseModel):
    RequestBody: RequestBody


class VchrDistStg(BaseModel):
    BUSINESS_UNIT: str
    VOUCHER_LINE_NUM: int
    DISTRIB_LINE_NUM: int
    BUSINESS_UNIT_GL: str
    ACCOUNT: str
    DEPTID: str
    OPERATING_UNIT: str
    MERCHANDISE_AMT: float
    BUSINESS_UNIT_PC: Optional[str] = None
    PROJECT_ID: Optional[str] = None
    ACTIVITY_ID: Optional[str] = None


class VchrLineStg(BaseModel):
    BUSINESS_UNIT: str
    VOUCHER_LINE_NUM: int
    DESCR: Optional[str] = None
    MERCHANDISE_AMT: float
    QTY_VCHR: int
    UNIT_OF_MEASURE: str
    UNIT_PRICE: float
    VAT_APPLICABILITY: str
    BUSINESS_UNIT_RECV: str
    RECEIVER_ID: str
    RECV_LN_NBR: int
    SHIPTO_ID: str
    VCHR_DIST_STG: List[VchrDistStg]


class VchrHdrStg(BaseModel):
    BUSINESS_UNIT: str
    VOUCHER_STYLE: str
    INVOICE_ID: str
    INVOICE_DT: str
    VENDOR_SETID: str
    VENDOR_ID: str
    ORIGIN: str
    ACCOUNTING_DT: str
    VOUCHER_ID_RELATED: Optional[str] = None
    GROSS_AMT: float
    SALETX_AMT: float
    FREIGHT_AMT: float
    MISC_AMT: float
    PYMNT_TERMS_CD: str
    TXN_CURRENCY_CD: str
    VAT_ENTRD_AMT: float
    VCHR_LINE_STG: List[VchrLineStg]


class InvMetadataStg(BaseModel):
    BUSINESS_UNIT: str
    INVOICE_ID: str
    INVOICE_DT: str
    VENDOR_SETID: str
    VENDOR_ID: str
    IMAGE_NBR: int
    FILE_NAME: str
    base64file: str


class OFVchrImportStg(BaseModel):
    VCHR_HDR_STG: List[VchrHdrStg]
    INV_METADATA_STG: List[InvMetadataStg]


class RequestBodyItem(BaseModel):
    OF_VCHR_IMPORT_STG: List[OFVchrImportStg]


class VchrImpRequestBody(BaseModel):
    RequestBody: List[RequestBodyItem]

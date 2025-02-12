from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel

class ProcessResponse(BaseModel):
    email_data: dict
    invoice_detail_list: list


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
    
    
class CorpMetadataCreate(BaseModel):
    synonyms_name: Optional[List[str]] = None
    synonyms_address: Optional[List[str]] = None
    dateformat: str
    
class CorpMetadataDelete(BaseModel):
    synonyms_name: Optional[List[str]] = None
    synonyms_address: Optional[List[str]] = None


class corpcolumnpos(BaseModel):
    id_tab_column: int
    column_pos: int
    is_active: int
    
class UpdateCorpInvoiceData(BaseModel):
    field: str
    OldValue: str
    NewValue: str

    
class UpdateCodinglineData(BaseModel):
    field: str
    OldValue: Union[str, int, float, Dict[str, Any], None]  # Can be string, number, or JSON
    NewValue: Union[str, int, float, Dict[str, Any], None]  # Can be string, number, or JSON
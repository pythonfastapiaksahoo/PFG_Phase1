from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, EmailStr


class InvoiceModel(BaseModel):
    idServiceAccount: Optional[int] = None
    idVendorAccount: Optional[int] = None


class NewInvoice(BaseModel):
    documentModelID: int
    idDocumentType: int
    entityID: int
    entityBodyID: int
    supplierAccountID: int
    vendorAccountID: int


class NewInvoiceData(BaseModel):
    documentTagDefID: int
    Value: str
    IsUpdated: int = 0
    stage: int


class NewInvoiceDataList(BaseModel):
    dataList: List[NewInvoiceData]


class InvoiceType(BaseModel):
    idDocumentType: int
    Name: str


class Tags(BaseModel):
    TagName: str
    TagDesc: str


class TagDef(BaseModel):
    """Tag label testing definition."""

    TagLabel: str
    NERActive: bool
    Xcord: str
    Ycord: str
    Width: str
    Height: str
    CreatedOn: Optional[datetime]
    UpdatedOn: Optional[datetime]
    idDocumentModel: Optional[int] = None


class LineItemDef(BaseModel):
    TagName: str
    TagDesc: str
    Xcord: str
    Ycord: str
    Width: str
    Height: str


class TableDef(BaseModel):
    """
    - Xcord & Ycord : origin X & Y cordinates of bounding box
    - Width & Height : Height and width of bounding box
    """

    Xcord: str
    Ycord: str
    Width: str
    Height: str


class tagDefList(BaseModel):
    tagList: List[TagDef]


class InvoiceLineItems(BaseModel):
    """Invoice Line items data."""

    lineItemtagID: int
    Value: str


class InvoiceLineItemsList(BaseModel):
    lineItemList: List[InvoiceLineItems]


class UpdateInvoiceStatus(BaseModel):
    InvoiceStatusID: int


class UpdateInvoiceStage(BaseModel):
    stage: int


class Response(BaseModel):
    result: str
    records: dict


class ResponseList(BaseModel):
    result: str
    records: list


class DocLineItems(BaseModel):
    documentID: int
    itemCode: int


class InvoiceUpdate(BaseModel):
    # invoiceDataID : int
    # invoiceLineItemID : Optional[int]
    IsActive: int
    OldValue: str
    NewValue: str


class UpdateServiceAccountInvoiceData(BaseModel):
    documentDataID: Optional[int] = None
    documentLineItemID: Optional[int] = None
    OldValue: str
    NewValue: str


class columnpos(BaseModel):
    idtabColumn: int
    ColumnPos: int
    isActive: int


class DocHistoryLog(BaseModel):
    documentdescription: Optional[str] = None


class BoundingBoxes(BaseModel):
    x: str
    y: str
    w: str
    h: str


class value(BaseModel):
    page: int
    text: str
    boundingBoxes: BoundingBoxes


class Labels(BaseModel):
    label: str
    key: str
    value: List[value]


class Tabels(BaseModel):
    col: str
    row: str
    value: List[value]


class tab_1(BaseModel):
    tab_1: List[Tabels]


class Other_tab_1(BaseModel):
    tab_1: Optional[List[Tabels]]


class InvoiceOnBoarding(BaseModel):
    """Model On-Boarding API body.

    - labels: Header data fields in the document
    - line_tables: Line item data
    - other_tables: Secondary Table data
    - VendorAccount/ServiceAccount: Vendor/service account number
    - ModelID: Form recogniser Model ID
    """

    Schema: str
    document: str
    labels: List[Labels]
    line_tables: Optional[tab_1]
    other_tables: Optional[Other_tab_1]
    labelingState: int
    VendorAccount: Optional[str]
    ServiceAccount: Optional[str]
    ModelID: str


class GrnData(BaseModel):
    idDocumentLineItems: int
    Value: str
    old_value: Optional[str] = None
    tagName: Optional[str] = None
    ErrorDesc: Optional[str] = None
    is_quantity: Optional[bool] = False


class GrnQuantity(BaseModel):
    line_id: int
    quantity: float


class SessionTime(BaseModel):
    session_status: bool
    client_address: str


class StatusData(BaseModel):
    documentStatusID: int
    documentsubstatusID: int


class pofliplinedetails(BaseModel):
    LineNumber: int
    PurchQty: float
    UnitPrice: float
    Name: str
    Quantity: float
    DiscAmount: float
    DiscPercent: float
    RemainPurchFinancial: float


# Process Vendor Uploaded Document Structure


class ProcessUploadedDocument(BaseModel):
    """This model is used for uploading document into blob after which OCR
    processing will be performed.

    Used currently for Whatsapp Bot Integration
    """

    vendorUserID: int
    source: str
    sender: EmailStr
    entityID: int
    document: str
    uploadType: Literal["url"]


class GrnDataUnitPriceValiDation(BaseModel):
    invoice_itemcode: int


class GRNApproveDoc(BaseModel):
    user_id: int
    grn_id: int


class GRNDownload(BaseModel):
    email: str
    option: Literal["All", "Pending"]


class StampDataUpdate(BaseModel):
    DEPTNAME: Optional[str] = None
    RECEIVING_DATE: Optional[str] = None
    CONFIRMATION_NUMBER: Optional[str] = None
    RECEIVER: Optional[str] = None
    SELECTED_DEPT: Optional[str] = None
    storenumber: Optional[str] = None


class UpdateStampData(BaseModel):
    stamptagname: str
    OldValue: str
    NewValue: str


class UpsertLineItemData(BaseModel):
    documentLineItemID: Optional[int] = None
    OldValue: Optional[str] = None
    NewValue: str
    lineItemTagID: Optional[int] = None
    itemCode: Optional[str] = None  


class DeleteLineItemData(BaseModel):
    documentLineItemID: int

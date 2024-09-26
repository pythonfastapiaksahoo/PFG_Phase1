from datetime import datetime
from typing import Optional, Union

from pydantic import BaseModel


class FrConfig(BaseModel):
    """Form Recogniser Configurations associated with a document template."""

    Endpoint: Optional[str]
    ConnectionString: Optional[str]
    Key1: Optional[str]
    Key2: Optional[str]
    ContainerName: Optional[str]
    SasToken: Optional[str]
    SasUrl: Optional[str]
    SasExpiry: Optional[datetime]
    ApiVersion: Optional[str]
    CallsPerMin: Optional[int]
    PagesPerMonth: Optional[int]
    EmailListenerInfo: Optional[dict]


class EmailListenerInfo(BaseModel):
    EmailListenerInfo: dict


class FrMetaData(BaseModel):
    """Meta Data associated with the document template in Serina."""

    idInvoiceModel: Optional[int]
    DateFormat: Optional[str]
    FolderPath: Optional[str]
    AccuracyOverall: Optional[str]
    AccuracyFeild: Optional[str]
    InvoiceFormat: Optional[str]
    Units: Optional[str]
    ruleID: Optional[int]
    TableLogic: Optional[str]
    ErrorThreshold: Optional[int]
    mandatorylinetags: Optional[str]
    mandatoryheadertags: Optional[str]
    batchmap: Optional[int]
    optionalheadertags: Optional[str]
    optionallinertags: Optional[str]
    erprule: Optional[int]
    vendorType: Optional[str]


class FrUpload(BaseModel):
    """Upload blob API body."""

    min_no: int
    max_no: int
    file_size_accepted: int
    accepted_file_type: str
    cnx_str: str
    cont_name: str
    local_path: str
    folderpath: str


class InvoiceModel(BaseModel):
    modelName: Optional[str] = None
    serviceproviderID: Optional[int] = None
    idVendorAccount: Optional[int] = None
    modelID: Optional[str] = None
    folderPath: Optional[str] = None
    modelStatus: Optional[int] = None
    labels: Optional[str] = None
    fields: Optional[str] = None
    training_result: Optional[str] = None


class FrReUpload(BaseModel):
    min_no: int
    max_no: int
    file_size_accepted: int
    accepted_file_type: str
    cnx_str: str
    cont_name: str
    local_path: str
    old_folder: str
    upload_type: str


class Entity(BaseModel):
    City: Optional[str]
    Country: Optional[str]
    EntityAddress: Optional[str]
    Synonyms: Optional[str]


class FrValidate(BaseModel):
    folderPath: str
    model_path: str
    fr_modelid: Optional[str] = None
    model_id: Optional[int] = None
    req_fields_accuracy: float
    req_model_accuracy: float
    mandatory_field_check: int
    mand_fld_list: str
    cnx_str: str
    cont_name: str
    VendorAccount: Union[int, None] = None
    ServiceAccount: Union[int, None] = None


class OCRLogs(BaseModel):
    documentId: int
    labelType: str
    predictedValue: str
    editedValue: str
    accuracy: str
    frModelID: str
    errorFlag: int


class ItemMapping(BaseModel):
    idDocumentModel: int
    itemCodePO: str
    itemCodeInvoice: str
    itemDescPO: str
    itemDescInvo: str


# class DefaultFieldsS(BaseModel):
#     Name: str
#     Type: str
#     Description: Optional[str]
#     Ismendatory: int

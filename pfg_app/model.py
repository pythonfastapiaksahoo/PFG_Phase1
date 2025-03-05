import datetime

from sqlalchemy import (
    JSON,
    TEXT,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    SmallInteger,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB

from pfg_app.session.session import Base

# from sqlalchemy.ext.automap import automap_base
# from sqlalchemy import MetaData

# metadata = MetaData()
# creating class to load Dataswitch table


class Dataswitch(Base):
    __tablename__ = "dataswitch"

    documentID = Column(Integer, primary_key=True, index=True)
    DocPrebuiltData = Column(JSON, nullable=True)
    DocCustData = Column(JSON, nullable=True)
    FilePathOld = Column(String(255), nullable=True)
    FilePathNew = Column(String(255), nullable=True)
    UserID = Column(String(45), nullable=True)
    CreatedON = Column(String(45), nullable=True)
    TranslatedHeaders = Column(JSON, nullable=True)
    TranslatedLines = Column(JSON, nullable=True)


class DocumentModel(Base):
    __tablename__ = "documentmodel"

    idDocumentModel = Column(BigInteger, primary_key=True, index=True)
    idServiceAccount = Column(
        Integer, ForeignKey("serviceaccount.idServiceAccount"), nullable=True
    )
    serviceproviderID = Column(
        Integer, ForeignKey("serviceprovider.idServiceProvider"), nullable=True
    )
    idVendorAccount = Column(Integer, ForeignKey("vendoraccount.idVendorAccount"))
    idDocumentType = Column(Integer, ForeignKey("documenttype.idDocumentType"))
    modelName = Column(String(100), nullable=True)
    modelStatus = Column(Integer, nullable=True)
    modelID = Column(String(45), nullable=True)
    folderPath = Column(String(100), nullable=True)
    CreatedOn = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)
    labels = Column(String(50), nullable=True)
    fields = Column(TEXT, nullable=True)
    training_result = Column(TEXT, nullable=True)
    test_result = Column(TEXT, nullable=True)
    docType = Column(String(50), nullable=True)
    tag_info = Column(JSON, nullable=True)
    extractiontype = Column(String(50), nullable=True)
    pagepreference = Column(JSON, nullable=True)
    userID = Column(String, nullable=True)
    update_by = Column(String, nullable=True)
    is_active = Column(Integer, nullable=True, default=1)
    is_enabled = Column(Integer, nullable=True, default=1)

    def to_dict(self):
        return {
            column.name: getattr(self, column.name) for column in self.__table__.columns
        }  # noqa: E501

    # document_model = relationship("DocumentModel", back_populates="document_tags")


class DocumentType(Base):
    __tablename__ = "documenttype"

    idDocumentType = Column(Integer, primary_key=True, index=True)
    Name = Column(String(45), nullable=True)
    CreatedOn = Column(DateTime, nullable=True)

    # __mapper_args__ = {"eager_defaults": True}


class Document(Base):
    __tablename__ = "document"

    idDocument = Column(Integer, primary_key=True, index=True)
    idDocumentType = Column(Integer, ForeignKey("documenttype.idDocumentType"))
    documentModelID = Column(Integer, ForeignKey("documentmodel.idDocumentModel"))
    entityID = Column(Integer, ForeignKey("entity.idEntity"))
    entityBodyID = Column(Integer, ForeignKey("entitybody.idEntityBody"))
    supplierAccountID = Column(Integer, nullable=True)
    vendorAccountID = Column(Integer, ForeignKey("vendoraccount.idVendorAccount"))
    docheaderID = Column(TEXT, nullable=True)
    documentStatusID = Column(Integer, ForeignKey("documentstatus.idDocumentstatus"))
    docPath = Column(TEXT, nullable=True)
    documentDate = Column(TEXT, nullable=True)
    totalAmount = Column(Float, nullable=True)
    documentDescription = Column(TEXT, nullable=True)
    documentTotalPages = Column(Integer, nullable=True)
    # ruleID = Column(Integer, nullable=True)
    # PODocumentID = Column(String(30), nullable=True)
    documentsubstatusID = Column(
        Integer, ForeignKey("documentsubstatus.idDocumentSubstatus")
    )
    # isRuleUpdated = Column(Integer, nullable=True)
    CreatedOn = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)
    sourcetype = Column(String(30), nullable=True)
    # ref_url = Column(TEXT, nullable=True)
    sender = Column(TEXT, nullable=True)
    supporting_doc = Column(JSON, nullable=True)
    JournalNumber = Column(String(60), nullable=True)
    uploadtime = Column(String(15), nullable=True)
    lock_info = Column(JSON, nullable=True)
    lock_user_id = Column(Integer, nullable=True)
    lock_status = Column(Integer, nullable=True)
    lock_date_time = Column(DateTime, nullable=True)
    journey_doc = Column(TEXT, nullable=True)
    approverData = Column(JSON, nullable=True)
    # document_to_upload = Column(TEXT, nullable=True)
    # MultiPoList = Column(TEXT, nullable=True)
    UploadDocType = Column(String(45), nullable=True)
    grn_documentID = Column(JSON, nullable=True)
    # json_download_path = Column(String(45), nullable=True)
    documentData = Column(JSON, nullable=True)
    # flippo_Approvers_Data = Column(JSON, nullable=True)
    # last_rule_run = Column(JSON, nullable=True)
    store = Column(TEXT, nullable=True)
    dept = Column(TEXT, nullable=True)
    voucher_id = Column(String, nullable=True)
    mail_row_key = Column(String, nullable=True)
    retry_count = Column(Integer, nullable=True)
    # __mapper_args__ = {"eager_defaults": True}


class DocumentStatus(Base):
    __tablename__ = "documentstatus"

    idDocumentstatus = Column(Integer, primary_key=True, index=True)
    status = Column(String(45), nullable=True)
    description = Column(String(100), nullable=True)
    createdOn = Column(DateTime, nullable=True)

    # __mapper_args__ = {"eager_defaults": True}


class DocumentData(Base):
    __tablename__ = "documentdata"
    # __table_args__ = {'schema': 'pfg_schema'}

    idDocumentData = Column(Integer, primary_key=True, autoincrement=True)
    documentID = Column(Integer, ForeignKey("document.idDocument"), nullable=True)
    documentTagDefID = Column(Integer, nullable=True)
    Value = Column(TEXT, nullable=True)
    IsUpdated = Column(Integer, nullable=True, default=0)
    isError = Column(Integer, nullable=True, default=0)
    ErrorDesc = Column(String(100), nullable=True)
    stage = Column(Integer, nullable=True)
    Xcord = Column(String(45), nullable=True)
    Ycord = Column(String(45), nullable=True)
    Width = Column(String(45), nullable=True)
    Height = Column(String(45), nullable=True)
    Fuzzy_scr = Column(Float, nullable=True)
    CreatedOn = Column(DateTime(timezone=True), nullable=True)

    # document = relationship("Document", back_populates="documentdata")


class DocumentTagDef(Base):
    __tablename__ = "documenttagdef"
    # __table_args__ = {'schema': 'pfg_schema'}

    idDocumentTagDef = Column(Integer, primary_key=True, autoincrement=True)
    idDocumentModel = Column(
        Integer, ForeignKey("documentmodel.idDocumentModel"), nullable=True
    )
    TagLabel = Column(String(100), nullable=True)
    NERActive = Column(LargeBinary, nullable=True)
    Xcord = Column(String(45), nullable=True)
    Ycord = Column(String(45), nullable=True)
    Width = Column(String(45), nullable=True)
    Height = Column(String(45), nullable=True)
    Pages = Column(String(20), nullable=True)
    CreatedOn = Column(DateTime(timezone=True), nullable=True)
    UpdatedOn = Column(DateTime(timezone=True), nullable=True)
    transformation = Column(JSON, nullable=True)
    isdelete = Column(JSON, nullable=True)
    datatype = Column(JSON, nullable=True)

    def to_dict(self):
        return {
            column.name: getattr(self, column.name) for column in self.__table__.columns
        }  # noqa: E501

    # # Relationship
    # document_model = relationship("DocumentModel", back_populates="document_tags")


# creating class to load DocumentUpdates table


class DocumentUpdates(Base):
    __tablename__ = "documentupdates"
    # __table_args__ = {'schema': 'pfg_schema'}

    idDocumentUpdates = Column(Integer, primary_key=True, autoincrement=True)
    documentDataID = Column(
        Integer, ForeignKey("documentdata.idDocumentData"), nullable=True
    )
    documentLineItemID = Column(
        Integer, ForeignKey("documentlineitems.idDocumentLineItems"), nullable=True
    )
    IsActive = Column(Integer, nullable=True)
    OldValue = Column(TEXT, nullable=True)
    NewValue = Column(TEXT, nullable=True)
    updatedBy = Column(Integer, ForeignKey("user.idUser"), nullable=True)
    UpdatedOn = Column(DateTime(timezone=True), nullable=True)

    # documentData = relationship('DocumentData', back_populates='documentUpdates')
    # documentLineItem = relationship('DocumentLineItems',
    # back_populates='documentUpdates')
    # user = relationship('User', back_populates='documentUpdates')


# creating class to load DocumentStage table


class DocumentStage(Base):
    __tablename__ = "documentstage"

    idDocumentStage = Column(Integer, primary_key=True, autoincrement=True)
    Stage = Column(String(45), nullable=True)
    CreatedOn = Column(DateTime(timezone=True), nullable=True)


# creating class to load DocumentTableDef table


class DocumentTableDef(Base):
    __tablename__ = "documenttabledef"

    idDocumentTableDef = Column(Integer, primary_key=True, autoincrement=True)
    documentID = Column(Integer, ForeignKey("document.idDocument"), nullable=True)
    Xcord = Column(String(45), nullable=True)
    Ycord = Column(String(45), nullable=True)
    Width = Column(String(45), nullable=True)
    Height = Column(String(45), nullable=True)
    CreatedOn = Column(DateTime(timezone=True), nullable=True)
    UpdatedOn = Column(DateTime(timezone=True), nullable=True)
    updatedBy = Column(Integer, ForeignKey("user.idUser"), nullable=True)

    # document = relationship("Document", back_populates="document_table_defs")
    # user = relationship("User", back_populates="document_table_defs")


class DocumentLineItems(Base):
    __tablename__ = "documentlineitems"
    # __table_args__ = {'schema': 'pfg_schema'}

    idDocumentLineItems = Column(Integer, primary_key=True, autoincrement=True)
    lineItemtagID = Column(
        BigInteger, ForeignKey("documentlineitemtags.idDocumentLineItemTags")
    )
    documentID = Column(BigInteger, ForeignKey("document.idDocument"))
    Value = Column(TEXT, nullable=True)
    IsUpdated = Column(Integer, nullable=True, default=0)
    isError = Column(Integer, nullable=True, default=0)
    ErrorDesc = Column(String(100), nullable=True)
    itemCode = Column(String(45), nullable=True)
    UpdatedDate = Column(DateTime(timezone=True), nullable=True)
    Xcord = Column(String(45), nullable=True)
    Ycord = Column(String(45), nullable=True)
    invoice_itemcode = Column(String(45), nullable=True)
    Width = Column(String(45), nullable=True)
    Height = Column(String(45), nullable=True)
    Fuzzy_scr = Column(Float, nullable=True)
    CK_status = Column(
        BigInteger, ForeignKey("documentrulecode.iddocumentrulecode"), nullable=True
    )
    CreatedOn = Column(DateTime(timezone=True), nullable=True)

    # document = relationship("Document", back_populates="line_items")
    # line_item_tag = relationship("DocumentLineItemTags", back_populates="line_items")
    # ck_status = relationship("DocumentRuleCode", back_populates="line_items")


# creating class to load DocumentLineItemTags table


class DocumentLineItemTags(Base):
    __tablename__ = "documentlineitemtags"

    idDocumentLineItemTags = Column(Integer, primary_key=True, autoincrement=True)
    idDocumentModel = Column(
        Integer, ForeignKey("documentmodel.idDocumentModel"), nullable=True
    )
    TagName = Column(String(45), nullable=True)
    TagDesc = Column(String(45), nullable=True)
    Xcord = Column(String(45), nullable=True)
    Ycord = Column(String(45), nullable=True)
    Width = Column(String(45), nullable=True)
    Height = Column(String(45), nullable=True)
    transformation = Column(JSON, nullable=True)
    isdelete = Column(JSON, nullable=True)
    datatype = Column(String(45), nullable=True)
    fillers = Column(String(45), nullable=True)

    def to_dict(self):
        return {
            column.name: getattr(self, column.name) for column in self.__table__.columns
        }  # noqa: E501

    # document_model = relationship('DocumentModel', back_populates='line_item_tags')


class DocumentHistoryLogs(Base):
    __tablename__ = "documenthistorylog"

    iddocumenthistorylog = Column(Integer, primary_key=True, autoincrement=True)
    documentID = Column(Integer, ForeignKey("document.idDocument"), nullable=True)
    documentdescription = Column(TEXT, nullable=True)
    documentStatusID = Column(
        Integer, ForeignKey("documentstatus.idDocumentstatus"), nullable=True
    )
    documentSubStatusID = Column(
        Integer, ForeignKey("documentsubstatus.idDocumentSubstatus"), nullable=True
    )
    userID = Column(SmallInteger, ForeignKey("user.idUser"), nullable=True)
    userAmount = Column(String(45), nullable=True)
    documentfinstatus = Column(SmallInteger, nullable=True)
    CreatedOn = Column(DateTime, nullable=True)

    # # Assuming relationships to the referenced tables for ORM purposes
    # document = relationship('Document', back_populates='history_logs')
    # document_status = relationship('DocumentStatus', back_populates='history_logs')
    # user = relationship('User', back_populates='history_logs')


# USER TABLES

# creating class to load customer table


class Customer(Base):
    __tablename__ = "customer"

    idCustomer = Column(Integer, primary_key=True, index=True)
    CustomerName = Column(String(90), nullable=True)

    # __mapper_args__ = {"eager_defaults": True}


# creating class to load entitytype table


class EntityType(Base):
    __tablename__ = "entitytype"

    idEntityType = Column(Integer, primary_key=True, index=True)
    TypeofEntity = Column(String(45), nullable=True)
    CreatedOn = Column(DateTime, nullable=True)

    # entity = relationship("Entity", back_populates="entity_type")

    # __mapper_args__ = {"eager_defaults": True}


# creating class to load entity table


class Entity(Base):
    __tablename__ = "entity"

    idEntity = Column(Integer, primary_key=True, index=True)
    customerID = Column(Integer, ForeignKey("customer.idCustomer"))
    EntityName = Column(String(150), nullable=True)
    EntityAddress = Column(String(100), nullable=True)
    City = Column(String(45), nullable=True)
    Country = Column(String(45), nullable=True)
    entityTypeID = Column(Integer, ForeignKey("entitytype.idEntityType"))
    sourceSystemType = Column(String(45), nullable=True)
    EntityCode = Column(String(45), nullable=True)
    groupvatcode = Column(String(45), nullable=True)
    entity_trn_number = Column(String(100), nullable=True)
    entity_po_box_number = Column(String(45), nullable=True)
    CreatedOn = Column(DateTime, nullable=True)
    Synonyms = Column(String(200), nullable=True)

    # __mapper_args__ = {"eager_defaults": True}


# creating class to load entitybodytype table


class EntityBodyType(Base):
    __tablename__ = "entitybodytype"

    idEntityBodyType = Column(Integer, primary_key=True, index=True)
    TypeofEntityBody = Column(String(45), nullable=True)
    CreatedOn = Column(DateTime, nullable=True)

    # __mapper_args__ = {"eager_defaults": True}


# creating class to load entitybody table


class EntityBody(Base):
    __tablename__ = "entitybody"

    idEntityBody = Column(Integer, primary_key=True, index=True)
    EntityBodyName = Column(String(150), nullable=True)
    EntityCode = Column(String(45), nullable=True)
    Address = Column(String(100), nullable=True)
    LocationCode = Column(String(45), nullable=True)
    City = Column(String(45), nullable=True)
    Country = Column(String(45), nullable=True)
    entityID = Column(Integer, ForeignKey("entity.idEntity"))
    entityBodyTypeID = Column(Integer, ForeignKey("entitybodytype.idEntityBodyType"))

    # __mapper_args__ = {"eager_defaults": True}


# creating class to load entitybody table


class Department(Base):
    __tablename__ = "department"

    ID = Column(Integer, primary_key=True, autoincrement=True)
    SETID = Column(String(5), nullable=True)
    DEPTID = Column(String(10), nullable=True)
    EFFDT = Column(String(50), nullable=True)
    EFF_STATUS = Column(String(1), nullable=True)
    DESCR = Column(String(50), nullable=True)
    DESCRSHORT = Column(String(10), nullable=True)
    entityID = Column(Integer, ForeignKey("entity.idEntity"))
    entityBodyID = Column(Integer, ForeignKey("entitybody.idEntityBody"))
    CreatedOn = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)

    # __mapper_args__ = {"eager_defaults": True}


class UserAccess(Base):
    __tablename__ = "useraccess"

    idUserAccess = Column(Integer, primary_key=True, index=True)
    UserID = Column(Integer, ForeignKey("user.idUser"))
    EntityID = Column(Integer, ForeignKey("entity.idEntity"))
    EntityBodyID = Column(Integer, ForeignKey("entitybody.idEntityBody"))
    DepartmentID = Column(Integer, ForeignKey("department.idDepartment"))
    isActive = Column(Integer, nullable=True)
    CreatedBy = Column(Integer, nullable=True)
    CreatedOn = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)
    categoryID = Column(Integer, ForeignKey("category.idCategory"))
    maxAmount = Column(Float, nullable=True)
    userPriority = Column(Integer, nullable=True)
    preApprove = Column(Integer, nullable=True)
    subRole = Column(Integer, nullable=True)

    # user = relationship("User", back_populates="user_access")
    # entity = relationship("Entity", back_populates="user_access")
    # entity_body = relationship("EntityBody", back_populates="user_access")
    # department = relationship("Department", back_populates="user_access")

    # __mapper_args__ = {"eager_defaults": True}


# to store the login table
class LoginInfoLog(Base):
    __tablename__ = "login_info_log"

    idlogininfo = Column(Integer, primary_key=True, autoincrement=True)
    userID = Column(SmallInteger, ForeignKey("user.idUser"), nullable=False)
    loginDate = Column(DateTime, nullable=False)

    # # Relationship to User table for ORM purposes
    # user = relationship('User', back_populates='login_logs')


# to store the otp and expiry date
class Otp_Code(Base):
    __tablename__ = "password_otp"

    idpassword_otp = Column(Integer, primary_key=True, autoincrement=True)
    password_otpcolcode = Column(String(6), nullable=True)
    password_otp_userid = Column(SmallInteger, ForeignKey("user.idUser"), nullable=True)
    expDate = Column(DateTime, nullable=True)

    # # Relationship to User table for ORM purposes
    # user = relationship('User', back_populates='otp_codes')


# creating class to load entitybody table
class User(Base):
    __tablename__ = "user"

    idUser = Column(Integer, primary_key=True, index=True)
    customerID = Column(Integer, ForeignKey("customer.idCustomer"))
    isCustomerUser = Column(Integer, nullable=True)
    firstName = Column(String(45), nullable=True)
    lastName = Column(String(45), nullable=True)
    UserCode = Column(String(45), nullable=True)
    Designation = Column(String(45), nullable=True)
    contact = Column(String(45), nullable=True)
    email = Column(String(45), nullable=True)
    miscalaneous = Column(JSON, nullable=True)
    isActive = Column(Integer, nullable=True)
    created_by = Column(Integer, nullable=True)
    CreatedOn = Column(DateTime, nullable=True)
    landingPage = Column(String(45), nullable=True)
    uploadOpt = Column(String(45), nullable=True)
    show_updates = Column(Integer, nullable=True)
    account_type = Column(String(20), nullable=True)
    dept_ids = Column(JSON, nullable=True)
    azure_id = Column(String, nullable=True)
    user_role = Column(String, nullable=True)
    employee_id = Column(String, nullable=True)

    # customers = relationship("Customer", back_populates="user")
    # user_access = relationship("UserAccess", back_populates="user")


#     __mapper_args__ = {"eager_defaults": True}


# Vendor Table
class Vendor(Base):
    __tablename__ = "vendor"

    idVendor = Column(Integer, primary_key=True, index=True)
    VendorName = Column(String(100), nullable=True)
    Address = Column(String(255), nullable=True)
    City = Column(String(45), nullable=True)
    Country = Column(String(100), nullable=True)
    Desc = Column(String(255), nullable=True)
    VendorCode = Column(String(45), nullable=True)
    Email = Column(String(100), nullable=True)
    Contact = Column(String(100), nullable=True)
    Website = Column(String(100), nullable=True)
    Salutation = Column(String(100), nullable=True)
    FirstName = Column(String(100), nullable=True)
    LastName = Column(String(100), nullable=True)
    Designation = Column(String(100), nullable=True)
    TradeLicense = Column(String(100), nullable=True)
    VATLicense = Column(String(100), nullable=True)
    TLExpiryDate = Column(String(100), nullable=True)
    VLExpiryDate = Column(String(100), nullable=True)
    TRNNumber = Column(String(100), nullable=True)
    createdBy = Column(Integer, nullable=True)
    CreatedOn = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)
    entityID = Column(Integer, nullable=True)
    Synonyms = Column(String(100), nullable=True)
    vendorType = Column(String(100), nullable=True)
    miscellaneous = Column(JSONB, nullable=True)
    currency = Column(String, nullable=True)
    account = Column(String(30), nullable=True)

    # __mapper_args__ = {"eager_defaults": True}


# VendorAccount Table
class VendorAccount(Base):
    __tablename__ = "vendoraccount"

    idVendorAccount = Column(Integer, primary_key=True, index=True)
    vendorID = Column(Integer, ForeignKey("vendor.idVendor"), nullable=False)
    AccountType = Column(String(45), nullable=True)
    Account = Column(String(45), nullable=True)
    entityID = Column(Integer, ForeignKey("entity.idEntity"), nullable=True)
    entityBodyID = Column(Integer, ForeignKey("entitybody.idEntityBody"), nullable=True)
    City = Column(String(45), nullable=True)
    Country = Column(String(45), nullable=True)
    LocationCode = Column(String(45), nullable=True)
    CreatedOn = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)

    # entity = relationship("Entity", back_populates="vendor_account")
    # entity_body = relationship("EntityBody", back_populates="vendor_account")
    # __mapper_args__ = {"eager_defaults": True}


# VendorUserAccess Table
class VendorUserAccess(Base):
    __tablename__ = "vendoruseraccess"

    idVendorUserAccess = Column(Integer, primary_key=True, index=True)
    VendorUserID = Column(SmallInteger, ForeignKey("user.idUser"))
    vendorID = Column(Integer, ForeignKey("vendor.idVendor"))
    vendorAccountID = Column(Integer, ForeignKey("vendoraccount.idVendorAccount"))
    entityID = Column(Integer, ForeignKey("entity.idEntity"))
    isActive = Column(Integer, nullable=True)
    CreatedBy = Column(SmallInteger, nullable=True)


#     __mapper_args__ = {"eager_defaults": True}


# # Vendor Service Table
class ServiceProvider(Base):
    __tablename__ = "serviceprovider"

    idServiceProvider = Column(Integer, primary_key=True, autoincrement=True)
    ServiceProviderName = Column(String(200), nullable=True)
    ServiceProviderCode = Column(String(45), nullable=True)
    entityID = Column(Integer, ForeignKey("entity.idEntity"), nullable=True)
    servicetrn = Column(String(45), nullable=True)
    serviceaddress = Column(String(100), nullable=True)
    City = Column(String(45), nullable=True)
    Country = Column(String(45), nullable=True)
    createdBy = Column(SmallInteger, ForeignKey("user.idUser"), nullable=True)
    LocationCode = Column(String(45), nullable=True)
    CreatedOn = Column(DateTime, nullable=True)
    miscellaneous = Column(JSON, nullable=True)
    default_url = Column(String(450), nullable=True)
    convert_img = Column(JSON, nullable=True)
    lang_check = Column(JSON, nullable=True)


# ServiceAccount Table
class ServiceAccount(Base):
    __tablename__ = "serviceaccount"

    idServiceAccount = Column(Integer, primary_key=True, autoincrement=True)
    serviceProviderID = Column(
        Integer, ForeignKey("serviceprovider.idServiceProvider"), nullable=True
    )
    Account = Column(String(45), nullable=True)
    Email = Column(String(45), nullable=True)
    MeterNumber = Column(String(45), nullable=True)
    LocationCode = Column(String(45), nullable=True)
    Address = Column(String(100), nullable=True)
    entityID = Column(Integer, ForeignKey("entity.idEntity"), nullable=True)
    entityBodyID = Column(Integer, ForeignKey("entitybody.idEntityBody"), nullable=True)
    isActive = Column(SmallInteger, default=True)
    operatingUnit = Column(String(45), nullable=True)
    approver = Column(String(20), nullable=True)
    fileType = Column(String(45), nullable=True)
    createdBy = Column(SmallInteger, ForeignKey("user.idUser"), nullable=True)
    CreatedOn = Column(DateTime, nullable=True)
    log_file_path = Column(JSON, nullable=True)
    miscellaneous = Column(JSON, nullable=True)

    # # Relationships to the referenced tables for ORM purposes
    # created_by_user = relationship('User', back_populates='created_service_accounts')
    # service_provider = relationship('ServiceProvider',
    # back_populates='service_accounts')
    # entity = relationship('Entity', back_populates='service_accounts')
    # entity_body = relationship('EntityBody', back_populates='service_accounts')


# SupplierSchedule Table
class ServiceProviderSchedule(Base):
    __tablename__ = "serviceproviderschedule"

    idAccountSchedule = Column(Integer, primary_key=True, autoincrement=True)
    serviceAccountID = Column(
        Integer, ForeignKey("serviceprovider.idServiceProvider"), nullable=True
    )
    ScheduleDateTime = Column(DateTime, nullable=True)
    CreatedOn = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)


# BatchTriggerHistory Table
class BatchTriggerHistory(Base):
    __tablename__ = "batchprocesshistory"

    idbatchprocesshistory = Column(Integer, primary_key=True, autoincrement=True)
    batchProcessType = Column(Integer, nullable=True)
    started_by = Column(Integer, nullable=True)
    entityID = Column(Integer, ForeignKey("entity.idEntity"), nullable=True)
    status = Column(String(45), nullable=True)
    uniqueID = Column(String(45), nullable=True)
    started_on = Column(DateTime, nullable=True)
    compeleted_on = Column(DateTime, nullable=True)


# AccountCostAllocation Table
class AccountCostAllocation(Base):
    __tablename__ = "accountcostallocation"

    idAccountCostAllocation = Column(Integer, primary_key=True, index=True)
    accountID = Column(Integer, ForeignKey("vendor.idVendor"), nullable=False)
    venAccountID = Column(String(100), nullable=True)
    entityID = Column(Integer, ForeignKey("entity.idEntity"), nullable=False)
    entityBodyID = Column(
        Integer, ForeignKey("entitybody.idEntityBody"), nullable=False
    )
    interco = Column(String(45), nullable=True)
    departmentID = Column(
        Integer, ForeignKey("department.idDepartment"), nullable=False
    )
    description = Column(String(255), nullable=True)
    mainAccount = Column(String(255), nullable=True)
    locationCode = Column(String(45), nullable=True)
    naturalAccountWater = Column(String(45), nullable=True)
    naturalAccountHousing = Column(String(45), nullable=True)
    costCenter = Column(String(45), nullable=True)
    product = Column(String(45), nullable=True)
    project = Column(String(45), nullable=True)
    elementFactor = Column(Integer, nullable=True)
    Element = Column(String(200), nullable=True)
    segments = Column(String(100), nullable=True)
    bsMovements = Column(String(100), nullable=True)
    fixedAssetDepartment = Column(String(100), nullable=True)
    fixedAssetGroup = Column(String(100), nullable=True)
    isactive_Alloc = Column(Integer)
    CreatedOn = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)
    new_columns = Column(JSON, nullable=True)


#     __mapper_args__ = {"eager_defaults": True}

# Credentials Table


class Credentials(Base):
    __tablename__ = "credentials"

    idCredentials = Column(Integer, primary_key=True, index=True)
    crentialTypeId = Column(Integer, nullable=True)
    LogName = Column(String(45), nullable=True)
    LogSecret = Column(String(100), nullable=True)
    UserName = Column(String(45), nullable=True)
    KeyValue = Column(String(200), nullable=True)
    URL = Column(String(200), nullable=True)
    serviceProviderAccountID = Column(Integer, nullable=True)
    userID = Column(Integer, ForeignKey("user.idUser"))
    entityID = Column(Integer, ForeignKey("entity.idEntity"))
    entityBodyID = Column(Integer, ForeignKey("entitybody.idEntityBody"))
    companyID = Column(Integer, nullable=True)
    lastloginDate = Column(DateTime, nullable=True)
    CreatedOn = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)
    isvalidtoken = Column(SmallInteger, nullable=True)
    token = Column(TEXT, nullable=True)
    refresh_token = Column(TEXT, nullable=True)


class CredentialType(Base):
    __tablename__ = "credentialtype"

    idCredentialType = Column(Integer, primary_key=True, index=True)
    CrentialType = Column(String(45), nullable=True)
    CreatedOn = Column(DateTime, nullable=True)


# Permisiions


class AccessPermission(Base):
    __tablename__ = "accesspermission"

    idAccessPermission = Column(Integer, primary_key=True, index=True)
    permissionDefID = Column(
        Integer, ForeignKey("accesspermissiondef.idAccessPermissionDef")
    )
    userID = Column(Integer, ForeignKey("user.idUser"))
    CreatedOn = Column(DateTime, nullable=True)


#     __mapper_args__ = {"eager_defaults": True}


# idAccessPermissionDef Table
class AccessPermissionDef(Base):
    __tablename__ = "accesspermissiondef"

    idAccessPermissionDef = Column(Integer, primary_key=True, index=True)
    NameOfRole = Column(String(100), nullable=True)
    Priority = Column(Integer, nullable=True)
    User = Column(Integer, nullable=True)
    Permissions = Column(Integer, nullable=True)
    isUserRole = Column(Integer, nullable=True)
    AccessPermissionTypeId = Column(
        Integer, ForeignKey("accesspermissiontype.idAccessPermissionType")
    )
    NewInvoice = Column(Integer, nullable=True)
    isActive = Column(Integer, nullable=True)
    isConfigPortal = Column(Integer, nullable=True)
    isDashboard = Column(Integer, nullable=True)
    is_epa = Column(Integer, nullable=True)
    is_gpa = Column(Integer, nullable=True)
    is_vspa = Column(Integer, nullable=True)
    is_spa = Column(Integer, nullable=True)
    is_fp = Column(Integer, nullable=True)
    is_fpa = Column(Integer, nullable=True)
    amountApprovalID = Column(Integer, nullable=True)
    allowBatchTrigger = Column(Integer, nullable=True)
    allowServiceTrigger = Column(Integer, nullable=True)
    iDefault = Column(Integer, nullable=True)
    CreatedOn = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)
    is_grn_approval = Column(Integer, nullable=True)


#     __mapper_args__ = {"eager_defaults": True}


# AccessPermissionType Table
class AccessPermissionType(Base):
    __tablename__ = "accesspermissiontype"

    idAccessPermissionType = Column(Integer, primary_key=True, index=True)
    PermissionType = Column(String(45), nullable=True)
    CreatedOn = Column(DateTime, nullable=True)


#     __mapper_args__ = {"eager_defaults": True}


# AmountApproveLevel Table
class AmountApproveLevel(Base):
    __tablename__ = "amountapprovelevel"

    idAmountApproveLevel = Column(Integer, primary_key=True, index=True)
    MaxAmount = Column(Integer, nullable=True)


#
class ColumnPosDef(Base):
    # __tablename__ = 'amountapprovelevel'
    __tablename__ = "columnnamesdef"

    idColumn = Column(Integer, primary_key=True, index=True)
    columnName = Column(String(45), nullable=True)
    columnDescription = Column(String(60), nullable=True)
    tableName = Column(String(45), nullable=True)
    dbColumnname = Column(String(45), nullable=True)
    CreatedOn = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)


class DocumentColumnPos(Base):
    # __tablename__ = 'amountapprovelevel'
    __tablename__ = "documentcolumns"

    idDocumentColumn = Column(Integer, primary_key=True, index=True)
    columnNameDefID = Column(Integer, nullable=True)
    documentColumnPos = Column(Integer, nullable=True)
    userID = Column(Integer, nullable=True)
    isActive = Column(Integer, nullable=True)
    tabtype = Column(Integer, nullable=True)
    CreatedOn = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)


# ----------- FR Tables ------------------

# FR configuration Table


class FRConfiguration(Base):
    __tablename__ = "frconfigurations"

    idFrConfigurations = Column(Integer, primary_key=True, index=True)
    idCustomer = Column(Integer, ForeignKey("customer.idCustomer"))
    Endpoint = Column(String(100), nullable=True)
    ConnectionString = Column(String(300), nullable=True)
    Key1 = Column(String(50), nullable=True)
    Key2 = Column(String(50), nullable=True)
    ContainerName = Column(String(45), nullable=True)
    ServiceContainerName = Column(String(45), nullable=True)
    SasToken = Column(String(45), nullable=True)
    SasUrl = Column(String(45), nullable=True)
    SasExpiry = Column(DateTime, nullable=True)
    ApiVersion = Column(String(30), nullable=True)
    CallsPerMin = Column(Integer, nullable=True)
    PagesPerMonth = Column(Integer, nullable=True)
    gptConfig = Column(JSON, nullable=True)
    email_listener_info = Column(JSON, nullable=True)


#     __mapper_args__ = {"eager_defaults": True}

# FR Meta Data Table


class FRMetaData(Base):
    __tablename__ = "frmetadata"

    idFrMetaData = Column(Integer, primary_key=True, index=True)
    idInvoiceModel = Column(Integer, ForeignKey("documentmodel.idDocumentModel"))
    FolderPath = Column(String(45), nullable=False)
    DateFormat = Column(String(45), nullable=False)
    AccuracyOverall = Column(String(10), nullable=True)
    AccuracyFeild = Column(String(10), nullable=True)
    InvoiceFormat = Column(String(45), nullable=True)
    ruleID = Column(Integer, ForeignKey("documentrules.idDocumentRules"), nullable=True)
    Units = Column(String(45), nullable=True)
    TableLogic = Column(String(45), nullable=True)
    ErrorThreshold = Column(Integer, nullable=True)
    mandatorylinetags = Column(TEXT, nullable=True)
    mandatoryheadertags = Column(TEXT, nullable=True)
    batchmap = Column(Integer, nullable=True)
    optionalheadertags = Column(String(500), nullable=True)
    optionallinertags = Column(String(500), nullable=True)
    erprule = Column(Integer, ForeignKey("erprulecodes.iderprules"), nullable=True)
    vendorType = Column(String(20), nullable=True)
    UnitPriceTol_percent = Column(Integer, nullable=True)
    QtyTol_percent = Column(Integer, nullable=True)
    GrnCreationType = Column(Integer, nullable=False, default=1)
    current_po_format = Column(String(85), nullable=True)
    req_po_format = Column(String(85), nullable=True)
    service_rules_function = Column(JSON, nullable=True)
    ref_url_config = Column(JSON, nullable=True)
    temp_language = Column(String(50), nullable=True)
    InvoiceNumberFormat = Column(String(50), nullable=True)

    def to_dict(self):
        return {
            column.name: getattr(self, column.name) for column in self.__table__.columns
        }  # noqa: E501


#     __mapper_args__ = {"eager_defaults": True}
# OCR Log Table


class OCRlogs(Base):
    __tablename__ = "ocrlogs"

    idOCRlogs = Column(Integer, primary_key=True, autoincrement=True)
    documentId = Column(Integer, ForeignKey("document.idDocument"), nullable=True)
    labelType = Column(String(45), nullable=True)
    predictedValue = Column(String(45), nullable=True)
    editedValue = Column(String(45), nullable=True)
    accuracy = Column(String(45), nullable=True)
    editedOn = Column(String(45), nullable=True)
    frModelID = Column(String(45), nullable=True)
    errorFlag = Column(Integer, nullable=True)

    # # Relationship to Document table for ORM purposes
    # document = relationship('Document', back_populates='ocr_logs')


# OCR UserItem Mapping


class UserItemMapping(Base):
    __tablename__ = "useritemmapping"

    iduserItemMapping = Column(Integer, primary_key=True, autoincrement=True)
    idDocumentModel = Column(
        Integer, ForeignKey("documentModel.idDocumentModel"), nullable=True
    )
    itemCodePO = Column(String(45), nullable=True)
    itemCodeInvoice = Column(String(45), nullable=True)
    itemDescPO = Column(String(100), nullable=True)
    itemDescInvo = Column(String(100), nullable=True)
    createdOn = Column(DateTime, nullable=True)

    # # Relationship to DocumentModel table for ORM purposes
    # document_model = relationship('DocumentModel',
    # back_populates='user_item_mappings')


# Application General config


class GeneralConfig(Base):
    __tablename__ = "generalconfig"

    idgeneralconfig = Column(Integer, primary_key=True, autoincrement=True)
    customerID = Column(SmallInteger, nullable=True)
    isRoleBased = Column(SmallInteger, nullable=True)
    delayednotification = Column(SmallInteger, nullable=True)
    serviceBatchConf = Column(JSON, primary_key=True, autoincrement=True)
    vendorBatchConf = Column(JSON, nullable=True)
    itemMetaDataConf = Column(JSON, nullable=True)
    updatedBy = Column(Integer, nullable=True)
    createdON = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)
    isApprovalEnabled = Column(Integer, nullable=True)


# documentRules data


class Rule(Base):
    __tablename__ = "documentrules"

    idDocumentRules = Column(Integer, primary_key=True, index=True)
    Name = Column(String(500), nullable=True)
    description = Column(String(100), nullable=True)
    IsActive = Column(Integer, nullable=True)
    createdOn = Column(DateTime, nullable=True)
    GrnCreationType = Column(Integer, nullable=True)


#     __mapper_args__ = {"eager_defaults": True}


class PFGRule(Base):
    __tablename__ = "erprulecodes"

    iderprules = Column(Integer, primary_key=True, index=True)
    Name = Column(String(45), nullable=True)
    description = Column(String(255), nullable=True)
    category = Column(String(45), nullable=True)
    IsActive = Column(Integer, nullable=True)
    createdOn = Column(DateTime, nullable=True)


class DocumentSubStatus(Base):
    __tablename__ = "documentsubstatus"

    idDocumentSubstatus = Column(Integer, primary_key=True, index=True)
    DocumentstatusID = Column(Integer, ForeignKey("documentstatus.idDocumentstatus"))
    status = Column(String(45), nullable=True)
    description = Column(String(100), nullable=True)
    type = Column(String(15), nullable=True)
    createdOn = Column(DateTime, nullable=True)


#     __mapper_args__ = {"eager_defaults": True}


class DocumentRulemapping(Base):
    __tablename__ = "docrulestatusmapping"

    iddocrulestatusmapping = Column(Integer, primary_key=True, index=True)
    DocumentstatusID = Column(Integer, ForeignKey("documentstatus.idDocumentstatus"))
    DocumentRulesID = Column(Integer, ForeignKey("documentrules.idDocumentRules"))
    createdOn = Column(DateTime, nullable=True)
    statusorder = Column(Integer, nullable=True)


class DocumentRuleupdates(Base):
    __tablename__ = "documentruleshistorylog"

    iddocumenthistorylog = Column(Integer, primary_key=True, index=True)
    documentID = Column(Integer, ForeignKey("document.idDocument"))
    documentSubStatusID = Column(
        Integer, ForeignKey("documentsubstatus.idDocumentSubStatus")
    )
    IsActive = Column(Integer, nullable=True)
    userID = Column(SmallInteger, ForeignKey("user.idUser"), nullable=True)
    oldrule = Column(String(45), nullable=True)
    newrule = Column(String(45), nullable=True)
    type = Column(String(30), nullable=True)
    CreatedOn = Column(DateTime, nullable=True)


class DocumentModelComposed(Base):
    __tablename__ = "documentmodelcomposed"

    composed_id = Column(Integer, primary_key=True, autoincrement=True)
    composed_name = Column(String(50), nullable=True)
    training_result = Column(TEXT, nullable=True)
    vendorAccountId = Column(Integer, nullable=True)
    serviceAccountId = Column(Integer, nullable=True)
    serviceproviderID = Column(Integer, nullable=True)


# notification Models


class NotificationPriority(Base):
    __tablename__ = "notificationpriority"

    idNotificationPriority = Column(Integer, primary_key=True, autoincrement=True)
    notificationPriority = Column(String(45), nullable=False)
    CreatedOn = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)


class NotificationType(Base):
    __tablename__ = "notificationtype"

    idNotificationType = Column(Integer, primary_key=True, autoincrement=True)
    notificationtype = Column(String(45), nullable=False)
    CreatedOn = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)


class PullNotification(Base):
    __tablename__ = "pullnotification"

    idPullNotification = Column(Integer, primary_key=True, autoincrement=True)
    userID = Column(SmallInteger, ForeignKey("user.idUser"), nullable=True)
    notificationPriorityID = Column(
        Integer,
        ForeignKey("notificationpriority.idNotificationPriority"),
        nullable=True,
    )
    notificationTypeID = Column(
        Integer, ForeignKey("notificationtype.idNotificationType"), nullable=True
    )
    notificationMessage = Column(String(180), nullable=True)
    delayby = Column(DateTime, nullable=True)
    isSeen = Column(Boolean, default=False)
    CreatedOn = Column(DateTime, nullable=True)

    # # Relationships to the referenced tables for ORM purposes
    # user = relationship('User', back_populates='pull_notifications')
    # notification_priority = relationship('NotificationPriority',
    # back_populates='pull_notifications')
    # notification_type = relationship('NotificationType',
    # back_populates='pull_notifications')


class PullNotificationTemplate(Base):
    __tablename__ = "pullnotificationtemplate"

    idPullNotificationTemplate = Column(Integer, primary_key=True, autoincrement=True)
    templateHeading = Column(String(180), nullable=True)
    templateMessage = Column(String(180), nullable=True)
    notificationTypeID = Column(
        Integer, ForeignKey("notificationtype.idNotificationType"), nullable=True
    )
    notificationPriorityID = Column(
        Integer,
        ForeignKey("notificationpriority.idNotificationPriority"),
        nullable=True,
    )
    triggerDescriptionID = Column(
        Integer, ForeignKey("triggerdescription.idTriggerDescription"), nullable=True
    )
    notification_on_off = Column(Boolean, default=True)
    notificationCategory = Column(SmallInteger, nullable=True)
    message_type = Column(Integer, nullable=True)
    subject = Column(String(180), nullable=True)
    CustomerID = Column(SmallInteger, ForeignKey("customer.idCustomer"), nullable=True)
    CreatedOn = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)

    # # Relationships to the referenced tables for ORM purposes
    # notification_type = relationship('NotificationType',
    # back_populates='pull_notification_templates')
    # notification_priority = relationship('NotificationPriority',
    # back_populates='pull_notification_templates')
    # trigger_description = relationship('TriggerDescription',
    # back_populates='pull_notification_templates')
    # user = relationship('User', back_populates='pull_notification_templates')


class NotificationCategoryRecipient(Base):
    __tablename__ = "notificationrecipents"

    idnotificationrecipents = Column(Integer, primary_key=True, autoincrement=True)
    entityID = Column(Integer, ForeignKey("entity.idEntity"), nullable=True)
    isDefaultRecepients = Column(SmallInteger, nullable=True)
    notificationTypeID = Column(
        Integer, ForeignKey("notificationtype.idNotificationType"), nullable=True
    )
    notificationrecipient = Column(JSON, nullable=True)
    roles = Column(JSON, nullable=True)
    updated_by = Column(Integer, nullable=True)
    CreatedOn = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)


class TriggerDescription(Base):
    __tablename__ = "triggerdescription"

    idTriggerDescription = Column(Integer, primary_key=True, autoincrement=True)
    triggerOriginName = Column(String(50), nullable=False)
    triggerDescription = Column(String(100), nullable=True)
    triggerExceptionCode = Column(String(45), nullable=True)
    event_type = Column(Integer, nullable=True)
    CreatedOn = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)

    # # Relationships to PullNotificationTemplate if needed
    # pull_notification_templates = relationship('PullNotificationTemplate',
    # back_populates='trigger_description')


# class BatchErrorType(Base):
#     __table__ = Table('batcherrortypes', Base.metadata,
#                       autoload=True, autoload_with=engine)


# class ItemMetaData(Base):
#     __table__ = Table('itemmetadata', Base.metadata,
#                       autoload=True, autoload_with=engine)


# class ItemUserMap(Base):
#     __table__ = Table('itemusermap', Base.metadata,
#                       autoload=True, autoload_with=engine)


class DefaultFields(Base):
    __tablename__ = "defaultfields"

    idDefaultFields = Column(Integer, primary_key=True, autoincrement=True)
    Name = Column(String(50), nullable=False)
    Type = Column(String(10), nullable=True)
    Description = Column(String(110), nullable=True)
    Ismendatory = Column(Integer, nullable=True)
    TagType = Column(String(15), nullable=True)
    doc_supported = Column(JSON, nullable=True)


# class AgiCostAlloc(Base):
#     __table__ = Table('agicostallocation', Base.metadata,
#                       autoload=True, autoload_with=engine)


# class ItemMapUploadHistory(Base):
#     __table__ = Table('itemmappinguploadhistory', Base.metadata,
#                       autoload=True, autoload_with=engine)


# class GrnReupload(Base):
#     __table__ = Table('grnreupload', Base.metadata,
#                       autoload=True, autoload_with=engine)


# class POLines(Base):
#     __table__ = Table('d3agi_poline', Base.metadata,
#                       autoload=True, autoload_with=engine)


# class PaymentsInfo(Base):
#     __table__ = Table('invoicepaymentinfo', Base.metadata,
#                       autoload=True, autoload_with=engine)


# class FrConfigData(Base):
#     __table__ = Table('frconfigurations', Base.metadata,
#                       autoload=True, autoload_with=engine)


class ERPTAGMAP(Base):
    __tablename__ = "erp_tag_map"

    erpmapid = Column(Integer, primary_key=True, autoincrement=True)
    serina_tag = Column(String(255), nullable=False)
    cust_tag = Column(String(255), nullable=True)


class RuleCodes(Base):
    __tablename__ = "documentrulecode"

    iddocumentrulecode = Column(Integer, primary_key=True, index=True)
    status = Column(String(45), nullable=True)
    description = Column(String(100), nullable=True)
    type = Column(String(15), nullable=True)
    createdOn = Column(DateTime, nullable=True)


# class ReleaseUpdate(Base):
#     __table__ = Table('release_updates', Base.metadata,
#                       autoload=True, autoload_with=engine)

# class UserDepartment(Base):
#     __table__ = Table('user_department', Base.metadata,
#                       autoload=True, autoload_with=engine)

# class RoveGrn(Base):
#     __table__ = Table('rove_grn', Base.metadata,
#                         Column('iddocumenthistorylog', Integer, primary_key=True),
#                         # Column('documentStatusID', Integer),
#                         # Column('documentSubStatusID'),
#                         # Column('userID', Integer),
#                     autoload=True, autoload_with=engine)


class PFGDepartment(Base):
    __tablename__ = "pfgdepartment"
    ID = Column(Integer, primary_key=True, autoincrement=True)
    SETID = Column(String(5), nullable=True)
    DEPTID = Column(String(10), nullable=True)
    EFFDT = Column(String(50), nullable=True)
    EFF_STATUS = Column(String(1), nullable=True)
    DESCR = Column(String(50), nullable=True)
    DESCRSHORT = Column(String(10), nullable=True)


class PFGStore(Base):
    __tablename__ = "pfgstore"
    ID = Column(Integer, primary_key=True, autoincrement=True)
    SETID = Column(String(5), nullable=True)
    STORE = Column(String(10), nullable=True)
    EFFDT = Column(String(10), nullable=True)
    EFF_STATUS = Column(String(1), nullable=True)
    DESCR = Column(String(30), nullable=True)
    DESCRSHORT = Column(String(55), nullable=True)
    ADDRESS1 = Column(String(55), nullable=True)
    ADDRESS2 = Column(String(55), nullable=True)
    ADDRESS3 = Column(String(55), nullable=True)
    ADDRESS4 = Column(String(55), nullable=True)
    CITY = Column(String(20), nullable=True)
    STATE = Column(String(6), nullable=True)
    POSTAL = Column(String(12), nullable=True)
    COUNTRY = Column(String(3), nullable=True)


class PFGVendor(Base):
    __tablename__ = "pfgvendor"
    ID = Column(Integer, primary_key=True, autoincrement=True)
    SETID = Column(String(5), nullable=True)
    VENDOR_ID = Column(String(10), nullable=True)
    NAME1 = Column(String(50), nullable=True)
    NAME2 = Column(String(50), nullable=True)
    VENDOR_CLASS = Column(String(1), nullable=True)
    VENDOR_STATUS = Column(String(1), nullable=True)
    DEFAULT_LOC = Column(String(10), nullable=True)
    VENDOR_LOC = Column(JSON, nullable=True)
    VENDOR_ADDR = Column(JSON, nullable=True)
    VNDR_FIELD_C30_B = Column(String(30), nullable=True)


# class PFGVendor(Base):
#     __tablename__ = 'pfgvendor'
#     ID = Column(Integer, primary_key=True, autoincrement=True)
#     SETID = Column(String(5), nullable=True)
#     VENDOR_ID = Column(String(10), nullable=True)
#     NAME1 = Column(String(50), nullable=True)
#     NAME2 = Column(String(50), nullable=True)
#     VENDOR_STATUS = Column(String(1), nullable=True)
#     DEFAULT_LOC = Column(String(10), nullable=True)
#     VENDOR_LOC = Column(String(10), nullable=True)
#     VNDR_LOC_EFFDT = Column(String(10), nullable=True)
#     VNDR_LOC_EFF_STATUS = Column(String(1), nullable=True)
#     CURRENCY_CD = Column(String(3), nullable=True)
#     ADDRESS_SEQ_NUM = Column(Integer, nullable=True)
#     VNDR_ADDR_EFFDT = Column(String(10), nullable=True)
#     VNDR_ADDR_EFF_STATUS = Column(String(1), nullable=True)
#     ADDRESS1 = Column(String(55), nullable=True)
#     ADDRESS2 = Column(String(55), nullable=True)
#     ADDRESS3 = Column(String(55), nullable=True)
#     ADDRESS4 = Column(String(55), nullable=True)
#     CITY = Column(String(20), nullable=True)
#     STATE = Column(String(10), nullable=True)
#     POSTAL = Column(String(12), nullable=True)
#     COUNTRY = Column(String(3), nullable=True)


class PFGAccount(Base):
    __tablename__ = "pfgaccount"
    ID = Column(Integer, primary_key=True, autoincrement=True)
    SETID = Column(String(5), nullable=True)
    ACCOUNT = Column(String(10), nullable=True)
    EFFDT = Column(String(50), nullable=True)
    EFF_STATUS = Column(String(1), nullable=True)
    DESCR = Column(String(50), nullable=True)
    DESCRSHORT = Column(String(10), nullable=True)


class PFGProject(Base):
    __tablename__ = "pfgproject"
    ID = Column(Integer, primary_key=True, autoincrement=True)
    BUSINESS_UNIT = Column(String(5), nullable=True)
    PROJECT_ID = Column(String(15), nullable=True)
    EFF_STATUS = Column(String(1), nullable=True)
    DESCR = Column(String(50), nullable=True)
    START_DT = Column(String(50), nullable=True)
    END_DT = Column(String(50), nullable=True)


class PFGProjectActivity(Base):
    __tablename__ = "pfgprojectactivity"
    ID = Column(Integer, primary_key=True, autoincrement=True)
    BUSINESS_UNIT = Column(String(5), nullable=True)
    PROJECT_ID = Column(String(15), nullable=True)
    ACTIVITY_ID = Column(String(15), nullable=True)
    EFF_STATUS = Column(String(1), nullable=True)
    DESCR = Column(String(50), nullable=True)


class PFGReceipt(Base):
    __tablename__ = "pfgreceipt"
    ID = Column(Integer, primary_key=True, autoincrement=True)
    BUSINESS_UNIT = Column(String(5), nullable=True)
    RECEIVER_ID = Column(String(10), nullable=True)
    # BILL_OF_LADING = Column(String(30), nullable=True)
    INVOICE_ID = Column(String(30), nullable=True)
    RECEIPT_DT = Column(String(10), nullable=True)
    SHIPTO_ID = Column(String(10), nullable=True)
    VENDOR_SETID = Column(String(5), nullable=True)
    VENDOR_ID = Column(String(10), nullable=True)
    RECV_STATUS = Column(String(1), nullable=True)
    RECV_LN_NBR = Column(Integer, nullable=True)
    RECV_SHIP_SEQ_NBR = Column(Integer, nullable=True)
    DISTRIB_LINE_NUM = Column(Integer, nullable=True)
    MERCHANDISE_AMT = Column(Float, nullable=True)
    ACCOUNT = Column(String(10), nullable=True)
    DEPTID = Column(String(10), nullable=True)
    LOCATION = Column(String(10), nullable=True)

class PFGStrategicLedger(Base):
    __tablename__ = "pfgstrategicledger"
    ID = Column(Integer, primary_key=True, autoincrement=True)
    SETID = Column(String(5), nullable=True)
    CHARTFIELD1 = Column(String(10), nullable=True)
    EFFDT = Column(String(10), nullable=True)
    EFF_STATUS = Column(String(1), nullable=True)
    DESCR = Column(String(50), nullable=True)
    DESCRSHORT = Column(String(10), nullable=True)
    

class StampData(Base):
    __tablename__ = "stampdata"
    STAMP_ID = Column(Integer, primary_key=True, autoincrement=True)
    DOCUMENT_ID = Column(Integer, nullable=False)
    CREATED_ON = Column(DateTime, nullable=True)
    UPDATED_ON = Column(DateTime, nullable=True)
    DEPTNAME = Column(String, nullable=True)
    RECEIVING_DATE = Column(String, nullable=True)
    CONFIRMATION_NUMBER = Column(String, nullable=True)
    RECEIVER = Column(String, nullable=True)
    SELECTED_DEPT = Column(String, nullable=True)
    storenumber = Column(String, nullable=True)


class StampDataValidation(Base):
    __tablename__ = "stampdatavalidation"
    StampDataValidationID = Column(Integer, primary_key=True, autoincrement=True)
    documentid = Column(Integer, nullable=False)
    stamptagname = Column(String, nullable=True)
    stampvalue = Column(String, nullable=True)
    is_error = Column(Integer, nullable=True)
    IsUpdated = Column(Integer, nullable=True)
    created_on = Column(DateTime, nullable=True)
    UpdatedOn = Column(DateTime, nullable=True)
    OldValue = Column(String, nullable=True)
    errordesc = Column(String, nullable=True)
    skipconfig_ck = Column(Integer, nullable=True)


class frtrigger_tab(Base):
    __tablename__ = "frtrigger_tab"

    frtrigger_id = Column(Integer, primary_key=True, autoincrement=True)
    splitdoc_id = Column(Integer, nullable=True)
    pagecount = Column(Integer, nullable=True)
    prebuilt_headerdata = Column(JSONB, nullable=True)
    prebuilt_linedata = Column(JSONB, nullable=True)
    blobpath = Column(String(350), nullable=True)
    vendorID = Column(String(250), nullable=True)
    status = Column(String, nullable=True)
    created_on = Column(DateTime, nullable=True)
    sender = Column(String, nullable=True)
    page_number = Column(String, nullable=True)
    filesize = Column(String, nullable=True)
    documentid = Column(Integer, nullable=True)


class VoucherData(Base):
    __tablename__ = "voucherdata"
    voucherdataID = Column(Integer, primary_key=True, autoincrement=True)
    documentID = Column(Integer, nullable=False)
    Business_unit = Column(String(5), nullable=True)
    Invoice_Id = Column(String(30), nullable=True)
    Invoice_Dt = Column(String(100), nullable=True)
    Vendor_Setid = Column(String(5), nullable=True)
    Vendor_ID = Column(String(100), nullable=True)
    Origin = Column(String(100), nullable=True)
    Gross_Amt = Column(Float, nullable=True)
    Voucher_Line_num = Column(Integer, nullable=True)
    Merchandise_Amt = Column(Float, nullable=True)
    Distrib_Line_num = Column(Integer, nullable=True)
    Account = Column(String(100), nullable=True)
    Deptid = Column(String(100), nullable=True)
    Image_Nbr = Column(Integer, nullable=True)
    File_Name = Column(String, nullable=True)
    storenumber = Column(String, nullable=True)
    storetype = Column(String, nullable=True)
    receiver_id = Column(String, nullable=True)
    status = Column(String, nullable=True)
    recv_ln_nbr = Column(Integer, nullable=True)
    gst_amt = Column(Float, nullable=True)
    currency_code = Column(String, nullable=True)
    freight_amt = Column(Float, nullable=True)
    misc_amt = Column(Float, nullable=True)
    vat_applicability = Column(String, nullable=True)


class NonintegratedStores(Base):
    __tablename__ = "nonintegrated_stores"

    nonintegrated_id = Column(Integer, primary_key=True, index=True)
    store_name = Column(String(345), nullable=True)
    store_number = Column(Integer, nullable=True)
    created_on = Column(DateTime, nullable=True)
    updated_on = Column(DateTime, nullable=True)
    created_by = Column(String(145), nullable=True)


class SplitDocTab(Base):
    __tablename__ = "splitdoctab"

    splitdoc_id = Column(Integer, primary_key=True, autoincrement=True)
    invoice_path = Column(TEXT, nullable=True)
    emailbody_path = Column(TEXT, nullable=True)
    created_on = Column(DateTime, nullable=True)
    totalpagecount = Column(Integer, nullable=True)
    pages_processed = Column(TEXT, nullable=True)
    vendortype = Column(String(50), nullable=True)
    status = Column(String(50), nullable=True)
    email_subject = Column(String, nullable=True)
    sender = Column(String, nullable=True)
    updated_on = Column(DateTime, nullable=True)
    mail_row_key = Column(String, nullable=True)


class QueueTask(Base):
    __tablename__ = "queue_tasks"
    id = Column(Integer, primary_key=True, index=True)
    request_data = Column(JSONB, nullable=False, index=False)  # JSONB column
    status = Column(String(50), nullable=False, default="queued")
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(
        DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow
    )

    # Define a GIN index on the request_data column
    __table_args__ = (
        Index("idx_queue_tasks_request_data", "request_data", postgresql_using="gin"),
    )

class CorpQueueTask(Base):
    __tablename__ = "corp_queue_task"
    id = Column(Integer, primary_key=True, index=True)
    request_data = Column(JSONB, nullable=False, index=False)  # JSONB column
    status = Column(String(50), nullable=False, default="queued")
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(
        DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow
    )
    mail_row_key = Column(String(50), nullable=False)

    # Define a GIN index on the request_data column
    __table_args__ = (
        Index("idx_queue_tasks_request_data", "request_data", postgresql_using="gin"),
    )

class corp_document_tab(Base):
    __tablename__ = "corp_document_tab"

    corp_doc_id = Column(Integer, primary_key=True, autoincrement=True)
    invoice_id = Column(String(30), nullable=True)
    invoicetotal = Column(Float, nullable=True)
    gst = Column(Float, nullable=True)
    invo_filepath = Column(String(1255), nullable=True)
    email_filepath = Column(String(1255), nullable=True)
    approved_by = Column(String(45), nullable=True)
    vendor_code = Column(String(45), nullable=True)
    uploaded_date = Column(DateTime, nullable=True)
    approver_title = Column(String(45), nullable=True)
    last_updated_by = Column(String(45), nullable=True)
    vendor_id = Column(Integer, nullable=True)
    documentstatus = Column((Integer), nullable=True)
    documentsubstatus = Column((Integer), nullable=True)
    mail_row_key = Column((String), nullable=True)
    invo_page_count = Column((Integer), nullable=True)
    invoice_date = Column(String, nullable=True)
    created_on = Column(DateTime, nullable=True)
    updated_on = Column(DateTime, nullable=True)
    document_type = Column((String), nullable=True)
    sender = Column((String), nullable=True)
    voucher_id = Column((String), nullable=True)

class corp_coding_tab(Base):
    __tablename__ = "corp_coding_tab"

    corp_coding_id = Column(Integer, primary_key=True, autoincrement=True)
    invoice_id = Column(String(30), nullable=True)
    corp_doc_id = Column((Integer), nullable=True)
    coding_details = Column(JSON, nullable=True)
    supplier_id = Column(String, nullable=True)
    approver_name = Column(String, nullable=True)
    tmid = Column(String, nullable=True)
    approver_title = Column(String, nullable=True)
    invoicetotal = Column(Float, nullable=True)
    gst = Column(Float, nullable=True)
    voucher_status = Column(String, nullable=True)
    sent_erp = Column(DateTime, nullable=True)
    created_on = Column(DateTime, nullable=True)
    updated_on = Column(DateTime, nullable=True)
    filesize = Column(Float, nullable=True)
    sender_name = Column(String, nullable=True)
    sender_email = Column(String, nullable=True)
    sent_to = Column(String, nullable=True)
    sent_time = Column(String, nullable=True)
    approver_email = Column(String, nullable=True)
    approved_on = Column(String, nullable=True)
    approval_status = Column(String, nullable=True)
    document_type = Column(String, nullable=True)




class corp_docdata(Base):
    __tablename__ = "corp_docdata"

    docdata_id = Column(Integer, primary_key=True, autoincrement=True)
    invoice_id = Column(String(30), nullable=True)
    invoice_date = Column(String, nullable=True)
    vendor_name = Column(String, nullable=True)
    vendoraddress = Column(String, nullable=True)
    customeraddress = Column(String, nullable=True)
    customername = Column(String, nullable=True)
    currency = Column(String, nullable=True)
    invoicetotal = Column(Float, nullable=True)
    subtotal = Column(Float, nullable=True)

    corp_doc_id = Column(Integer, nullable=True)
    bottledeposit = Column(Float, nullable=True)
    shippingcharges =Column(Float, nullable=True)
    litterdeposit =Column(Float, nullable=True)
    gst = Column(Float, nullable=True)
    pst = Column(Float, nullable=True)
    pst_sk = Column(Float,nullable=True)
    pst_bc = Column(Float,nullable=True)
    ecology_fee = Column(Float,nullable=True)
    misc = Column(Float,nullable=True)
    created_on = Column(DateTime, nullable=True)
    pst_sk = Column(Float,nullable=True)
    pst_bc = Column(Float,nullable=True)
    ecology_fee = Column(Float,nullable=True)
    misc = Column(Float,nullable=True)
    doc_updates = Column(JSON, nullable=True)
    document_type = Column(String, nullable=True)

class corp_metadata(Base):
    __tablename__ = "corp_metadata"
    
    vendorcode = Column(String(50), nullable=False)
    vendorid = Column(Integer, nullable=False)
    synonyms_name = Column(TEXT, nullable=False)
    synonyms_address = Column(TEXT, nullable=False)
    dateformat = Column(String(50), nullable=False)
    status = Column(String(50), nullable=False)
    created_on = Column(DateTime, nullable=True)
    updated_on = Column(DateTime, nullable=True)
    metadata_id = Column(Integer, primary_key=True, index=True)
    currency = Column(String, nullable=True)
    vendorname = Column(String, nullable=True)
    vendoraddress = Column(String, nullable=True)



class corp_trigger_tab(Base):
    __tablename__ = "corp_trigger_tab"


    corp_trigger_id = Column(Integer, primary_key=True, autoincrement=True)
    corp_queue_id = Column(Integer, nullable=True)
    pagecount = Column(Integer, nullable=True)
    blobpath = Column(String(350), nullable=True)
    vendor_id = Column(Integer, nullable=True)
    status = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)
    sender = Column(String, nullable=True)
    filesize = Column(String, nullable=True)
    documentid = Column(Integer, nullable=True)
    mail_row_key = Column(String, nullable=True)
    
class CorpColumnNameDef(Base):

    __tablename__ = "corp_column_def" 

    id_column = Column(Integer, primary_key=True, index=True)
    column_name = Column(String(45), nullable=True)
    column_description = Column(String(60), nullable=True)
    table_name = Column(String(45), nullable=True)
    db_columnname = Column(String(45), nullable=True)
    created_on = Column(DateTime, nullable=True)
    updated_on = Column(DateTime, nullable=True)
    
    
class CorpDocumentColumnPos(Base):
    __tablename__ = "corp_doc_column"

    id_document_column = Column(Integer, primary_key=True, index=True)
    column_name_def_id = Column(Integer, nullable=True)
    document_column_pos = Column(Integer, nullable=True)
    user_id = Column(Integer, nullable=True)
    is_active = Column(Integer, nullable=True)
    tab_type = Column(Integer, nullable=True)
    created_on = Column(DateTime, nullable=True)
    updated_on = Column(DateTime, nullable=True)

class corp_hist_logs(Base):
    __tablename__ = "corp_hist_logs"
    document_desc = Column(TEXT, nullable=True)
    document_status = Column(Integer, nullable=True)
    document_substatus = Column(Integer, nullable=True)
    user_id =  Column(Integer, nullable=True)
    created_on = Column(DateTime, nullable=True)
    document_id = Column(Integer, nullable=True)
    histlog_id = Column(Integer, primary_key=True, autoincrement=True)

class CorpDocumentUpdates(Base): 
    __tablename__ = "corp_documentupdates" 

    iddocumentupdates = Column(Integer, primary_key=True, autoincrement=True) 
    doc_id = Column(Integer, nullable=True )
    updated_field = Column(Integer, nullable=True )
    is_active = Column(Integer, nullable=True) 
    old_value = Column(String, nullable=True) 
    new_value= Column(String, nullable=True) 
    created_on = Column(DateTime, nullable=True)
    user_id = Column(Integer, nullable=True)
    update_type = Column(String, nullable=True)
    


class CorpVoucherData(Base):
    __tablename__ = "corp_voucher_data"
    VOUCHER_ID = Column(Integer, primary_key=True, autoincrement=True)
    DOCUMENT_ID = Column(Integer, nullable=False)
    BUSINESS_UNIT = Column(String(5), nullable=True)
    INVOICE_ID = Column(String(30), nullable=True)
    INVOICE_DT = Column(String(100), nullable=True)
    VENDOR_SETID = Column(String(5), nullable=True)
    VENDOR_ID = Column(String(10), nullable=True)
    ORIGIN = Column(String(10), nullable=True)
    ACCOUNTING_DT = Column(String(10), nullable=True)
    GROSS_AMT = Column(Float, nullable=True)
    SALETX_AMT = Column(Float, nullable=True)
    FREIGHT_AMT = Column(Float, nullable=True)
    MISC_AMT = Column(Float, nullable=True)
    TXN_CURRENCY_CD = Column(String(3), nullable=True)
    VAT_ENTRD_AMT = Column(Float, nullable=True)
    VCHR_SRC = Column(String(5), nullable=True)
    OPRID = Column(String(10), nullable=True)
    MERCHANDISE_AMT = Column(Float, nullable=True)
    VCHR_DIST_STG = Column(JSON, nullable=True)
    VAT_APPLICABILITY = Column(String(1), nullable=True)
    SHIPTO_ID = Column(String(10), nullable=True)
    INVOICE_FILE_PATH = Column(String, nullable=True)
    EMAIL_PATH = Column(String, nullable=True)


class TaskSchedular(Base): 
    __tablename__ = "task_schedular" 

    schedular_id = Column(Integer, primary_key=True, autoincrement=True) 
    task_name = Column(String, nullable=True )
    time_interval = Column(Integer, nullable=True )
    user_id = Column(Integer, nullable=True) 
    is_active = Column(Integer, nullable=True) 
    updated_at= Column(DateTime, nullable=True) 
    updated_by = Column(String, nullable=True)
    

class SetRetryCount(Base): 
    __tablename__ = "set_retry_count" 

    set_id = Column(Integer, primary_key=True, autoincrement=True) 
    frequency = Column(Integer, nullable=True )
    user_id = Column(Integer, nullable=True) 
    is_active = Column(Integer, nullable=True) 
    updated_at= Column(DateTime, nullable=True) 
    updated_by = Column(String, nullable=True)
    task_name = Column(String, nullable=True )
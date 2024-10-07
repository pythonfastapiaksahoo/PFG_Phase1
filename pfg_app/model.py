from sqlalchemy import Column, Integer, MetaData, Table

from pfg_app.session.session import Base, engine

metadata = MetaData()
# creating class to load Dataswitch table


class Dataswitch(Base):
    __table__ = Table("dataswitch", Base.metadata, autoload=True, autoload_with=engine)


# Base = automap_base(bind=engine, metadata=metadata)


class DocumentModel(Base):
    __table__ = Table(
        "documentmodel", Base.metadata, autoload=True, autoload_with=engine
    )


# creating class to load DocumentType table


class DocumentType(Base):
    __table__ = Table(
        "documenttype", Base.metadata, autoload=True, autoload_with=engine
    )


# creating class to load Document table


class Document(Base):
    __table__ = Table("document", Base.metadata, autoload=True, autoload_with=engine)


# creating class to load Document table


class DocumentStatus(Base):
    __table__ = Table(
        "documentstatus", Base.metadata, autoload=True, autoload_with=engine
    )


# creating class to load DocumentData table


class DocumentData(Base):
    __table__ = Table(
        "documentdata", Base.metadata, autoload=True, autoload_with=engine
    )


# creating class to load DocumentTagDef table


class DocumentTagDef(Base):
    __table__ = Table(
        "documenttagdef", Base.metadata, autoload=True, autoload_with=engine
    )


# creating class to load DocumentTags table
# class DocumentTags(Base):
#     __table__ = Table('DocumentTags', Base.metadata,
#                           autoload=True, autoload_with=engine)

# creating class to load DocumentUpdates table


class DocumentUpdates(Base):
    __table__ = Table(
        "documentupdates", Base.metadata, autoload=True, autoload_with=engine
    )


# creating class to load DocumentStage table


class DocumentStage(Base):
    __table__ = Table(
        "documentstage", Base.metadata, autoload=True, autoload_with=engine
    )


# creating class to load DocumentTableDef table


class DocumentTableDef(Base):
    __table__ = Table(
        "documenttabledef", Base.metadata, autoload=True, autoload_with=engine
    )


# creating class to load DocumentLineItems table


class DocumentLineItems(Base):
    __table__ = Table(
        "documentlineitems", Base.metadata, autoload=True, autoload_with=engine
    )


# creating class to load DocumentLineItemTags table


class DocumentLineItemTags(Base):
    __table__ = Table(
        "documentlineitemtags", Base.metadata, autoload=True, autoload_with=engine
    )


class DocumentHistoryLogs(Base):
    __table__ = Table(
        "documenthistorylog", Base.metadata, autoload=True, autoload_with=engine
    )


# USER TABLES

# creating class to load customer table


class Customer(Base):
    __table__ = Table("customer", Base.metadata, autoload=True, autoload_with=engine)

    def datadict(self):
        d = {
            "idCustomer": self.idCustomer,
            "CustomerName": self.CustomerName,
        }
        return d


# creating class to load entitytype table


class EntityType(Base):
    __table__ = Table("entitytype", Base.metadata, autoload=True, autoload_with=engine)


# creating class to load entity table


class Entity(Base):
    __table__ = Table("entity", Base.metadata, autoload=True, autoload_with=engine)


# creating class to load entitybodytype table


class EntityBodyType(Base):
    __table__ = Table(
        "entitybodytype", Base.metadata, autoload=True, autoload_with=engine
    )


# creating class to load entitybody table


class EntityBody(Base):
    __table__ = Table("entitybody", Base.metadata, autoload=True, autoload_with=engine)


# creating class to load entitybody table


class Department(Base):
    __table__ = Table("department", Base.metadata, autoload=True, autoload_with=engine)


class UserAccess(Base):
    """Stores user access for entity and entity body."""

    __table__ = Table("useraccess", Base.metadata, autoload=True, autoload_with=engine)

    def datadict(self):
        d = {
            "idUserAccess": self.idUserAccess,
            "UserID": self.UserID,
            "EntityID": self.EntityID,
            "EntityBodyID": self.EntityBodyID,
            "CreatedBy": self.CreatedBy,
        }
        return d


# to store the login table
class Login_info(Base):
    __table__ = Table(
        "login_info_log", Base.metadata, autoload=True, autoload_with=engine
    )


# to store the otp and expiry date
class Otp_Code(Base):
    __table__ = Table(
        "password_otp", Base.metadata, autoload=True, autoload_with=engine
    )


# creating class to load entitybody table
class User(Base):
    __table__ = Table("user", Base.metadata, autoload=True, autoload_with=engine)

    def datadict(self):
        d = {
            "idUser": self.idUser,
            "customerID": self.customerID,
            "firstName": self.firstName,
            "lastName": self.lastName,
            "contact": self.contact,
            "UserCode": self.UserCode,
            "Designation": self.Designation,
            "email": self.email,
        }
        return d


# Vendor Table
class Vendor(Base):
    # __tablename__ = 'Vendor'
    __table__ = Table("vendor", Base.metadata, autoload=True, autoload_with=engine)


# VendorAccount Table
class VendorAccount(Base):
    # __tablename__ = 'VendorAccount'
    __table__ = Table(
        "vendoraccount", Base.metadata, autoload=True, autoload_with=engine
    )


# VendorUserAccess Table
class VendorUserAccess(Base):
    # __tablename__ = 'VendorAccount'
    __table__ = Table(
        "vendoruseraccess", Base.metadata, autoload=True, autoload_with=engine
    )


# Vendor Service Table
class ServiceProvider(Base):
    # __tablename__ = 'Service'
    __table__ = Table(
        "serviceprovider", Base.metadata, autoload=True, autoload_with=engine
    )


# ServiceAccount Table
class ServiceAccount(Base):
    # __tablename__ = 'ServiceAccount'
    __table__ = Table(
        "serviceaccount", Base.metadata, autoload=True, autoload_with=engine
    )


# SupplierSchedule Table
class ServiceProviderSchedule(Base):
    # __tablename__ = 'SupplierSchedule'
    __table__ = Table(
        "serivceproviderschedule", Base.metadata, autoload=True, autoload_with=engine
    )


# BatchTriggerHistory Table
class BatchTriggerHistory(Base):
    __table__ = Table(
        "batchprocesshistory", Base.metadata, autoload=True, autoload_with=engine
    )


# AccountCostAllocation Table
class AccountCostAllocation(Base):
    # __tablename__ = 'AccountCostAllocation'
    __table__ = Table(
        "accountcostallocation", Base.metadata, autoload=True, autoload_with=engine
    )


# Credentials Table


class Credentials(Base):
    # __tablename__ = 'AccountCostAllocation'
    __table__ = Table("credentials", Base.metadata, autoload=True, autoload_with=engine)


# Preparing the classes to reflect the existing table structure
# Base.prepare(engine, reflect=True)
# Permisiions


class AccessPermission(Base):
    """Holds access permission details of the user and vendor."""

    # __tablename__ = 'accesspermission'
    __table__ = Table(
        "accesspermission", Base.metadata, autoload=True, autoload_with=engine
    )

    def datadict(self):
        """Custom dictionary function to return only required columns :return:
        dictionary of selected columns."""
        d = {
            "idAccessPermission": self.idAccessPermission,
            "permissionDefID": self.permissionDefID,
            "userID": self.userID,
            "vendorUserID": self.vendorUserID,
            "approvalLevel": self.approvalLevel,
        }
        return d


# idAccessPermissionDef Table
class AccessPermissionDef(Base):
    """Stores the definition of the permission and permission id."""

    # __tablename__ = 'accesspermissiondef'
    __table__ = Table(
        "accesspermissiondef", Base.metadata, autoload=True, autoload_with=engine
    )

    def datadict(self):
        """Custom dictionary function to return only required columns :return:
        dictionary of selected columns."""
        d = {
            "idAccessPermissionDef": self.idAccessPermissionDef,
            "NameOfRole": self.NameOfRole,
            "Priority": self.Priority,
            "User": self.User,
            "Permissions": self.Permissions,
            "AccessPermissionTypeId": self.AccessPermissionTypeId,
            "NewInvoice": self.NewInvoice,
            "amountApprovalID": self.amountApprovalID,
        }
        return d


# AccessPermissionType Table
class AccessPermissionType(Base):
    # __tablename__ = 'accesspermissiontype'
    __table__ = Table(
        "accesspermissiontype", Base.metadata, autoload=True, autoload_with=engine
    )


# AmountApproveLevel Table
class AmountApproveLevel(Base):
    # __tablename__ = 'amountapprovelevel'
    __table__ = Table(
        "amountapprovelevel", Base.metadata, autoload=True, autoload_with=engine
    )

    def datadict(self):
        d = {
            "idAmountApproveLevel": self.idAmountApproveLevel,
            "MaxAmount": self.MaxAmount,
        }
        return d


#
class ColumnPosDef(Base):
    # __tablename__ = 'amountapprovelevel'
    __table__ = Table(
        "columnnamesdef", Base.metadata, autoload=True, autoload_with=engine
    )


class DocumentColumnPos(Base):
    # __tablename__ = 'amountapprovelevel'
    __table__ = Table(
        "documentcolumns", Base.metadata, autoload=True, autoload_with=engine
    )


# ----------- FR Tables ------------------

# FR configuration Table


class FRConfiguration(Base):
    __table__ = Table(
        "frconfigurations", Base.metadata, autoload=True, autoload_with=engine
    )


# FR Meta Data Table


class FRMetaData(Base):
    __table__ = Table("frmetadata", Base.metadata, autoload=True, autoload_with=engine)


# OCR Log Table


class OCRLogs(Base):
    __table__ = Table("ocrlogs", Base.metadata, autoload=True, autoload_with=engine)


# OCR UserItem Mapping


class UserItemMapping(Base):
    __table__ = Table(
        "useritemmapping", Base.metadata, autoload=True, autoload_with=engine
    )


# Application General config


class GeneralConfig(Base):
    __table__ = Table(
        "generalconfig", Base.metadata, autoload=True, autoload_with=engine
    )


# documentRules data


class Rule(Base):
    __table__ = Table(
        "documentrules", Base.metadata, autoload=True, autoload_with=engine
    )


class AGIRule(Base):
    __table__ = Table(
        "erprulecodes", Base.metadata, autoload=True, autoload_with=engine
    )


class DocumentSubStatus(Base):
    __table__ = Table(
        "documentsubstatus", Base.metadata, autoload=True, autoload_with=engine
    )


class DocumentRulemapping(Base):
    __table__ = Table(
        "docrulestatusmapping", Base.metadata, autoload=True, autoload_with=engine
    )


class DocumentRuleupdates(Base):
    __table__ = Table(
        "documentruleshistorylog", Base.metadata, autoload=True, autoload_with=engine
    )


class DocumentModelComposed(Base):
    __table__ = Table(
        "documentmodelcomposed", Base.metadata, autoload=True, autoload_with=engine
    )


# notification Models


class PullNotification(Base):
    __table__ = Table(
        "pullnotification", Base.metadata, autoload=True, autoload_with=engine
    )


class PullNotificationTemplate(Base):
    __table__ = Table(
        "pullnotificationtemplate", Base.metadata, autoload=True, autoload_with=engine
    )


class NotificationCategoryRecipient(Base):
    __table__ = Table(
        "notificationrecipents", Base.metadata, autoload=True, autoload_with=engine
    )


class TriggerDescription(Base):
    __table__ = Table(
        "triggerdescription", Base.metadata, autoload=True, autoload_with=engine
    )


class BatchErrorType(Base):
    __table__ = Table(
        "batcherrortypes", Base.metadata, autoload=True, autoload_with=engine
    )


class ItemMetaData(Base):
    __table__ = Table(
        "itemmetadata", Base.metadata, autoload=True, autoload_with=engine
    )


class ItemUserMap(Base):
    __table__ = Table("itemusermap", Base.metadata, autoload=True, autoload_with=engine)


class DefaultFields(Base):
    __table__ = Table(
        "defaultfields", Base.metadata, autoload=True, autoload_with=engine
    )


class AgiCostAlloc(Base):
    __table__ = Table(
        "agicostallocation", Base.metadata, autoload=True, autoload_with=engine
    )


class ItemMapUploadHistory(Base):
    __table__ = Table(
        "itemmappinguploadhistory", Base.metadata, autoload=True, autoload_with=engine
    )


class GrnReupload(Base):
    __table__ = Table("grnreupload", Base.metadata, autoload=True, autoload_with=engine)


class POLines(Base):
    __table__ = Table(
        "d3agi_poline", Base.metadata, autoload=True, autoload_with=engine
    )


class PaymentsInfo(Base):
    __table__ = Table(
        "invoicepaymentinfo", Base.metadata, autoload=True, autoload_with=engine
    )


class FrConfigData(Base):
    __table__ = Table(
        "frconfigurations", Base.metadata, autoload=True, autoload_with=engine
    )


class ERPTAGMAP(Base):
    __table__ = Table("erp_tag_map", Base.metadata, autoload=True, autoload_with=engine)


class RuleCodes(Base):
    __table__ = Table(
        "documentrulecode", Base.metadata, autoload=True, autoload_with=engine
    )


class ReleaseUpdate(Base):
    __table__ = Table(
        "release_updates", Base.metadata, autoload=True, autoload_with=engine
    )


class UserDepartment(Base):
    __table__ = Table(
        "user_department", Base.metadata, autoload=True, autoload_with=engine
    )


class RoveGrn(Base):
    __table__ = Table(
        "rove_grn",
        Base.metadata,
        Column("iddocumenthistorylog", Integer, primary_key=True),
        # Column('documentStatusID', Integer),
        # Column('documentSubStatusID'),
        # Column('userID', Integer),
        autoload=True,
        autoload_with=engine,
    )


class PFGDepartment(Base):
    __table__ = Table(
        "pfgdepartment", Base.metadata, autoload=True, autoload_with=engine
    )


class PFGStore(Base):
    __table__ = Table("pfgstore", Base.metadata, autoload=True, autoload_with=engine)


class PFGVendor(Base):
    __table__ = Table("pfgvendor", Base.metadata, autoload=True, autoload_with=engine)


class PFGAccount(Base):
    __table__ = Table("pfgaccount", Base.metadata, autoload=True, autoload_with=engine)


class PFGProject(Base):
    __table__ = Table("pfgproject", Base.metadata, autoload=True, autoload_with=engine)


class PFGProjectActivity(Base):
    __table__ = Table(
        "pfgprojectactivity", Base.metadata, autoload=True, autoload_with=engine
    )


class PFGReceipt(Base):
    __table__ = Table("pfgreceipt", Base.metadata, autoload=True, autoload_with=engine)


class StampData(Base):
    __table__ = Table("stampdata", Base.metadata, autoload=True, autoload_with=engine)


class frtrigger_tab(Base):
    __table__ = Table(
        "frtrigger_tab", Base.metadata, autoload=True, autoload_with=engine
    )


class Vendor3(Base):
    __table__ = Table("vendor3", Base.metadata, autoload=True, autoload_with=engine)


class VendorAccount3(Base):
    __table__ = Table(
        "vendoraccount3", Base.metadata, autoload=True, autoload_with=engine
    )


class StampDataValidation(Base):
    __table__ = Table(
        "stampdatavalidation", Base.metadata, autoload=True, autoload_with=engine
    )


class VoucherData(Base):
    __table__ = Table("voucherdata", Base.metadata, autoload=True, autoload_with=engine)


class NonintegratedStores(Base):
    __table__ = Table(
        "nonintegrated_stores", Base.metadata, autoload=True, autoload_with=engine
    )

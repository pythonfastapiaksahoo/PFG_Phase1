from typing import List, Optional

from pydantic import BaseModel


# new AmountApprovalLvl Structure
class Maxamount(BaseModel):
    applied_uid: int
    MaxAmount: int


class UpdateMaxamount(BaseModel):
    MaxAmount: int


# AccessPermissionDef Structure
class AccessPermissionDef(BaseModel):
    NameOfRole: str
    Priority: int
    User: bool
    isConfigPortal: bool
    is_epa: bool
    is_gpa: bool
    is_vspa: bool
    is_spa: bool
    Permissions: bool
    AccessPermissionTypeId: int
    NewInvoice: bool
    isDashboard: bool
    allowBatchTrigger: bool
    allowServiceTrigger: bool
    max_amount: Optional[int] = None


# new AccessPermission Structure
class AccessPermission(BaseModel):
    permissionDefID: AccessPermissionDef
    userID: Optional[int] = None
    vendorUserID: Optional[int] = None
    approvalLevel: Optional[Maxamount] = None


# Update AccessPermissionDef Structure
class UAccessPermissionDef(BaseModel):
    NameOfRole: str
    Priority: int
    User: bool
    isConfigPortal: bool
    is_epa: bool
    is_gpa: bool
    is_vspa: bool
    is_spa: bool
    is_grn_approval: bool
    Permissions: bool
    AccessPermissionTypeId: int
    NewInvoice: bool
    isDashboard: bool
    allowBatchTrigger: bool
    allowServiceTrigger: bool
    max_amount: Optional[int] = None


# Update AccessPermission Structure
class UAccessPermission(BaseModel):
    permissionDefID: Optional[UAccessPermissionDef] = None
    userID: Optional[int] = None
    vendorUserID: Optional[int] = None
    approvalLevel: Optional[Maxamount] = None


# new UserAccess Structure
class UserAccess(BaseModel):
    idUserAccess: Optional[int] = None
    EntityID: int
    EntityBodyID: Optional[int] = None
    DepartmentID: Optional[int] = None


# Update CustomerInvoiceAccess Structure
class UUserAccess(BaseModel):
    EntityID: int
    EntityBodyID: int


# new VendorUserAccess Structure
class VendorUserAccess(BaseModel):
    vendorUserID: Optional[int] = None
    vendorCode: str
    entityID: List[int]
    vendorAccountID: Optional[List[int]] = None


# update VendorUserAccess Structure
class UVendorUserAccess(BaseModel):
    idVendorUserAccess: Optional[List[int]] = None
    vendorCode: str
    entityID: List[int]
    vendorAccountID: Optional[List[int]] = None


class ApplyPermission(BaseModel):
    applied_uid: int
    appied_permission_def_id: int


class ApplyFiancialLevel(BaseModel):
    applied_uid: int
    applied_finacial_level_id: int


class UpdateServiceSchedule(BaseModel):
    schedule: str
    isScheduleActive: bool
    isTriggerActive: bool

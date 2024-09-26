from datetime import datetime
from typing import Optional

from pydantic import BaseModel, EmailStr


# new Vendor structure
class Vendor(BaseModel):
    VendorName: str
    Address: Optional[str]
    City: Optional[str]
    Country: Optional[str]
    Desc: Optional[str]
    VendorCode: Optional[str]
    Email: Optional[str]
    Contact: Optional[str]
    Website: Optional[str]
    Salutation: Optional[str]
    FirstName: Optional[str]
    LastName: Optional[str]
    Designation: Optional[str]
    TradeLicense: Optional[str]
    VATLicense: Optional[str]
    TLExpiryDate: datetime
    VLExpiryDate: datetime
    TRNNumber: Optional[str]
    Synonyms: Optional[str]


# new Vendor Account Structure
class VendorAccount(BaseModel):
    Account: str
    entityID: Optional[int]
    entityBodyID: Optional[int]
    City: Optional[str]
    Country: Optional[str]
    LocationCode: Optional[str]


# new Vendor Invoice Access Structure
class VendorInvoiceAccess(BaseModel):
    vendorUserID: int
    vendorAccountID: int
    accessPermissionID: int
    updatedBy: int


# new Vendor User Structure
class VendorUser(BaseModel):
    UserName: str
    Email: EmailStr
    Contact: str
    LogSecret: str
    idAccessPermissionDef: Optional[int] = None


# # new Vendor Invoice Access Structure # TODO this was commented because it is already defined above
# class VendorInvoiceAccess(BaseModel):
#     vendorAccountID: int
#     accessPermissionID: int
#     updatedBy: int


# Update Vendor Structure
class UpdateVendor(BaseModel):
    VendorName: Optional[str] = None
    Address: Optional[str] = None
    City: Optional[str] = None
    Country: Optional[str] = None
    Desc: Optional[str] = None
    VendorCode: Optional[str] = None
    Email: Optional[str] = None
    Contact: Optional[str] = None
    Website: Optional[str] = None
    Salutation: Optional[str] = None
    FirstName: Optional[str] = None
    LastName: Optional[str] = None
    Designation: Optional[str] = None
    TradeLicense: Optional[str] = None
    VATLicense: Optional[str] = None
    TLExpiryDate: Optional[datetime] = None
    VLExpiryDate: Optional[datetime] = None
    TRNNumber: Optional[str] = None


# Update Vendor Account Structure
class UpdateVendorAccount(BaseModel):
    Account: Optional[str] = None
    entityID: Optional[int] = None
    entityBodyID: Optional[int] = None
    City: Optional[str] = None
    Country: Optional[str] = None
    LocationCode: Optional[str] = None


# Update Vendor User Structure
class UVendorUser(BaseModel):
    UserName: Optional[str] = None
    Email: Optional[EmailStr] = None
    Contact: Optional[str] = None
    idAccessPermissionDef: Optional[int] = None


# Update Vendor Invoice Access Structure
class UVendorInvoiceAccess(BaseModel):
    vendorAccountID: Optional[int] = None
    accessPermissionID: Optional[int] = None
    updatedBy: Optional[int] = None

import sys
from typing import List, Optional

from pydantic import BaseModel, EmailStr
from schemas import permissionssm

sys.path.append("..")

# class Customer(BaseModel):
# CustomerName: str


class User(BaseModel):
    firstName: Optional[str] = None
    lastName: Optional[str] = None
    contact: Optional[str] = None
    UserCode: Optional[str] = None
    Designation: Optional[str] = None
    email: EmailStr
    role_id: int
    dept_id: int
    userentityaccess: List[permissionssm.UserAccess]


class VendorUser(BaseModel):
    tempVendorName: Optional[str] = None
    firstName: Optional[str] = None
    lastName: Optional[str] = None
    contact: Optional[str] = None
    UserCode: Optional[str] = None
    Designation: Optional[str] = None
    email: EmailStr
    role_id: int
    uservendoraccess: Optional[List[permissionssm.VendorUserAccess]] = None


class UUser(BaseModel):
    # idUser: int
    firstName: Optional[str] = None
    lastName: Optional[str] = None
    UserCode: Optional[str] = None
    Designation: Optional[str] = None
    email: Optional[EmailStr] = None
    dept_id: Optional[int] = None


class CustomerAndPermissionDetails(BaseModel):
    # Customer: Customer
    User: User
    UserAccessPermission: permissionssm.AccessPermission


class UCustomerAndPermissionDetails(BaseModel):
    # Customer: Optional[Customer]
    User: Optional[UUser]
    userentityaccess: Optional[List[permissionssm.UserAccess]]


class UVendorUserAndPermissionDetails(BaseModel):
    # Customer: Optional[Customer]
    User: Optional[UUser]
    uservendoraccess: Optional[permissionssm.UVendorUserAccess]


class UDepartment(BaseModel):
    idDepartment: int
    entityID: Optional[int] = None
    entityBodyID: Optional[int] = None
    DepartmentName: Optional[str] = None


class UEntity(BaseModel):
    idEntity: int
    customerID: Optional[int] = None
    EntityName: Optional[str] = None
    EntityAddress: Optional[str] = None
    City: Optional[str] = None
    Country: Optional[str] = None
    entityTypeID: Optional[int] = None
    EntityCode: Optional[str] = None


class UEntityBody(BaseModel):
    idEntityBody: int
    EntityBodyName: Optional[str] = None
    EntityCode: Optional[str] = None
    Address: Optional[str] = None
    LocationCode: Optional[str] = None
    City: Optional[str] = None
    Country: Optional[str] = None
    EntityID: Optional[int] = None
    entityBodyTypeID: Optional[int] = None


class UEntityDept(BaseModel):
    Department: Optional[UDepartment] = None
    Entity: Optional[UEntity] = None


class UEntityBodyDept(BaseModel):
    Department: Optional[UDepartment] = None
    EntityBody: Optional[UEntityBody] = None


class Credentials(BaseModel):
    LogName: str
    LogSecret: str
    userID: Optional[int] = None
    # entityID:Optional[int] = None
    # entityBodyID:Optional[int] = None


class UCredentials(BaseModel):
    LogName: str
    LogSecret: str
    entityID: Optional[int] = None
    entityBodyID: Optional[int] = None


class UPassword(BaseModel):
    old_pass: str
    new_pass: str

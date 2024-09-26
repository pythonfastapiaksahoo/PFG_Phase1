from typing import List, Optional

from pydantic import BaseModel


# Service Provider Schemas
# new Service Structure
class ServiceProvider(BaseModel):
    ServiceProviderName: str
    ServiceProviderCode: Optional[str]
    City: Optional[str]
    Country: Optional[str]
    LocationCode: Optional[str]


# new Service Account Structure
class ServiceAccount(BaseModel):
    Account: str
    entityID: int
    entityBodyID: Optional[int] = None
    Email: str
    isActive: int
    operatingUnit: Optional[str]
    MeterNumber: Optional[str]
    LocationCode: Optional[str]
    Address: Optional[str]


# new and update Service Supplier Schedule Structure
class ServiceProviderSchedule(BaseModel):
    ScheduleDateTime: str


# new Account Cost Allocation Structure
class AccountCostAllocation(BaseModel):
    entityID: int
    entityBodyID: int
    Element: Optional[str] = None
    interco: Optional[str] = None
    description: Optional[str] = None
    mainAccount: Optional[str] = None
    naturalAccountWater: Optional[str] = None
    naturalAccountHousing: Optional[str] = None
    costCenter: Optional[str] = None
    product: Optional[str] = None
    project: Optional[str] = None
    elementFactor: Optional[float] = None
    segments: Optional[str] = None
    bsMovements: Optional[str] = None
    fixedAssetDepartment: Optional[str] = None
    fixedAssetGroup: Optional[str] = None
    isActive_Alloc: bool


# update Account Cost Allocation Structure
class UAccountCostAllocation(BaseModel):
    idAccountCostAllocation: Optional[int] = None
    entityID: int
    entityBodyID: int
    Element: Optional[str] = None
    interco: Optional[str] = None
    description: Optional[str] = None
    mainAccount: Optional[str] = None
    naturalAccountWater: Optional[str] = None
    naturalAccountHousing: Optional[str] = None
    costCenter: Optional[str] = None
    product: Optional[str] = None
    project: Optional[str] = None
    elementFactor: Optional[float] = None
    segments: Optional[str] = None
    bsMovements: Optional[str] = None
    fixedAssetDepartment: Optional[str] = None
    fixedAssetGroup: Optional[str] = None
    isActive_Alloc: bool


# Update Service Structure
class UService(BaseModel):
    SupplierName: Optional[str] = None
    City: Optional[str] = None
    Country: Optional[str] = None
    LocationCode: Optional[str] = None


# new Account Cost Allocation Structure
class Credentials(BaseModel):
    UserName: str
    LogSecret: str
    URL: str
    entityID: int
    entityBodyID: Optional[int] = None


# trigger body
class TriggerBody(BaseModel):
    entity_ids: Optional[List[int]] = None

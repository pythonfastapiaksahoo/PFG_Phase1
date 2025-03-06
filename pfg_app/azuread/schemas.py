from typing import List, Optional

from pydantic import BaseModel


class AzureUser(BaseModel):
    id: str
    name: str
    email: Optional[str] = None
    preferred_username: str
    roles: List[str]
    employeeId: Optional[str] = None
# class AzureUser(BaseModel):
#     oid: str
#     # name: str
#     # email: str
#     preferred_username: str
#     roles: List[str]
#     employeeId: Optional[str]

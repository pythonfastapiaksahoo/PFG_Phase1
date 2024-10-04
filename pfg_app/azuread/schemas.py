from typing import List

from pydantic import BaseModel


class AzureUser(BaseModel):
    id: str
    name: str
    email: str
    preferred_username: str
    roles: List[str]

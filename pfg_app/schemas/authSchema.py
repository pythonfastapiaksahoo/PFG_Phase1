from typing import Optional

from pydantic import BaseModel


class AuthDetails(BaseModel):
    username: str
    password: str
    type: str


class Token(BaseModel):
    token: str


class ActivationBody(BaseModel):
    activation_code: str
    password: str


class ReleaseUpdate(BaseModel):
    releaseno: str
    heading: str
    description: str
    link: Optional[str]

from typing import Any

from fastapi import Depends, HTTPException, status

from pfg_app.azuread.AzureADAuthorization import authorize
from pfg_app.azuread.schemas import AzureUser


class ForbiddenAccess(HTTPException):
    def __init__(self, detail: Any = None) -> None:
        super().__init__(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=detail,
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_user(user: AzureUser = Depends(authorize)) -> AzureUser:
    return user


def get_admin_user(user: AzureUser = Depends(authorize)) -> AzureUser:
    if "Admin" in user.roles:
        return user
    raise ForbiddenAccess("Admin privileges required")

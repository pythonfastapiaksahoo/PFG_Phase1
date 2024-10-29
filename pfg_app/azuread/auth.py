from typing import Any

from fastapi import Depends, HTTPException, status

from pfg_app.azuread.AzureADAuthorization import authorize
from pfg_app.azuread.schemas import AzureUser
from pfg_app.model import User
from pfg_app.session.session import Session, get_db


class ForbiddenAccess(HTTPException):
    def __init__(self, detail: Any = None) -> None:
        super().__init__(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=detail,
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_user(
    user: AzureUser = Depends(authorize),
    db: Session = Depends(get_db),
) -> AzureUser:

    user = AzureUser(
        id="generic_id",
        name="Generic User",
        email="generic_email",
        preferred_username="Generic User",
    )
    # check if this user exists in the database agaisnt user tabel
    user_in_db = db.query(User).filter(User.azure_id == user.id).first()

    # if user does not exist in the database, create a new user
    if not user_in_db:
        user_in_db = User(
            azure_id=user.id,
            email=user.email,
            customerID=1,
            firstName=user.name,
        )
        db.add(user_in_db)
        db.commit()
        db.refresh(user_in_db)

    return user_in_db


def get_admin_user(user: AzureUser = Depends(authorize)) -> AzureUser:
    user = AzureUser(
        id="generic_id",
        name="Test User",
        email="generic_email",
        roles=["Admin"],
        preferred_username="Generic admin",
    )
    if "Admin" in user.roles:
        return user
    raise ForbiddenAccess("Admin privileges required")

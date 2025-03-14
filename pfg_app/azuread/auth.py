from typing import Any, List

from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session

from pfg_app.azuread.AzureADAuthorization import authorize
from pfg_app.azuread.schemas import AzureUser
from pfg_app.logger_module import logger
from pfg_app.model import User
from pfg_app.session.session import get_db


class ForbiddenAccess(HTTPException):
    def __init__(self, detail: Any = None) -> None:
        super().__init__(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=detail,
            headers={"WWW-Authenticate": "Bearer"},
        )

    # )
    #     "roles": [
    #     "CORP_ConfigPortal_User",
    #     "DSD_APPortal_User",
    #     "DSD_ConfigPortal_User",
    #     "User",
    #     "Admin",
    #     "CORP_APPortal_User"
    # ],

def get_user(
    user: AzureUser = Depends(authorize),
    db: Session = Depends(get_db),
) -> AzureUser:

    if "User" in user.roles:
        # base_user = AzureUser(
        #     id="generic_id",
        #     name="Generic User",
        #     email="generic_email",
        #     preferred_username="Generic User",
        #     roles=user.roles,
        # )
        # check if this user exists in the database agaisnt user tabel
        user_in_db = db.query(User).filter(User.azure_id == user.id).first()

        # if user does not exist in the database, create a new user
        if not user_in_db:
            user_in_db = User(
                azure_id=user.id,
                email=user.preferred_username,
                employee_id=user.employeeId,
                customerID=1,
                firstName=user.name,
            )
            db.add(user_in_db)
            db.commit()
            db.refresh(user_in_db)
        else:
            # Update fields only if they do not match
            updated = False
            
            if user_in_db.firstName != user.name:
                user_in_db.firstName = user.name
                updated = True
            
            if user_in_db.email != user.preferred_username:
                user_in_db.email = user.preferred_username
                updated = True
            
            if user_in_db.user_role != ",".join(user.roles):
                user_in_db.user_role = ",".join(user.roles)
                updated = True
            
            if user_in_db.employee_id != user.employeeId:
                user_in_db.employee_id = user.employeeId
                updated = True
            
            if updated:
                db.commit()
                db.refresh(user_in_db)
        return user_in_db
    raise ForbiddenAccess("User privileges required")


def get_user_dependency(allowed_roles: list[str]):
    def dependency(
        user: AzureUser = Depends(authorize), 
        db: Session = Depends(get_db)
    ) -> AzureUser:
        if any(role in user.roles for role in allowed_roles):
            user_in_db = db.query(User).filter(User.azure_id == user.id).first()

            if not user_in_db:
                # Insert new user
                user_in_db = User(
                    azure_id=user.id,
                    email=user.preferred_username,
                    # preferred_username=user.preferred_username,
                    user_role=",".join(user.roles),  # Store roles as comma-separated values
                    employee_id=user.employeeId,
                    customerID=1,
                    firstName=user.name
                )
                db.add(user_in_db)
                db.commit()
                db.refresh(user_in_db)
            else:
                # Update fields only if they do not match
                updated = False
                
                if user_in_db.firstName != user.name:
                    user_in_db.firstName = user.name
                    updated = True
                
                if user_in_db.email != user.preferred_username:
                    user_in_db.email = user.preferred_username
                    updated = True
                
                if user_in_db.user_role != ",".join(user.roles):
                    user_in_db.user_role = ",".join(user.roles)
                    updated = True
                
                if user_in_db.employee_id != user.employeeId:
                    user_in_db.employee_id = user.employeeId
                    updated = True
                
                if updated:
                    db.commit()
                    db.refresh(user_in_db)

            return user_in_db
        
        raise HTTPException(status_code=403, detail= f"{allowed_roles} is required to access this resource.")

    return dependency

def get_admin_user(
    user: AzureUser = Depends(authorize), db: Session = Depends(get_db)
) -> AzureUser:

    # base_user = AzureUser(
    #     id="generic_id",
    #     name="Test User",
    #     email="generic_email",
    #     roles=["Admin"],
    #     preferred_username="Generic admin",
    # )
    #     "roles": [
    #     "CORP_ConfigPortal_User",
    #     "DSD_APPortal_User",
    #     "DSD_ConfigPortal_User",
    #     "User",
    #     "Admin",
    #     "CORP_APPortal_User"
    # ],
    if "Admin" in user.roles:
        try:
            all_results = []
            current_schema = db.execute("SELECT current_schema();").fetchall()
            all_results.append({"current_schema": current_schema})
            tables = db.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'pfg_schema';"
            ).fetchall()
            all_results.append({"tables_in_schema": tables})
        except Exception as e:
            all_results.append({"error": str(e)})
        finally:
            logger.info(all_results)

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
        else:
            # Update fields only if they do not match
            updated = False
            
            if user_in_db.firstName != user.name:
                user_in_db.firstName = user.name
                updated = True
            
            if user_in_db.email != user.preferred_username:
                user_in_db.email = user.preferred_username
                updated = True
            
            if user_in_db.user_role != ",".join(user.roles):
                user_in_db.user_role = ",".join(user.roles)
                updated = True
            
            if user_in_db.employee_id != user.employeeId:
                user_in_db.employee_id = user.employeeId
                updated = True
            
            if updated:
                db.commit()
                db.refresh(user_in_db)
        return user_in_db
    raise ForbiddenAccess("Admin privileges required")

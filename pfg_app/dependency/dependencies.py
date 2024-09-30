import traceback

from fastapi import Depends, HTTPException
from sqlalchemy.orm import scoped_session

import pfg_app.model as models
from pfg_app.logger_module import logger
from pfg_app.schemas import permissionssm
from pfg_app.session import Session


def get_db():
    """This function yields a DB session object if the connection is
    established with the backend DB, takes in  no parameter.

    :return: It returns a DB session Object to the calling function.
    """
    db = Session()
    try:
        yield db
    finally:
        db.close()


# check user create permission
async def check_create_user(u_id: int, db: scoped_session = Depends(get_db)):
    """Function to check if user has create user permission.

    :param u_id: It is a path parameters that is of integer type, it
        provides the user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: return user id or raise an exception
    """
    try:
        sub_query = db.query(models.AccessPermission.permissionDefID).filter_by(
            userID=u_id
        )
        main_query = (
            db.query(models.AccessPermissionDef.User)
            .filter_by(idAccessPermissionDef=sub_query)
            .scalar()
        )
        if main_query == 1:
            return u_id
        raise HTTPException(status_code=403, detail="Permission Denied")
    except Exception:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=403, detail="Permission Denied")


# check vendor user create permission
async def check_create_vendor_user(u_id: int, db: scoped_session = Depends(get_db)):
    """Function to check if vendor user has create user permission.

    :param u_id: It is a path parameters that is of integer type, it
        provides the user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: return user id or raise an exception
    """
    try:
        sub_query = db.query(models.AccessPermission.permissionDefID).filter_by(
            userID=u_id
        )
        main_query = (
            db.query(models.AccessPermissionDef.User)
            .filter_by(idAccessPermissionDef=sub_query)
            .scalar()
        )
        if main_query == 1:
            return u_id
        raise HTTPException(status_code=403, detail="Permission Denied")
    except Exception:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=403, detail="Permission Denied")


# check user update permission
async def check_update_user(u_id: int, db: scoped_session = Depends(get_db)):
    """Function to check if user has update user permission.

    :param u_id: It is a path parameters that is of integer type, it
        provides the user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: return user id or raise an exception
    """
    try:
        # sub_query = db.query(models.AccessPermission.permissionDefID).filter_by(userID=u_id)
        # main_query = db.query(models.AccessPermissionDef.Permissions).filter_by(idAccessPermissionDef=sub_query).scalar()
        # if main_query == 1:
        return u_id
        raise HTTPException(status_code=403, detail="Permission Denied")
    except Exception:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=403, detail="Permission Denied")


# check vendor user update permission
async def check_update_vendor_user(vu_id: int, db: scoped_session = Depends(get_db)):
    """Function to check if vendor user has update user permission.

    :param u_id: It is a path parameters that is of integer type, it
        provides the user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: return user id or raise an exception
    """
    try:
        sub_query = db.query(models.AccessPermission.permissionDefID).filter_by(
            userID=vu_id
        )
        main_query = (
            db.query(models.AccessPermissionDef.Permissions)
            .filter_by(idAccessPermissionDef=sub_query)
            .scalar()
        )
        if main_query == 1:
            return vu_id
        raise HTTPException(status_code=403, detail="Permission Denied")
    except Exception:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=403, detail="Permission Denied")


# check user invoice upload permission
async def check_user_invoice(u_id: int, db: scoped_session = Depends(get_db)):
    """Function to check if user has upload invoice permission.

    :param u_id: It is a path parameters that is of integer type, it
        provides the user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: return user id or raise an exception
    """
    try:
        sub_query = db.query(models.AccessPermission.permissionDefID).filter_by(
            userID=u_id
        )
        main_query = (
            db.query(models.AccessPermissionDef.NewInvoice)
            .filter_by(idAccessPermissionDef=sub_query)
            .scalar()
        )
        if main_query == 1:
            return u_id
        raise HTTPException(status_code=403, detail="Permission Denied")
    except Exception:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=403, detail="Permission Denied")


# check vendor user invoice upload permission
async def check_upload_vendor_user(vu_id: int, db: scoped_session = Depends(get_db)):
    """Function to check if vendor user has upload invoice permission.

    :param u_id: It is a path parameters that is of integer type, it
        provides the user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: return user id or raise an exception
    """
    try:
        sub_query = db.query(models.AccessPermission.permissionDefID).filter_by(
            vendorUserID=vu_id
        )
        main_query = (
            db.query(models.AccessPermissionDef.NewInvoice)
            .filter_by(idAccessPermissionDef=sub_query)
            .scalar()
        )
        if main_query == 1:
            return vu_id
        raise HTTPException(status_code=403, detail="Permission Denied")
    except Exception:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=403, detail="Permission Denied")


# check if user has permission to apprve the amount for the invoice entity
async def check_invoice_entity_user(u_id, inv_id, db: scoped_session = Depends(get_db)):
    """Function to check if vendor user has upload invoice permission.

    :param u_id: It is a path parameters that is of integer type, it
        provides the user Id.
    :param db: It provides a session to interact with the backend
        Database,that is of Session Object Type.
    :return: return user id or raise an exception
    """
    try:
        # db: scoped_session = next(get_db())
        # can include logic for entity body if needed in future
        # sub query to get entity of invoice
        sub_query = db.query(models.Document.entityID).filter_by(idDocument=inv_id)
        # get iduseraccess to check if any access is avilable for the user on
        # the given entity
        data = (
            db.query(models.UserAccess.idUserAccess)
            .filter_by(EntityID=sub_query.scalar_subquery(), UserID=u_id)
            .all()
        )
        if len(data) > 0:
            return True
        else:
            return False
        # raise HTTPException(status_code=403, detail="Permission Denied")
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=403, detail=f"Permission Denied {e}")


# check if user has permission to update other vendor users
def check_vendor_user_update(vu_id):
    """

    :param v_id:
    :param db:
    :return:
    """
    try:
        db: scoped_session = next(get_db())
        # sub query to get permission def id from access permission using vu_id
        sub_query = db.query(models.AccessPermission.permissionDefID).filter_by(
            userID=vu_id
        )
        # getting the column related to update user permission
        permissionbool = (
            db.query(models.AccessPermissionDef.Permissions)
            .filter_by(idAccessPermissionDef=sub_query)
            .one()
        )
        if permissionbool.Permissions == 0:
            raise HTTPException(status_code=403, detail="Permission Denied")
        return vu_id
    except Exception:
        logger.error(traceback.format_exc())
        print(traceback.print_exc())
        raise HTTPException(status_code=403, detail="Permission Denied")


def check_eidt_invoice_approve_permission(u_id, inv_id):
    try:
        db: scoped_session = next(get_db())
        # sub query to get permission def id from access permission using u_id
        sub_query = db.query(models.AccessPermission.permissionDefID).filter_by(
            userID=u_id
        )
        # getting the column related to update user permission type for invoice
        inv_perm = (
            db.query(models.AccessPermissionDef.AccessPermissionTypeId)
            .filter_by(idAccessPermissionDef=sub_query.scalar_subquery())
            .one()
        )
        # check if edit permission is there
        if inv_perm.AccessPermissionTypeId < 3:
            raise HTTPException(status_code=403, detail="Permission Denied")
    except Exception:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=403, detail="Permission Denied")


def check_eidt_invoice_permission(u_id, inv_id, db: scoped_session = Depends(get_db)):
    try:
        # sub query to get permission def id from access permission using u_id
        sub_query = db.query(models.AccessPermission.permissionDefID).filter_by(
            userID=u_id
        )
        # getting the column related to update user permission type for invoice
        inv_perm = (
            db.query(models.AccessPermissionDef.AccessPermissionTypeId)
            .filter_by(idAccessPermissionDef=sub_query.scalar_subquery())
            .one()
        )
        # check if edit permission is there
        if inv_perm.AccessPermissionTypeId < 2:
            raise HTTPException(status_code=403, detail="Permission Denied")
    except Exception:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=403, detail="Permission Denied")


def check_usertype(u_id, db: scoped_session = Depends(get_db)):
    try:
        # query to get user type
        user_type = (
            db.query(models.Credentials.crentialTypeId)
            .filter_by(userID=u_id)
            .filter(models.Credentials.crentialTypeId.in_((1, 2)))
            .scalar()
        )
        if user_type:
            return user_type
        raise HTTPException(status_code=403, detail="Unknown user type")
    except Exception:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=403, detail=traceback.format_exc())


async def check_finance_approve(u_id, inv_id, db: scoped_session = Depends(get_db)):
    try:
        fn_perm = (
            db.query(models.AccessPermission.permissionDefID)
            .filter_by(userID=u_id)
            .scalar()
        )
        fn_perm = (
            db.query(models.AccessPermissionDef.AccessPermissionTypeId)
            .filter_by(idAccessPermissionDef=fn_perm)
            .scalar()
        )
        # check if document is of invoice type
        inv_type = (
            db.query(models.Document.idDocument)
            .filter_by(idDocument=inv_id, idDocumentType=3)
            .scalar()
        )
        if not inv_type:
            raise HTTPException(status_code=403, detail="Wrong invoice type")
        # check entity access
        val = await check_invoice_entity_user(u_id, inv_id, db)
        if not val:
            raise HTTPException(
                status_code=403, detail="Permission Denied, Entity Access not Available"
            )
        # check if finance permission is there
        if fn_perm < 4:
            raise HTTPException(
                status_code=403, detail="Permission Denied, Request Admin for Upgrade"
            )
    except Exception as e:
        print(e)
        raise HTTPException(status_code=403, detail="Permission Denied")


def check_user_access_customer(u_id, db: scoped_session = Depends(get_db)):
    try:
        # query to get user type
        user_type = (
            db.query(models.Credentials.crentialTypeId)
            .filter_by(userID=u_id)
            .filter_by(crentialTypeId=1)
            .scalar()
        )
        if user_type:
            return
        raise HTTPException(status_code=403, detail="API access denied")
    except Exception:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=403, detail="API access denied")


def check_user_access_vendor(u_id, db: scoped_session = Depends(get_db)):
    try:
        # query to get user type
        user_type = (
            db.query(models.Credentials.crentialTypeId)
            .filter_by(userID=u_id)
            .filter_by(crentialTypeId=2)
            .scalar()
        )
        if user_type:
            return
        raise HTTPException(status_code=403, detail="API access denied")
    except Exception:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=403, detail="API access denied")


def check_if_user_amount_approval(
    maxamount: permissionssm.Maxamount, db: scoped_session = Depends(get_db)
):
    try:
        # query to get user type
        pdef_id = (
            db.query(models.AccessPermission.permissionDefID)
            .filter_by(userID=maxamount.applied_uid)
            .scalar()
        )
        if pdef_id in (3, 9):
            return
        raise HTTPException(status_code=403, detail="Role not applicable")
    except Exception:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=403, detail="Role not applicable")


def check_if_cust_user_and_admin(u_id, db: scoped_session = Depends(get_db)):
    try:
        # query to get user type
        type = (
            db.query(models.Credentials.crentialTypeId)
            .filter_by(userID=u_id, crentialTypeId=1)
            .scalar()
        )
        # query to get user permission
        def_id = (
            db.query(models.AccessPermission.permissionDefID)
            .filter_by(userID=u_id)
            .scalar()
        )
        if type == 1 and def_id in (8, 1):
            return
        raise HTTPException(status_code=403, detail="Role not applicable")
    except Exception:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=403, detail="Role not applicable")


def check_if_service_trigger(u_id, db: scoped_session = Depends(get_db)):
    """Function to verify if the roles has permission to trigger service batch
    :param u_id: :param db: :return:"""
    try:
        # query to get user permission
        def_id = (
            db.query(models.AccessPermission.permissionDefID)
            .filter_by(userID=u_id)
            .scalar()
        )
        allow_service_trig = (
            db.query(models.AccessPermissionDef.allowServiceTrigger)
            .filter_by(idAccessPermissionDef=def_id)
            .scalar()
        )
        if allow_service_trig:
            return
        raise HTTPException(status_code=403, detail="Role not applicable")
    except Exception:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=403, detail="Role not applicable")

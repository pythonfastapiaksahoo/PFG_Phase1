from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from pfg_app.auth import AuthHandler
from pfg_app.azuread.auth import get_user
from pfg_app.crud import BatchexceptionCrud as crud
from pfg_app.FROps.pfg_trigger import pfg_sync
from pfg_app.session.session import get_db

auth_handler = AuthHandler()
router = APIRouter(
    prefix="/apiv1.1/Exception",
    tags=["Exception"],
    # dependencies=[Depends(get_user)],
    responses={404: {"description": "Not found"}},
)


@router.get("/batchprocesssummary")
async def read_batchprocesssummary(
    db: Session = Depends(get_db), user=Depends(get_user)
):
    return await crud.readbatchprocessdetails(user.id, db)


# main api to read line data
@router.get("/testlinedata/invoiceid/{inv_id}")
async def testlinedata(
    inv_id: int, db: Session = Depends(get_db), user=Depends(get_user)
):
    return await crud.readlinedatatest(user.id, inv_id, db)


@router.get("/pfg/pfgsync/{inv_id}")
async def pfgsyncflw(inv_id: int, db: Session = Depends(get_db)):
    return pfg_sync(inv_id, db)

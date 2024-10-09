from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from pfg_app.auth import AuthHandler
from pfg_app.azuread.auth import get_user
from pfg_app.azuread.schemas import AzureUser
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
    db: Session = Depends(get_db), user: AzureUser = Depends(get_user)
):
    read_batchprocesssummary = await crud.readbatchprocessdetails(user.idUser, db)
    return read_batchprocesssummary


# main api to read line data
@router.get("/testlinedata/invoiceid/{inv_id}")
async def testlinedata(
    inv_id: int, db: Session = Depends(get_db), user: AzureUser = Depends(get_user)
):
    lineData = await crud.readlinedatatest(int(user.idUser), inv_id, db)
    return lineData


@router.get("/pfg/pfgsync/{inv_id}")
async def pfgsyncflw(
    inv_id: int,
    db: Session = Depends(get_db),
    user: AzureUser = Depends(get_user),
):
    overall_status = pfg_sync(inv_id, user.idUser, db)
    return overall_status

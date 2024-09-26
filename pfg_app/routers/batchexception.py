from auth import AuthHandler
from azuread.auth import get_user
from crud import BatchexceptionCrud as crud
from fastapi import APIRouter, Depends
from session import get_db
from sqlalchemy.orm import Session

auth_handler = AuthHandler()


router = APIRouter(
    prefix="/apiv1.1/Exception",
    tags=["Exception"],
    dependencies=[Depends(get_user)],
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

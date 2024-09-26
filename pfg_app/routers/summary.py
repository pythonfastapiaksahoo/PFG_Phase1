import sys

# from database import SessionLocal, engine
from typing import Optional

from auth import AuthHandler
from crud import summaryCrud
from fastapi import APIRouter, Depends
from session import get_db
from sqlalchemy.orm import Session

sys.path.append("..")

auth_handler = AuthHandler()

router = APIRouter(
    prefix="/apiv1.1/Summary",
    tags=["Summary"],
    dependencies=[Depends(auth_handler.auth_wrapper)],
    responses={404: {"description": "Not found"}},
)


@router.get("/apiv1.1/invoiceProcessSummary/{u_id}")
async def read_galadhari_summary_item(
    u_id: int,
    ftdate: Optional[str] = None,
    sp_id: Optional[int] = None,
    fentity: Optional[str] = None,
    db: Session = Depends(get_db),
):
    return await summaryCrud.read_galadhari_summary(u_id, ftdate, sp_id, fentity, db)


@router.get("/apiv1.1/pages/{u_id}")
async def read_pages_summary(
    u_id: int,
    ftdate: Optional[str] = None,
    endate: Optional[str] = None,
    entity: Optional[int] = None,
    vendor: Optional[str] = None,
    sp: Optional[str] = None,
    db: Session = Depends(get_db),
):
    return await summaryCrud.read_pages_summary(
        u_id, ftdate, endate, entity, vendor, sp, db
    )


@router.get("/apiv1.1/EntityFilter/{u_id}")
async def read_entity_filter_item(u_id: int, db: Session = Depends(get_db)):
    return await summaryCrud.read_entity_filter(u_id, db)


@router.get("/apiv1.1/ServiceFilter/{u_id}")
async def read_service_filter_item(u_id: int, db: Session = Depends(get_db)):
    return await summaryCrud.read_service_filter(u_id, db)

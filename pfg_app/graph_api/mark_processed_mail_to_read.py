import traceback
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
import requests
from sqlalchemy.orm import Session
from datetime import datetime

from pfg_app import model
from pfg_app.logger_module import logger
from pfg_app.session.session import get_db
from pfg_app.graph_api.ms_graphapi_token_manager import MSGraphAPITokenManager
from pfg_app import settings

router = APIRouter()

class MarkAsReadRequest(BaseModel):
    message_id: str

# @router.patch("/mails/mark-as-read")
def mark_processed_mail_as_read(mail_row_key):
    try:
        db = next(get_db())
        mail_id_str = mail_row_key.split("-")[-1]
        mail_id = int(mail_id_str)

        # Query CorpMail to get message_id
        corp_mail = db.query(model.CorpMail).filter(model.CorpMail.id == mail_id).first()
        if not corp_mail:
            raise Exception(f"CorpMail with id {mail_id} not found")

        message_id = corp_mail.message_id
        logger.info(f"Marking mail as read")
        # Get access token
        token_manager = MSGraphAPITokenManager()
        access_token = token_manager.get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        # Call Graph API to mark mail as read
        patch_url = (
            f"https://graph.microsoft.com/v1.0/users/{settings.graph_corporate_mail_id}/messages/{message_id}"
        )
        body = {
            "isRead": True
        }

        response = requests.patch(patch_url, json=body, headers=headers)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail="Failed to update message status")

        return {f"Mail with mail_row_key {mail_row_key} marked as read successfully"}

    except Exception as e:
        logger.error(f"Error marking mail as read: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))
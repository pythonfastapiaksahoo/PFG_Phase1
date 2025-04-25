import traceback
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
import requests
from sqlalchemy.orm import Session
from datetime import datetime

from pfg_app import model
from pfg_app.graph_api.utils import get_folder_id
from pfg_app.logger_module import logger
from pfg_app.session.session import get_db
from pfg_app.graph_api.ms_graphapi_token_manager import MSGraphAPITokenManager
from pfg_app import settings



def mark_processed_mail_as_read(mail_row_key):
    try:
        logger.info(f"Marking mail as read for mail_row_key: {mail_row_key} initiated.")
        db = next(get_db())
        mail_id_str = mail_row_key.split("-")[-1]
        mail_id = int(mail_id_str)

        # Query CorpMail to get message_id
        corp_mail = db.query(model.CorpMail).filter(model.CorpMail.id == mail_id).first()
        if not corp_mail:
            logger.error(f"CorpMail with id {mail_id} not found")

        message_id = corp_mail.message_id
        logger.info(f"Marking mail as read")
        # Get access token
        token_manager = MSGraphAPITokenManager()
        access_token = token_manager.get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        mail_folder_id = get_folder_id("IDP", access_token)
        if mail_folder_id:
            logger.info(f"Mail folder IDP found: {mail_folder_id}")
        else:
            logger.info(f"Mail folder IDP not found, creating a new one Manually")
            return False

        # Call Graph API to mark mail as read
        patch_url = (
            f"https://graph.microsoft.com/v1.0/users/{settings.graph_corporate_mail_id}/mailFolders/{mail_folder_id}/messages/{message_id}"
        )
        body = {
            "isRead": True
        }

        response = requests.patch(patch_url, json=body, headers=headers)
        if response.status_code != 200:
            # raise HTTPException(status_code=response.status_code, detail="Failed to update message status")
            logger.error(f"Failed to update message status: {response.status_code}")
            return "failed"
            # return {"error": "Failed to update message status"}
            
        return "success"

    except Exception as e:
        logger.error(f"Error marking mail as read: {traceback.format_exc()}")
        return "failed"
        
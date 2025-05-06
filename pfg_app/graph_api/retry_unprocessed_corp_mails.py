import datetime
import traceback
import requests
import uuid
from pfg_app import model
from pfg_app.graph_api.message_processing import process_new_message
from pfg_app.graph_api.utils import get_folder_id
from pfg_app.logger_module import logger, set_operation_id
from pfg_app.session.session import get_db
from pfg_app.graph_api.ms_graphapi_token_manager import MSGraphAPITokenManager
from pfg_app import settings


def fetch_and_process_recent_graph_mails(operation_id: str):
    try:
        db = next(get_db())
        # 1) Get access token for Graph API
        token_manager = MSGraphAPITokenManager()
        access_token = token_manager.get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }
        mail_folder_id = get_folder_id("Inbox", access_token)
        if mail_folder_id:
            logger.info(f"Mail folder Inbox found: {mail_folder_id}")
        else:
            logger.info(f"Mail folder Inbox not found, creating a new one Manually")
            return False

        # 2) Calculate time window for the last 2 days (ISO 8601 format)
        seven_days_ago = (datetime.datetime.utcnow() - datetime.timedelta(days=7)).isoformat() + "Z"
        url = (
            f"https://graph.microsoft.com/v1.0/users/{settings.graph_corporate_mail_id}/mailFolders/{mail_folder_id}/messages"
            f"?$filter=receivedDateTime ge {seven_days_ago}"
            f"&$select=id,receivedDateTime&$orderby=receivedDateTime desc"
        )

        messages = []
        while url:
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            logger.info(f"Fetched {len(data.get('value', []))} messages from Graph API.")
            messages.extend(data.get("value", []))
            url = data.get("@odata.nextLink")  # handle paging

        # 3) Load all message_ids from CorpMail for fast comparison
        existing_message_ids = {
            row[0] for row in db.query(model.CorpMail.message_id).all()
        }

        # 4) Process messages
        for msg in messages:
            message_id = msg["id"]
            if message_id not in existing_message_ids:
                # Insert into CorpMail
                new_mail = model.CorpMail(message_id=message_id)
                db.add(new_mail)
                db.commit()
                db.refresh(new_mail)

                logger.info(f"New message inserted with ID {new_mail.id}, processing...")

                # Process the message
                process_new_message(message_id, new_mail.id, operation_id)

    except Exception as e:
        logger.error(f"Error in fetch_and_process_recent_graph_mails: {traceback.format_exc()}")
        
        




import re
import traceback
import requests
import base64
from pfg_app import settings
from pfg_app.core.utils import upload_blob_securely
from pfg_app.graph_api.ms_graphapi_token_manager import MSGraphAPITokenManager
from pfg_app.logger_module import get_operation_id, logger
from pfg_app.model import CorpQueueTask
from pfg_app.routers.CorpIntegrationapi import save_to_database


def process_new_message(message_id: str, corp_mail_id: int, operation_id: str):
    """
    Fetch the full message (with attachments) in its native RFC822 format,
    upload the message as an EML file, recursively process attachments (including emails embedded as attachments)
    and upload them to blob storage, 
    and add an entry to the CorpQueue table.
    # and finally move the message to the 'inbox' folder.
    """
    try:
        # 1) Get the access token
        token_manager = MSGraphAPITokenManager()
        access_token = token_manager.get_access_token()

        # 2) Fetch message metadata with attachments expanded
        meta_url = f"https://graph.microsoft.com/v1.0/users/{settings.graph_corporate_mail_id}/messages/{message_id}?$expand=attachments" # TODO:FLAG_GRAPH
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }
        meta_resp = requests.get(meta_url, headers=headers)
        meta_resp.raise_for_status()
        message_data = meta_resp.json()

        # 2.1) Extract the message subject (sanitize if needed)
        message_sub = message_data.get("subject", "NoSubject")
        # Remove all special characters, keep only letters and digits
        message_subject = re.sub(r'[^A-Za-z0-9]', '', message_sub)

        # Trim to first 25 characters
        message_subject = message_subject[:30]
        # 3) Retrieve the full MIME content (native RFC822 format)
        mime_url = f"https://graph.microsoft.com/v1.0/users/{settings.graph_corporate_mail_id}/messages/{message_id}/$value" # TODO:FLAG_GRAPH
        mime_headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "message/rfc822"
        }
        mime_resp = requests.get(mime_url, headers=mime_headers)
        mime_resp.raise_for_status()
        mime_content = mime_resp.content  # Binary MIME content

        # 4) Define the base folder path using corp_mail_id and subject
        base_folder = f"ap-portal-invoices/CORPORATE/CE-{corp_mail_id}"
        eml_filename = f"{base_folder}/{message_subject}.eml"
        eml_url = upload_blob_securely('apinvoice-mail-container', eml_filename, mime_content, "message/rfc822")

        # 5) Recursive function to process attachments
        def process_attachments(attachments, folder_path):
            for att in attachments:
                att_type = att.get("@odata.type", "")
                if att_type == "#microsoft.graph.fileAttachment":
                    # Regular file attachment
                    filename = att.get("name", "unnamed")
                    content_b64 = att.get("contentBytes", "")
                    if content_b64:
                        file_content = base64.b64decode(content_b64)
                        file_path = f"{folder_path}/{filename}"
                        content_type = att.get("contentType", "application/octet-stream")
                        upload_blob_securely('apinvoice-mail-container', file_path, file_content, content_type)
                elif att_type == "#microsoft.graph.itemAttachment":
                    # This might be an embedded email (or other item)
                    item = att.get("item", {})
                    # Save the nested email as an EML file.
                    nested_subject = item.get("subject", "NoSubject")
                    nested_eml_filename = f"{folder_path}/{nested_subject}.eml"
                    # Since raw MIME isn’t available here, we serialize the email JSON.
                    import json
                    nested_email_content = json.dumps(item, indent=2)
                    upload_blob_securely('apinvoice-mail-container', nested_eml_filename, 
                                           nested_email_content.encode('utf-8'), "message/rfc822")
                    # If the nested email contains its own attachments, process them recursively.
                    nested_attachments = item.get("attachments", [])
                    if nested_attachments:
                        nested_folder = f"{folder_path}/{nested_subject}"
                        process_attachments(nested_attachments, nested_folder)
                else:
                    # Handle any additional attachment types if needed.
                    pass

        # 6) Process top-level attachments recursively
        attachments = message_data.get("attachments", [])
        process_attachments(attachments, base_folder+f"/{message_subject}")

        # 7) Add an entry to the CorpQueue table
        new_mail_id = "CE-" + str(corp_mail_id)
        request_data = {
            "eml_path": eml_url,
            "mail_row_key": new_mail_id,
            "sender": message_data.get("sender", {}).get("emailAddress", {}).get("address", "NoSender"),
            "subject": message_subject,
            "file_type": "eml",
            "invoice_type": "corp",
            "email_listener_ts": message_data.get("receivedDateTime", "NoReceivedDateTime"),
            "operation_id": operation_id
        }
        if settings.build_type == "debug":
            queued_status = f"{settings.local_user_name}-queued"
        else:
            queued_status = "queued"

        # Create a new CorpQueueTask with the extracted mail_row_key
        new_task = CorpQueueTask(
            request_data=request_data, status=queued_status, mail_row_key=new_mail_id
        )
        # Retry logic encapsulated in save_to_database
        task_id = save_to_database(new_task)
        logger.info(f"CorpQueueTask submitted successfully with ID: {task_id}")

        # # 8) Move the message to the "inbox" folder
        # move_url = f"{settings.MS_GRAPH_BASE_URL}/users/{settings.EMAIL_ID}/messages/{message_id}/move"
        # move_body = {
        #     "destinationId": "inbox"  # Adjust this if your folder ID differs.
        # }
        # move_resp = requests.post(move_url, headers=headers, json=move_body)
        # if move_resp.status_code == 404:
        #     print("Message not found, likely already moved or deleted.")
        # else:
        #     move_resp.raise_for_status()
        #     print(f"Message {message_id} moved to 'inbox' folder.")

    except Exception:
        logger.info(f"Error processing message {message_id}: {traceback.format_exc()}")

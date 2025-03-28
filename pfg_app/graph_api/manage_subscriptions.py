import traceback
import uuid
import requests
import json
import time

from pfg_app.graph_api.ms_graphapi_token_manager import MSGraphAPITokenManager
from pfg_app.logger_module import logger, set_operation_id
from pfg_app.model import BackgroundTask
from pfg_app.session.session import get_db
from pfg_app import settings


def parse_timestamp(dt_str):
    # dt_str like "2025-01-23T12:34:56Z"
    return time.mktime(time.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ"))

def get_subscriptions(access_token):
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    endpoint = 'https://graph.microsoft.com/v1.0/subscriptions'
    
    response = requests.get(endpoint, headers=headers)
    
    if response.status_code == 200:
        subscriptions = response.json()
        return subscriptions
    else:
        logger.info(f"Error: {response.status_code}")
        logger.info(response.text)
        return None
    

def delete_subscription(access_token, subscription_id):
    """
    Delete a specific Microsoft Graph API subscription by ID.
    
    Args:
        access_token (str): The OAuth access token for Microsoft Graph API
        subscription_id (str): The ID of the subscription to delete
        
    Returns:
        bool: True if deletion was successful, False otherwise
    """
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    endpoint = f'https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}'
    
    response = requests.delete(endpoint, headers=headers)
    
    if response.status_code == 204:  # Success response for deletion is 204 No Content
        logger.info(f"Successfully deleted subscription {subscription_id}")
        return True
    else:
        logger.info(f"Error deleting subscription: {response.status_code}")
        logger.info(response.text)
        return False
    
def create_subscriptions(access_token,PUBLIC_ENDPOINT,EMAIL_ID):
    # delete the existing subscription
    # delete_subscription(access_token,'01106b6f-3a1c-4358-a123-c58023358568')
    subscription_details={
        "CLIENT_STATE": "mPmirf4hPku74aT0TQjgCA",
        "SUBSCRIPTION_ID": None,
        "SUBSCRIPTION_EXPIRATION": None
    }
    now_ts = time.time()
    expires_in_seconds = 60 * 60 * 60  # 60 hours
    expiration_time = int(now_ts + expires_in_seconds)
    expiration_str = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(expiration_time))

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    body = {
        "changeType": "created",
        "notificationUrl": PUBLIC_ENDPOINT,
        "resource": f"/users/{EMAIL_ID}/mailFolders('Inbox')/messages", # TODO:FLAG_GRAPH
        "expirationDateTime": expiration_str,
        "clientState": subscription_details["CLIENT_STATE"]
    }
    url = f"https://graph.microsoft.com/v1.0/subscriptions"
    resp = requests.post(url, headers=headers, json=body)
    logger.info(resp.text)
    if resp.status_code == 201:
        data = resp.json()
        logger.info(data)
        subscription_details["SUBSCRIPTION_ID"] = data["id"]
        subscription_details["SUBSCRIPTION_EXPIRATION"] = data["expirationDateTime"]
        logger.info(f"Created new subscription:{subscription_details['SUBSCRIPTION_ID']} expires:{data['expirationDateTime']}")
        return subscription_details
    else:
        raise Exception(f"Failed to create subscription: {resp.status_code} {resp.text}")

def subscription_renewal_loop(operation_id):
    logger.info(f"subscription_renewal_loop operation_id:{operation_id}")
    set_operation_id(operation_id)
    try:
        db = next(get_db())
        # get the background task and lock it and if it could not be locked end the thread
        if settings.build_type == "debug":
            background_task_name = f"{settings.local_user_name}-subscription_renewal_loop"
        else:
            background_task_name = "subscription_renewal_loop"
        logger.info(f"subscription_renewal_loop background_task_name:{background_task_name}")
        background_task = db.query(BackgroundTask).filter(BackgroundTask.task_name == background_task_name)
        # .with_for_update(skip_locked=True).
        background_task = background_task.first()
        # If we didn't acquire the lock, exit the thread function gracefully.
        if background_task is None:
            logger.info("Background task is picked by other Threads, exiting the thread")
            return
        

        while True:
            logger.info(f"subscription_renewal_loop loop running")
            try:
                create_or_renew_subscription(background_task,db)
            except Exception:
                logger.error(f"create_or_renew_subscription_task error: {traceback.format_exc()}")
            logger.info(f"subscription_renewal_loop loop sleeping for 5 minutes")
            time.sleep(300)  # 5 minutes
            logger.info(f"subscription_renewal_loop loop woke up")
    except Exception:
        logger.error(f"subscription_renewal_loop error: {traceback.format_exc()}")

def create_or_renew_subscription(background_task,db):
    """
    Create or renew a subscription for the user's Inbox to get notifications when new messages arrive.
    """
 
    now_ts = time.time()
    token_manager = MSGraphAPITokenManager()
    access_token = token_manager.get_access_token()
    # from background_task get the subscription_details if it is not present create a new one
    subscription_details = background_task.task_metadata
    if not subscription_details:
        subscription_details = create_subscriptions(access_token,'https://dev.mail.ia.owfg.com',settings.graph_corporate_mail_id) # TODO:FLAG_GRAPH
        background_task.task_metadata = subscription_details
        db.add(background_task)
        db.commit()
        return
    


    # If we have at least 30 minutes left, skip renewal
    if subscription_details["SUBSCRIPTION_ID"] and subscription_details["SUBSCRIPTION_EXPIRATION"] and (subscription_details["SUBSCRIPTION_EXPIRATION"] - now_ts > 1800):
        return  # still valid


    # Set an expiration time. Max ~70.5 hours for mail, let's choose 60 hours to be safe.
    expires_in_seconds = 60 * 60 * 60  # 60 hours
    expiration_time = int(now_ts + expires_in_seconds)
    expiration_str = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(expiration_time))

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    body = {
        "changeType": "created",
        "notificationUrl": 'https://dev.mail.ia.owfg.com', # TODO:FLAG_GRAPH # 7c7c-209-52-125-81.ngrok-free.app/apiv1.1/MailListener/webhook
        "resource": f"/users/{settings.graph_corporate_mail_id}/mailFolders('Inbox')/messages", # TODO:FLAG_GRAPH
        "expirationDateTime": expiration_str,
        "clientState": subscription_details["CLIENT_STATE"]
    }

    if subscription_details["SUBSCRIPTION_ID"]:
        # Try to renew
        url = f"https://graph.microsoft.com/v1.0/subscriptions/{subscription_details['SUBSCRIPTION_ID']}" # TODO:FLAG_GRAPH
        resp = requests.patch(url, headers=headers, json={
            "expirationDateTime": expiration_str
        })
        if resp.status_code == 200:
            data = resp.json()
            subscription_details["SUBSCRIPTION_EXPIRATION"] = data["expirationDateTime"]
            logger.info(f"Subscription renewed:{subscription_details['SUBSCRIPTION_ID']} expires:{parse_timestamp(data['expirationDateTime'])}")
            # update the background task
            background_task.task_metadata = subscription_details
            db.add(background_task)
            db.commit()
            return
        else:
            logger.info(f"Renewal failed, creating a new subscription. {resp.status_code} {resp.text}")
            subscription_details["SUBSCRIPTION_ID"] = None  # reset to create a new one
            # update the background task
            background_task.task_metadata = subscription_details
            db.add(background_task)
            db.commit()
            return



import traceback
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, Response, status
from fastapi.responses import PlainTextResponse
from pfg_app.azuread.auth import get_admin_user, get_user_dependency
from pfg_app.azuread.schemas import AzureUser
from pfg_app.graph_api.manage_subscriptions import delete_subscription, get_subscriptions
from pfg_app.graph_api.message_processing import process_new_message
from pfg_app.graph_api.ms_graphapi_token_manager import MSGraphAPITokenManager
from pfg_app.model import BackgroundTask, CorpMail
from sqlalchemy.orm import Session
from pfg_app.logger_module import get_operation_id, logger
from pfg_app.session.session import get_db
from pfg_app import settings

router = APIRouter(
    prefix="/apiv1.1/MailListener",
    tags=["MailListener"],
    # dependencies=[Depends(get_admin_user)],
    responses={404: {"description": "Not found"}},
)

# @router.get("/webhook")
# def graph_webhook_validation(request: Request):
#     """
#     Handle validation request from Microsoft Graph (GET).
#     """
    
#     raise HTTPException(status_code=400, detail="validationToken missing")

@router.post("/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Handle actual webhook notifications from Microsoft Graph (POST).
    """
    operation_id = get_operation_id()
    validation_token = request.query_params.get("validationToken")
    if validation_token:
        logger.info(f"Validation token received: {validation_token}")
        return PlainTextResponse(validation_token, status_code=200)
    try:
        # get the data from the request
        data = await request.json()
    except Exception as e:
        logger.error(f"Webhook JSON parsing error: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON payload.")
    db = next(get_db())
    notifications = data.get("value")
    for notification in notifications:
        # (A) Verify clientState to ensure authenticity
        # fetch the subscription details from the database
        subscription_details = db.query(BackgroundTask).filter(BackgroundTask.task_name == "subscription_renewal_loop").first() # TODO: FLAG_GRAPH
        if not subscription_details:
            logger.error("Subscription details not found in the database")
            continue
        subscription_details = subscription_details.task_metadata
        if notification.get("clientState") != subscription_details.get("CLIENT_STATE"):
            # Ignore if mismatch
            logger.error("clientState mismatch. Possible spoofed request.")
            continue

        # (B) Handle creation event
        if notification.get("changeType") == "created":
            resource = notification.get("resource", "")
            # Resource is like: /users/{userId}/messages/{messageId}
            parts = resource.split("/")
            if len(parts) >= 4:
                message_id = parts[-1]
                # Save the message_id to the database
                # Check if the message_id already exists in the database
                existing_mail = db.query(CorpMail).filter(CorpMail.message_id == message_id).first()
                if existing_mail:
                    logger.info(f"Message ID {message_id} already exists in the database")
                    continue
                corp_mail = CorpMail(message_id=message_id)
                db.add(corp_mail)
                db.commit()
                db.refresh(corp_mail)
                logger.info(f"New message ID: {message_id}, Mail ID: {corp_mail.id}")
                # Offload processing to a background task
                background_tasks.add_task(process_new_message, message_id, corp_mail.id, operation_id)
    logger.info(f"Webhook received {len(notifications)} notifications")
    return Response(status_code=202)

# List all the subscriptions
@router.get("/subscriptions")
def list_subscriptions(db: Session = Depends(get_db), user: AzureUser = Depends(get_user_dependency(["DSD_APPortal_User","CORP_APPortal_User","DSD_ConfigPortal_User","CORP_ConfigPortal_User"]))):
    """
    List all the subscriptions
    """
    results = {
        "subscriptions-in-db": [],
        "subscriptions-in-graph": [],
    }
    subscriptions = db.query(BackgroundTask).filter(BackgroundTask.task_name == "subscription_renewal_loop").all()
    for subscription in subscriptions:
        results["subscriptions-in-db"].append(subscription.task_metadata)
    # Get the subscriptions from the graph
    token_manager = MSGraphAPITokenManager()
    access_token = token_manager.get_access_token()
    subscriptions = get_subscriptions(access_token)
    results["subscriptions-in-graph"] = subscriptions
    return results

# Delete a subscription
@router.delete("/subscriptions/{subscription_id}")
def delete_subscriptions(subscription_id: str, db: Session = Depends(get_db), user: AzureUser = Depends(get_user_dependency(["DSD_APPortal_User","CORP_APPortal_User","DSD_ConfigPortal_User","CORP_ConfigPortal_User"]))):
    """
    Delete a subscription
    """
    # delete from db 
    try:
        subscription = db.query(BackgroundTask).filter(
            BackgroundTask.task_name == "subscription_renewal_loop"
        ).filter(
            BackgroundTask.task_metadata["SUBSCRIPTION_ID"].astext == subscription_id
        ).first()
        if not subscription:
            logger.info(f"Subscription not found in the database")
        else:
            logger.info(f"Subscription found in the database")
            # delete the subscription from the database's task_metadata
            subscription.task_metadata = {
                "SUBSCRIPTION_ID": None,
                "SUBSCRIPTION_EXPIRATION": None,
                "CLIENT_STATE": "mPmirf4hPku74aT0TQjgCA",
            }
            db.add(subscription)
            db.commit()
    except Exception:
        logger.info(f"Exception while delete in db {traceback.format_exc() }")
    try:
        # delete from graph
        token_manager = MSGraphAPITokenManager()
        access_token = token_manager.get_access_token()
        delete_subscription(access_token, subscription_id)
        return {"message": "Subscription deleted"}
    except Exception:
        return {"error": traceback.format_exc() }

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, Response, status
from fastapi.responses import PlainTextResponse
from pfg_app.graph_api.message_processing import process_new_message
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
        subscription_details = db.query(BackgroundTask).filter(BackgroundTask.task_name == f"{settings.local_user_name}-subscription_renewal_loop").first()
        if not subscription_details:
            logger.error("Subscription details not found in the database")
            continue
        subscription_details = subscription_details.task_metadata
        if notification.get("clientState") != subscription_details.get("CLIENT_STATE"):
            # Ignore if mismatch
            print("clientState mismatch. Possible spoofed request.")
            continue

        # (B) Handle creation event
        if notification.get("changeType") == "created":
            resource = notification.get("resource", "")
            # Resource is like: /users/{userId}/messages/{messageId}
            parts = resource.split("/")
            if len(parts) >= 4:
                message_id = parts[-1]
                # Save the message_id to the database
                corp_mail = CorpMail(message_id=message_id)
                db.add(corp_mail)
                db.commit()
                db.refresh(corp_mail)
                # Offload processing to a background task
                background_tasks.add_task(process_new_message, message_id, f"CE-{corp_mail.id}", operation_id)
    logger.info(f"Webhook received {len(notifications)} notifications")
    return Response(status_code=202)


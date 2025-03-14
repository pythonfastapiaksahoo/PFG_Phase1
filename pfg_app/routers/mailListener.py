from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import PlainTextResponse
from pfg_app.logger_module import logger

router = APIRouter(
    prefix="/apiv1.1/MailListener",
    tags=["MailListener"],
    # dependencies=[Depends(get_admin_user)],
    responses={404: {"description": "Not found"}},
)

@router.get("/webhook")
async def graph_webhook_validation(request: Request):
    """
    Handle validation request from Microsoft Graph (GET).
    """
    validation_token = request.query_params.get("validationToken")
    if validation_token:
        return PlainTextResponse(validation_token, status_code=200)
    raise HTTPException(status_code=400, detail="validationToken missing")

@router.post("/webhook")
async def webhook(request: Request):
    """
    Handle actual webhook notifications from Microsoft Graph (POST).
    """
    try:
        data = await request.json()
    except Exception as e:
        logger.error(f"Webhook JSON parsing error: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON payload.")

    notifications = data.get("value")
    if not notifications:
        raise HTTPException(status_code=400, detail="No notifications received.")

    logger.info(f"Webhook received successfully: {notifications}")

    # Process notifications asynchronously here if required
    # TODO: Process notifications asynchronously here if required

    return {"message": "Notifications received successfully"}


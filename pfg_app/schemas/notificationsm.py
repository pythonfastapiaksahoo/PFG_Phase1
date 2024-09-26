from typing import List, Optional

from pydantic import BaseModel


# new AmountApprovalLvl Structure
class NotificationTemplateRecipients(BaseModel):
    to_addr: List[str]
    cc_addr: List[str]
    bcc_addr: List[str]
    isDefaultRecepients: bool


# update notification recipients
class UNotificationRecipient(BaseModel):
    notification_type: int
    usersIDS: Optional[List[int]] = None
    rolesIDS: Optional[List[int]] = None

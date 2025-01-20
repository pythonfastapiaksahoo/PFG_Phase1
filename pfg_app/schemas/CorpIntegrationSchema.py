from pydantic import BaseModel


class ProcessResponse(BaseModel):
    email_data: dict
    invoice_detail_list: list
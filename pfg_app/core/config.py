from os import getenv

from dotenv import load_dotenv
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Base
    api_v1_prefix: str
    debug: bool
    project_name: str
    version: str
    description: str
    build_type: str = "debug"  # debug or release

    # Azure
    form_recognizer_endpoint: str
    api_version: str
    tenant_id: str
    client_id: str
    client_secret: str
    key_vault_url: str
    open_ai_endpoint: str
    storage_account_name: str
    application_insights_instrumentation_key: str

    # ERP
    erp_url: str
    erp_invoice_import_endpoint: str
    erp_invoice_status_endpoint: str
    erp_user: str
    erp_password: str


load_dotenv(getenv("ENV_FILE"))

settings = Settings()

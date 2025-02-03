from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Base
    api_v1_prefix: str = "/api/v1"
    debug: bool = False
    project_name: str = "FIA App"
    version: str = "0.0.1"
    description: str = "The API for FIA app. "
    build_type: str = "debug"  # dev or qa or prod

    # Azure Auth
    api_client_id: str = ""
    api_client_secret: str = ""
    swagger_ui_client_id: str = ""
    aad_tenant_id: str = ""
    aad_instance: str = "https://login.microsoftonline.com/"
    api_audience: str = ""

    # DB (TODO - Change to use system identity)
    db_host: str = ""
    db_port: int = 0
    db_user: str = ""
    db_password: str = ""
    db_name: str = ""
    db_schema: str = ""

    azure_postgresql_connectionstring: str = ""

    # Azure
    form_recognizer_endpoint: str = ""
    api_version: str = ""
    tenant_id: str = ""
    client_id: str = ""
    client_secret: str = ""
    key_vault_url: str = ""
    open_ai_endpoint: str = ""
    storage_account_name: str = ""
    appinsights_instrumentation_key: str = ""
    appinsights_connection_string: str = ""

    # ERP
    erp_url: str = ""
    erp_invoice_import_endpoint: str = ""
    erp_invoice_status_endpoint: str = ""
    erp_user: str = ""
    erp_password: str = ""

    # local setup for testing
    local_user_name: str = "local"
    wkhtmltoimage_path: str = ""

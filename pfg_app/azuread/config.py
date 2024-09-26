import os

from starlette.config import Config

config = Config(".env")


# Authentication
API_CLIENT_ID: str = config("API_CLIENT_ID", default=os.getenv("API_CLIENT_ID", ""))
API_CLIENT_SECRET: str = config(
    "API_CLIENT_SECRET", default=os.getenv("API_CLIENT_SECRET", "")
)
SWAGGER_UI_CLIENT_ID: str = config(
    "SWAGGER_UI_CLIENT_ID", default=os.getenv("SWAGGER_UI_CLIENT_ID", "")
)
AAD_TENANT_ID: str = config("AAD_TENANT_ID", default=os.getenv("AAD_TENANT_ID", ""))

AAD_INSTANCE: str = config("AAD_INSTANCE", default=os.getenv("AAD_INSTANCE", ""))
API_AUDIENCE: str = config("API_AUDIENCE", default=os.getenv("API_AUDIENCE", ""))

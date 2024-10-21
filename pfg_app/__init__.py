import concurrent.futures
from os import getenv

from dotenv import load_dotenv

from pfg_app.core.config import Settings

load_dotenv(getenv("ENV-FILE"))

settings: Settings = Settings()

from pfg_app.core.utils import get_credential, get_secret_from_vault  # noqa: E402
from pfg_app.logger_module import logger  # noqa: E402

if settings.build_type not in ["debug", "dev"]:
    credential = get_credential()
    key_vault_secrets = []
    key_vault_secrets_names = [
        {"api_client_id": "APPORTAL-API-CLIENT-ID"},
        {"api_client_secret": "APPORTAL-API-CLIENT-SECRET"},
        {"swagger_ui_client_id": "APPORTAL-SWAGGER-UI-CLIENT-ID"},
        {"aad_tenant_id": "APPORTAL-AAD-TENANT-ID"},
        {"aad_instance": "APPORTAL-AAD-INSTANCE"},
        {"api_audience": "APPORTAL-API-AUDIENCE"},
        {"db_name": "APPORTAL-DB-NAME"},
        {"db_schema": "APPORTAL-DB-SCHEMA"},
        {"db_user": "APPORTAL-DB-USER"},
        {"db_password": "APPORTAL-DB-PASSWORD"},
        {"db_host": "APPORTAL-DB-HOST"},
        {"db_port": "APPORTAL-DB-PORT"},
        {"form_recognizer_endpoint": "APPORTAL-FORM-RECOGNIZER-ENDPOINT"},
        {"api_version": "APPORTAL-API-VERSION"},
        {"tenant_id": "APPORTAL-TENANT-ID"},
        {"client_id": "APPORTAL-CLIENT-ID"},
        {"client_secret": "APPORTAL-CLIENT-SECRET"},
        {"key_vault_url": "APPORTAL-KEY-VAULT-URL"},
        {"open_ai_endpoint": "APPORTAL-OPEN-AI-ENDPOINT"},
        {"storage_account_name": "APPORTAL-STORAGE-ACCOUNT-NAME"},
        {
            "application_insights_instrumentation_key": "APPORTAL-APPLICATION-INSIGHTS-INSTRUMENTATION-KEY"  # noqa: E501
        },
        {
            "application_insights_connection_string": "APPORTAL-APPLICATION-INSIGHTS-CONNECTION-STRING"  # noqa: E501
        },
        {"erp_url": "APPORTAL-ERP-URL"},
        {"erp_invoice_import_endpoint": "APPORTAL-ERP-INVOICE-IMPORT-ENDPOINT"},
        {"erp_invoice_status_endpoint": "APPORTAL-ERP-INVOICE-STATUS-ENDPOINT"},
        {"erp_user": "APPORTAL-ERP-USER"},
        {"erp_password": "APPORTAL-ERP-PASSWORD"},
    ]
    # Retrieve secrets from Key Vault and assign them to the settings object
    # via multiprocessing
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                get_secret_from_vault, credential, secret_name, settings_key
            )
            for secret_object in key_vault_secrets_names
            for settings_key, secret_name in secret_object.items()
        ]

        # Collect the results as they complete
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                key_vault_secrets.append(result)
                logger.info(
                    (
                        f"Secret retrieved: {result['settings_key']}:"
                        f"{result['secret']}"
                    )
                )  # noqa: E501
                # Assign the secret to the settings object
                setattr(settings, result["settings_key"], result["secret"])

            except Exception as e:
                logger.error(f"Error processing a secret: {e}")  # type: ignore

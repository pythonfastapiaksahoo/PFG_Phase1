from typing import Optional

from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import AzureError
from azure.identity import (
    ClientSecretCredential,
    CredentialUnavailableError,
    DefaultAzureCredential,
)
from azure.keyvault.secrets import SecretClient
from core.config import settings
from logger_module import logger


# Shared function to get the correct credential based on the environment
# variable
def get_credential(secret_name: Optional[str] = None):
    """Retrieves credentials based on the build type specified in the
    'BUILD_TYPE' environment variable. The function attempts to acquire
    credentials through Managed Identity (MI) or, optionally, retrieves secrets
    from Azure Key Vault using key-based access if a 'secret_name' is provided.

    Handles exception scenarios for both Managed Identity and Key Vault
    access.

    :param secret_name: Optional name of the secret to retrieve from Key
        Vault. If None, credential is returned.
    :return: A Credential object for use with Azure services.
    """
    try:
        # Determine the build type (default to "debug" if not set)
        build_type = settings.build_type

        if build_type == "release" or build_type == "uat":
            # Use Managed Identity for release
            logger.info("Using Managed Identity for authentication in release.")
            try:
                # Automatically handles MI and other chained credentials
                return DefaultAzureCredential()
            except CredentialUnavailableError as e:
                logger.error(f"Managed Identity is not available: {str(e)}")
                raise

        else:
            # Use Key Vault to retrieve the API key for build_type
            logger.info(f"Using API key for authentication in {build_type}.")

            key_vault_url = settings.key_vault_url
            # Credentials to access Key Vault (using client ID & secret for
            # debug)
            tenant_id = settings.tenant_id
            client_id = settings.client_id
            client_secret = settings.client_secret

            try:
                # Try to create a ClientSecretCredential and access Key Vault
                credential = ClientSecretCredential(tenant_id, client_id, client_secret)

                if secret_name:
                    secret_client = SecretClient(
                        vault_url=key_vault_url, credential=credential
                    )

                    # Retrieve the API key from Key Vault
                    retrieved_secret = secret_client.get_secret(secret_name)
                    secret_key = retrieved_secret.value

                # Return the API key credential if secret_name is provided else
                # return the credential
                return AzureKeyCredential(secret_key) if secret_name else credential

            except AzureError as e:
                logger.error(f"Error accessing Key Vault: {str(e)}")
                raise

    except AzureError as e:
        logger.error(f"Azure error occurred: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
        raise

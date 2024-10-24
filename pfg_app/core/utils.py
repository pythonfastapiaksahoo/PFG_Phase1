import traceback
from datetime import date, datetime, timedelta

# from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import AzureError
from azure.identity import (
    ClientSecretCredential,
    CredentialUnavailableError,
    DefaultAzureCredential,
)
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import (
    AccountSasPermissions,
    BlobServiceClient,
    generate_container_sas,
)

from pfg_app import settings
from pfg_app.logger_module import logger

# from typing import Optional


def get_connection_string_with_access_token():

    try:
        # Use passwordless authentication via DefaultAzureCredential.

        credential = get_credential()

        # Call get_token() to get a token from Microsft Entra ID and
        # add it as the password in the URI.
        # Note the requested scope parameter in the call to get_token,
        # "https://ossrdbms-aad.database.windows.net/.default".
        access_token = credential.get_token(
            "https://ossrdbms-aad.database.windows.net/.default"
        ).token

        params = dict(
            param.split("=")
            for param in settings.azure_postgresql_connectionstring.split()
        )
        # Assign the extracted values to respective variables
        db_name = params.get("dbname")
        db_host = params.get("host")
        db_user = params.get("user", "ase_api_serviceconnector")
        ssl_mode = params.get("sslmode")

        db_uri = (
            "postgresql://"
            + f"{db_user}:{access_token}@{db_host}/{db_name}"
            # + f"?sslmode={ssl_mode}"
            + f"?sslmode={ssl_mode}&options=-csearch_path=pfg_schema"
        )

        return db_uri
    except Exception:
        logger.error(f"Error {traceback.format_exc()}")
        raise


# Shared function to get the correct credential based on the environment
# variable
def get_credential():
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

        if build_type == "prod" or build_type == "qa" or build_type == "dev":
            # Use Managed Identity for prod/qa/dev
            logger.info(f"Using Managed Identity for authentication in {build_type}.")
            try:
                # Automatically handles MI and other chained credentials
                credential = DefaultAzureCredential()
                logger.info("Managed Identity is available.")
                return credential
            except CredentialUnavailableError as e:
                logger.error(f"Managed Identity is not available: {str(e)}")
                raise

        else:
            # Use Key Vault to retrieve the API key for build_type
            logger.info(f"Using SPN for authentication in {build_type}.")

            # key_vault_url = settings.key_vault_url
            # # Credentials to access Key Vault (using client ID & secret for
            # # debug)
            tenant_id = settings.tenant_id
            client_id = settings.client_id
            client_secret = settings.client_secret

            try:
                # Try to create a ClientSecretCredential and access Key Vault
                credential = ClientSecretCredential(tenant_id, client_id, client_secret)

                # if secret_name:
                #     secret_client = SecretClient(
                #         vault_url=key_vault_url, credential=credential
                #     )

                #     # Retrieve the API key from Key Vault
                #     retrieved_secret = secret_client.get_secret(secret_name)
                #     secret_key = retrieved_secret.value

                # Return the API key credential if secret_name is provided else
                # return the credential
                logger.info("SPN is available.")
                return credential
                # return AzureKeyCredential(secret_key) if secret_name else credential

            except AzureError as e:
                logger.error(f"Error to get Credential with SPN: {str(e)}")
                raise

    except AzureError as e:
        logger.error(f"Azure error occurred: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
        raise


def get_secret_from_vault(credential, secret_name: str, settings_key: str):
    """Function to retrieve a secret from Azure Key Vault.

    Parameters:
    ----------
    credential : Credential
        Credential object for accessing Azure Key Vault.
    secret_name : str
        Name of the secret to retrieve from Azure Key Vault.

    Returns:
    -------
    str
        Value of the secret retrieved from Azure Key Vault.
    """

    try:
        key_vault_url = settings.key_vault_url
        secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

        # Retrieve the secret from Key Vault
        retrieved_secret = secret_client.get_secret(secret_name)
        secret_value = retrieved_secret.value

        return {"settings_key": settings_key, "secret": secret_value}

    except AzureError as e:
        logger.error(f"Error accessing Key Vault: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
        raise


def get_blob_securely(container_name, blob_path):
    """Function to securely retrieve a blob from Azure Blob Storage.

    Parameters:
    ----------
    container_name : str
        Name of the container in Azure Blob Storage.
    blob_path : str
        Path to the blob in Azure Blob Storage.

    Returns:
    -------
    Tuple containing the blob data and the content type.
    """

    try:
        # Get the credential
        credential = get_credential()

        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=credential
        )

        # Create a BlobClient
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_path
        )
        blob_properties = blob_client.get_blob_properties()
        content_type = (
            blob_properties.content_settings.content_type
        )  # Get the Content-Type dynamically

        # Download the blob data
        blob_data = blob_client.download_blob().readall()

        return blob_data, content_type

    except AzureError as e:
        logger.error(f"Error accessing Azure Blob Storage: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
        raise


# Recursive function to convert date objects to ISO format strings
def convert_dates(obj):
    if isinstance(obj, dict):
        # If obj is a dictionary, check each key-value pair
        for key, value in obj.items():
            obj[key] = convert_dates(value)
    elif isinstance(obj, list):
        # If obj is a list, check each element
        return [convert_dates(item) for item in obj]
    elif isinstance(obj, date):
        # If obj is a date, convert to ISO string
        return obj.isoformat()
    return obj


def get_container_sas(container_name: str):
    """Function to generate a shared access signature (SAS) token for a
    container in Azure Blob Storage."""

    account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"

    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient(
        account_url=account_url, credential=get_credential()
    )
    # Get the user delegation key using the managed identity
    user_delegation_key = blob_service_client.get_user_delegation_key(
        key_start_time=datetime.utcnow(),
        key_expiry_time=datetime.utcnow()
        + timedelta(hours=1),  # Set appropriate expiry time
    )

    # Generate SAS for the container using the user delegation key
    sas_token = generate_container_sas(
        account_name=blob_service_client.account_name,
        container_name=container_name,
        user_delegation_key=user_delegation_key,
        permission=AccountSasPermissions(
            read=True, write=True, list=True
        ),  # Set appropriate permissions
        expiry=datetime.utcnow() + timedelta(hours=1),  # Set appropriate expiry time
    )
    return sas_token

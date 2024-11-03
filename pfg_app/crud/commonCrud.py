import json
import time
import traceback

from azure.ai.formrecognizer import DocumentModelAdministrationClient
from azure.identity import ClientSecretCredential, DefaultAzureCredential

from pfg_app import settings
from pfg_app.logger_module import logger


def acquire_lock(container_client, LOCK_BLOB_NAME):

    lock_blob_client = container_client.get_blob_client(LOCK_BLOB_NAME)
    try:
        # Try creating the lock blob, set metadata to track lock creation time
        lock_blob_client.upload_blob(
            "lock", overwrite=False, metadata={"status": "in_progress"}
        )
        return True
    except Exception:
        # Lock blob already exists if upload_blob fails with overwrite=False
        return False


def release_lock(container_client, LOCK_BLOB_NAME):
    lock_blob_client = container_client.get_blob_client(LOCK_BLOB_NAME)
    if lock_blob_client.exists():
        lock_blob_client.delete_blob()


def set_task_status(container_client, task_id, status, details=None):
    # Convert status data to JSON and upload as a blob
    task_data = {"status": status, "details": details, "timestamp": time.time()}
    blob_client = container_client.get_blob_client(task_id)
    blob_client.upload_blob(json.dumps(task_data), overwrite=True)


def get_task_status(container_client, task_id):
    blob_client = container_client.get_blob_client(task_id)
    if blob_client.exists():
        blob_data = blob_client.download_blob().readall()
        return json.loads(blob_data)
    else:
        return {"status": "not found"}


def copy_models_in_background(
    container_client, task_id, source_di_name, target_di_name
):
    try:
        set_task_status(
            container_client, task_id, "in_progress", "Starting model copy process"
        )

        source_endpoint = f"https://{source_di_name}.cognitiveservices.azure.com/"
        target_endpoint = f"https://{target_di_name}.cognitiveservices.azure.com/"

        try:
            # check if source is accessible with System Identity
            source_credential = DefaultAzureCredential()
            # Initialize the Form Recognizer client
            source_document_model_admin_client = DocumentModelAdministrationClient(
                source_endpoint, source_credential
            )
            source_document_model_admin_client.get_document_model(
                model_id="prebuilt-invoice"
            )
        except Exception:
            # Check if source is accessible with Service Principal
            try:
                source_credential = ClientSecretCredential(
                    settings.tenant_id, settings.client_id, settings.client_secret
                )
                # Initialize the Form Recognizer client
                source_document_model_admin_client = DocumentModelAdministrationClient(
                    source_endpoint, source_credential
                )
                source_document_model_admin_client.get_document_model(
                    model_id="prebuilt-invoice"
                )

            except Exception as e:
                set_task_status(
                    container_client,
                    task_id,
                    "failed",
                    f"Error accessing source DI: {str(e)}",
                )
                return False

        try:
            # check if target is accessible with System Identity
            target_credential = DefaultAzureCredential()
            # Initialize the Form Recognizer client
            target_document_model_admin_client = DocumentModelAdministrationClient(
                target_endpoint, target_credential
            )
            target_document_model_admin_client.get_document_model(
                model_id="prebuilt-invoice"
            )
        except Exception:
            # Check if target is accessible with Service Principal
            try:
                target_credential = ClientSecretCredential(
                    settings.tenant_id, settings.client_id, settings.client_secret
                )
                # Initialize the Form Recognizer client
                target_document_model_admin_client = DocumentModelAdministrationClient(
                    target_endpoint, target_credential
                )
                target_document_model_admin_client.get_document_model(
                    model_id="prebuilt-invoice"
                )
            except Exception as e:
                set_task_status(
                    container_client,
                    task_id,
                    "failed",
                    f"Error accessing target DI: {str(e)}",
                )
                return False

        try:
            # Get all models from Source
            source_models = source_document_model_admin_client.list_document_models()

            # Convert the list to a readable format
            source_model_list = [
                {
                    "model_id": model.model_id,
                    "description": model.description,
                    "created_on": model.created_on,
                }
                for model in source_models
            ]

            # Get all models from Target
            target_models = target_document_model_admin_client.list_document_models()

            # Convert the list to a readable format
            target_model_list = [
                {
                    "model_id": model.model_id,
                    "description": model.description,
                    "created_on": model.created_on,
                }
                for model in target_models
            ]
            target_model_id_list = [model["model_id"] for model in target_model_list]
            copy_process_info = []
            for source_model in source_model_list:
                set_task_status(
                    container_client,
                    task_id,
                    "in_progress",
                    {
                        "source_models": source_model_list,
                        "copy_process_info": copy_process_info,
                    },
                )
                if not source_model["model_id"].startswith("prebuilt-"):
                    try:
                        # copy only if the model is not already present in the target
                        if source_model["model_id"] not in target_model_id_list:
                            # Generate copy authorization
                            copy_auth = target_document_model_admin_client.get_copy_authorization(  # noqa: E501
                                model_id=source_model["model_id"]
                            )

                            # Copy the model from source to target
                            copy_result = source_document_model_admin_client.begin_copy_document_model_to(  # noqa: E501
                                model_id=source_model["model_id"],
                                target={
                                    "targetResourceId": copy_auth["targetResourceId"],
                                    "targetResourceRegion": copy_auth[
                                        "targetResourceRegion"
                                    ],
                                    "targetModelId": copy_auth["targetModelId"],
                                    "accessToken": copy_auth["accessToken"],
                                    "expirationDateTime": copy_auth[
                                        "expirationDateTime"
                                    ],
                                    "targetModelLocation": copy_auth[
                                        "targetModelLocation"
                                    ],
                                },
                            )
                            # If copy not done in 1 minute, print status as copy failed
                            start_time = time.time()
                            while not copy_result.done():
                                logger.info(
                                    f"Copying model {source_model['model_id']} from "
                                    + f"{source_endpoint} to {target_endpoint}"
                                )
                                time.sleep(5)  # Polling interval
                                if time.time() - start_time > 60:
                                    logger.info(
                                        "Copy operation taking longer than expected. Aborting."  # noqa: E501
                                    )
                                    break
                            else:
                                logger.info(
                                    f"Model {source_model['model_id']} copied successfully"  # noqa: E501
                                )

                            if copy_result.done():
                                copy_process_info.append(
                                    {
                                        "model_id": source_model["model_id"],
                                        "status": "success",
                                    }
                                )  # copy_result.status()
                            else:
                                copy_process_info.append(
                                    {
                                        "model_id": source_model["model_id"],
                                        "status": "failed",
                                    }
                                )  # copy_result.status()
                        else:
                            copy_process_info.append(
                                {
                                    "model_id": source_model["model_id"],
                                    "status": "already exists",
                                }
                            )

                    except Exception as e:
                        logger.error(traceback.format_exc())
                        copy_process_info.append(
                            {
                                "model_id": source_model["model_id"],
                                "status": "failed",
                                "error": str(e),
                            }  # noqa: E501
                        )

                        continue
                else:
                    copy_process_info.append(
                        {
                            "model_id": source_model["model_id"],
                            "status": "prebuilt model",
                        }
                    )

            # Get all models from Target
            target_models = target_document_model_admin_client.list_document_models()

            # Convert the list to a readable format
            target_model_list = [
                {
                    "model_id": model.model_id,
                    "description": model.description,
                    "created_on": model.created_on,
                }
                for model in target_models
            ]
            set_task_status(
                container_client,
                task_id,
                "completed",
                {
                    "source_models": source_model_list,
                    "target_models": target_model_list,
                    "copy_process_info": copy_process_info,
                },
            )

            return True
        except Exception as e:
            logger.error(traceback.format_exc())
            set_task_status(
                container_client, task_id, "failed", f"Error copying models: {str(e)}"
            )
            return False

    except Exception as e:
        logger.error(traceback.format_exc())
        set_task_status(container_client, task_id, "failed", f"Error: {str(e)}")
        return False
    finally:
        # Release the lock once done
        release_lock(container_client, "copy-process-lock")

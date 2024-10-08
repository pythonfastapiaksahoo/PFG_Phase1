import time
import traceback

from azure.ai.formrecognizer import (
    DocumentAnalysisClient,
    DocumentModelAdministrationClient,
)
from azure.core.exceptions import ResourceNotFoundError
from azure.core.pipeline.policies import RetryPolicy

from pfg_app.core.utils import get_credential
from pfg_app.logger_module import logger


def get_fr_data(
    inputdata_list, API_version, endpoint, model_type, inv_model_id="prebuilt-invoice"
):
    output_data = []
    fr_model_status = 0
    fr_model_msg = ""
    fr_status = "Fail"
    isComposed = False
    template = ""

    # Initialize the Form Recognizer client
    document_analysis_client = DocumentAnalysisClient(
        endpoint, get_credential(), api_version=API_version
    )
    # if model_type is custom, then we use the inv_model_id to get the data
    # else we use the prebuilt-invoice model
    inv_model_id = "prebuilt-invoice" if model_type == "prebuilt" else inv_model_id
    for input_data in inputdata_list:
        try:
            poller = document_analysis_client.begin_analyze_document(
                model_id=inv_model_id, document=input_data
            )

            result = poller.result().to_dict()

            # Process the result
            if result and result["model_id"] == inv_model_id:
                output_data.append(result)
                if model_type == "custom":
                    doctype = result["documents"][0]["doc_type"]
                    if doctype.split(":")[0] != "custom":
                        isComposed = True
                        template = doctype.split(":")[-1]
                    else:
                        isComposed = False
                        template = doctype.split(":")[-1]
            else:
                fr_model_status = 0
                fr_model_msg = "Azure Form Recognizer returned no results."
        except Exception as e:
            fr_model_status = 0
            fr_model_msg = f"Azure Form Recognizer error: {str(e)}"

    if len(inputdata_list) == len(output_data):
        fr_model_status = 1
        fr_model_msg = "succeeded"
        fr_status = "succeeded"

    if model_type == "custom":
        return (
            fr_model_status,
            fr_model_msg,
            output_data,
            fr_status,
            isComposed,
            template,
        )

    return fr_model_status, fr_model_msg, output_data, fr_status


def copy_model(
    source_endpoint,
    target_endpoint,
    source_credential,
    target_credential,
    source_model_id,
):
    """Copy a model from one Form Recognizer resource to another.

    :param source_endpoint: The endpoint of the source Form Recognizer
        resource.
    :param target_endpoint: The endpoint of the target Form Recognizer
        resource.
    :param source_credential: The credential for the source Form
        Recognizer resource.
    :param target_credential: The credential for the target Form
        Recognizer resource.
    :param source_model_id: The ID of the model to copy.
    :return: The status of the copy operation.
    """
    # Initialize the Form Recognizer client
    source_client = DocumentModelAdministrationClient(
        endpoint=source_endpoint, credential=source_credential
    )
    target_client = DocumentModelAdministrationClient(
        endpoint=target_endpoint, credential=target_credential
    )

    # Generate copy authorization
    copy_auth = target_client.get_copy_authorization(model_id=source_model_id)

    # Copy the model from source to target
    copy_result = source_client.begin_copy_document_model_to(
        model_id=source_model_id,
        target={
            "targetResourceId": copy_auth["targetResourceId"],
            "targetResourceRegion": copy_auth["targetResourceRegion"],
            "targetModelId": copy_auth["targetModelId"],
            "accessToken": copy_auth["accessToken"],
            "expirationDateTime": copy_auth["expirationDateTime"],
            "targetModelLocation": copy_auth["targetModelLocation"],
        },
    )

    while not copy_result.done():
        logger.info(
            f"Copying model {source_model_id} from "
            + f"{source_endpoint} to {target_endpoint}"
        )
        time.sleep(5)  # Polling interval

    # Once done, get the status
    copy_result.result()

    logger.info(f"Copy completed with status: {copy_result.status()}")

    return copy_result.status()


def call_form_recognizer(
    input_file, endpoint, api_version, invoice_model_id="prebuilt-invoice", locale="en"
):

    # Create a custom retry policy
    custom_retry_policy = RetryPolicy(
        retry_on_status_codes=[429],  # Retry on HTTP 429 Too Many Requests
        retry_total=5,  # Maximum retries
        retry_backoff_factor=1,  # Exponential backoff factor
        retry_backoff_max=60,  # Max backoff time in seconds
    )

    # Initialize the Form Recognizer client
    document_analysis_client = DocumentAnalysisClient(
        endpoint,
        get_credential(),
        api_version=api_version,
        retry_policy=custom_retry_policy,
    )

    poller = document_analysis_client.begin_analyze_document(
        model_id=invoice_model_id, document=input_file, locale=locale
    )

    result = poller.result().to_dict()

    return result


def analyze_form(
    input_file, endpoint, api_version, invoice_model_id="prebuilt-invoice", locale="en"
):
    try:
        # call the call_form_recognizer function
        result = call_form_recognizer(
            input_file, endpoint, api_version, invoice_model_id, locale
        )
        return result
    except Exception:
        logger.error(f"Error in Form Recognizer: analyzeForm {traceback.format_exc()}")
        return {"message": "failure to fetch"}


def train_model(endpoint, model_id, blob_container_url, prefix):
    try:
        # Initialize the Form Recognizer client
        document_model_admin_client = DocumentModelAdministrationClient(
            endpoint, get_credential()
        )
        #
        poller = document_model_admin_client.begin_build_document_model(
            build_mode="template",
            model_id=model_id,
            blob_container_url=blob_container_url,
            description=f"Model for {model_id}",
            prefix=prefix,
        )

        model = poller.result().to_dict()

        return {"message": "success", "result": model}

    except Exception:
        logger.error(f"Error in Form Recognizer: train_model {traceback.format_exc()}")
        return {"message": f"error {traceback.format_exc()}", "result": None}


def compose_model(endpoint, model_id, model_ids):

    # Initialize the Form Recognizer client
    document_model_admin_client = DocumentModelAdministrationClient(
        endpoint, get_credential()
    )

    poller = document_model_admin_client.begin_compose_document_model(
        component_model_ids=model_ids,
        model_id=model_id,
        description=f"Composed model for {model_id}",
    )

    model = poller.result().to_dict()

    return model


def get_model(endpoint, model_id):
    try:
        # Initialize the Form Recognizer client
        document_model_admin_client = DocumentModelAdministrationClient(
            endpoint, get_credential()
        )

        model = document_model_admin_client.get_document_model(model_id=model_id)

        return {"message": "success", "result": model}
    # Capture specific ResourceNotFoundError
    except ResourceNotFoundError as e:
        logger.error(f"Model not found: {model_id}. Error details: {str(e)}")
        return {"message": "error", "result": None, "details": "Model not found"}

    except Exception:
        logger.error(f"Error in Form Recognizer: get_model {traceback.format_exc()}")
        return {
            "message": "error",
            "result": None,
            "details": f"Error in Form Recognizer {traceback.format_exc()}",
        }

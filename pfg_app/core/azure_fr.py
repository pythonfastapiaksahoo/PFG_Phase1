import time

from azure.ai.formrecognizer import (
    DocumentAnalysisClient,
    DocumentModelAdministrationClient,
)
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
            f"Copying model {source_model_id} from {source_endpoint} to {target_endpoint}"
        )
        time.sleep(5)  # Polling interval

    # Once done, get the status
    copy_result.result()

    logger.info(f"Copy completed with status: {copy_result.status()}")

    return copy_result.status()


def call_form_recognizer(
    input_file, endpoint, api_version, invoice_model_id="prebuilt-invoice"
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

    # # if model_type is custom, then we use the inv_model_id to get the data
    # # else we use the prebuilt-invoice model
    # inv_model_id = "prebuilt-invoice" if model_type == "prebuilt" else inv_model_id

    poller = document_analysis_client.begin_analyze_document(
        model_id=invoice_model_id, document=input_file
    )

    result = poller.result().to_dict()

    return result

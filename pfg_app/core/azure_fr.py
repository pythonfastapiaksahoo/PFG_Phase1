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
from azure.core.exceptions import HttpResponseError

def get_fr_data(
    inputdata_list, API_version, endpoint, model_type, inv_model_id="prebuilt-invoice"
):
    output_data = []
    fr_model_status = 0
    fr_model_msg = ""
    fr_status = "Fail"
    isComposed = False
    template = ""
    # Create a custom retry policy
    custom_retry_policy = RetryPolicy(
        retry_on_status_codes=[429],  # Retry on HTTP 429 Too Many Requests
        retry_total=20,  # Maximum retries
        retry_backoff_factor=1,  # Exponential backoff factor
        retry_backoff_max=60,  # Max backoff time in seconds
    )

    # Initialize the Form Recognizer client
    document_analysis_client = DocumentAnalysisClient(
        endpoint,
        get_credential(),
        api_version=API_version,
        retry_policy=custom_retry_policy,
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

            # logger.info(f"FUNC => [get_fr_data result] : {result}")

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
        retry_total=20,  # Maximum retries
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
    if invoice_model_id == "prebuilt-invoice":
        # Call the Form Recognizer service
        poller = document_analysis_client.begin_analyze_document(
            model_id=invoice_model_id,
            document=input_file,
            locale=locale,
            features=["keyValuePairs"],
        )
    else:
        poller = document_analysis_client.begin_analyze_document(
            model_id=invoice_model_id,
            document=input_file,
            locale=locale,
        )

    result = poller.result().to_dict()

    # logger.info(f"FUNC => [call_form_recognizer] result: {result}")

    return result


def analyze_form(
    input_file, endpoint, api_version, invoice_model_id="prebuilt-invoice", locale="en"
):
    try:
        # call the call_form_recognizer function
        result = call_form_recognizer(
            input_file, endpoint, api_version, invoice_model_id, locale
        )
        # logger.info(f"FUNC => [analyzeForm] result: {result}")
        return result
    except Exception:
        logger.error(f"Error in Form Recognizer: analyzeForm {traceback.format_exc()}")
        return {"message": "failure to fetch"}


# def train_model(endpoint, model_id, blob_container_url, prefix):
#     try:
#         # Initialize the Form Recognizer client
#         document_model_admin_client = DocumentModelAdministrationClient(
#             endpoint, get_credential()
#         )
#         #
#         poller = document_model_admin_client.begin_build_document_model(
#             build_mode="template",
#             model_id=model_id,
#             blob_container_url=blob_container_url,
#             description=f"Model for {model_id}",
#             prefix=prefix,
#         )

#         model = poller.result().to_dict()
#         logger.info(f"FUNC => [train_model] model: {model}")

#         return {"message": "success", "result": model}

#     except Exception:
#         logger.error(f"Error in Form Recognizer: train_model {traceback.format_exc()}")
#         return {"message": f"error {traceback.format_exc()}", "result": None}


def train_model(endpoint, model_id, blob_container_url, prefix, max_retries=3):
    attempt = 0

    while attempt < max_retries:
        try:
            # Initialize the Form Recognizer client
            document_model_admin_client = DocumentModelAdministrationClient(
                endpoint, get_credential()
            )

            # Start training
            poller = document_model_admin_client.begin_build_document_model(
                build_mode="template",
                model_id=model_id,
                blob_container_url=blob_container_url,
                description=f"Model for {model_id}",
                prefix=prefix,
            )

            model = poller.result().to_dict()
            logger.info(f"FUNC => [train_model] model: {model}")

            return {"message": "success", "result": model}

        except HttpResponseError as e:
            if "InvalidContentSourceFormat" in str(e):
                logger.error(f"Error: Invalid content source. Retrying... Attempt {attempt + 1} of {max_retries}")
                return {f"message": f"Invalid content source. Retrying... Attempt {attempt + 1} of {max_retries}", "result": None}
            else:
                logger.error(f"Error in Form Recognizer: train_model {traceback.format_exc()}")
                return {
                    "message": "An unexpected error occurred. Please try again later.",
                    "result": None
                }

        except Exception as e:
            logger.error(f"Unexpected error: {traceback.format_exc()}")
            return {
                "message": f"Unexpected error: {str(e)}",
                "result": None
            }

        attempt += 1
        time.sleep(5)  # Wait for 5 seconds before retrying

    return {
        "message": "Training failed after multiple attempts. Please check your data and try again later.",
        "result": None
    }

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
    logger.info(f"FUNC => [compose_model] model: {model}")
    return model


def get_model(endpoint, model_id):
    try:
        # Initialize the Form Recognizer client
        document_model_admin_client = DocumentModelAdministrationClient(
            endpoint, get_credential()
        )

        model = document_model_admin_client.get_document_model(model_id=model_id)
        logger.info(f"FUNC => [get_model] model: {model}")
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


# Helper function to convert snake_case to camelCase
def snake_to_camel(snake_str):
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


# Helper function to recursively convert all dictionary keys
# from snake_case to camelCase
def convert_snake_to_camel(data):
    if isinstance(data, dict):
        new_data = {}
        for key, value in data.items():
            new_key = snake_to_camel(key)
            new_data[new_key] = convert_snake_to_camel(
                value
            )  # Recursively apply to nested structures
        return new_data
    elif isinstance(data, list):
        return [
            convert_snake_to_camel(item) for item in data
        ]  # Recursively apply to lists
    else:
        return data


# Helper function to flatten polygons with x and y dictionaries into plain lists
def flatten_polygon(polygon):
    if isinstance(polygon, list):
        flattened = []
        for point in polygon:
            if isinstance(point, dict) and "x" in point and "y" in point:
                flattened.extend([point["x"], point["y"]])
            else:
                flattened.append(point)
        return flattened
    return polygon


# Function to process polygons in the data and flatten them
def process_polygons(data):
    if isinstance(data, dict):
        for key, value in data.items():
            if key == "polygon" and isinstance(value, list):
                data[key] = flatten_polygon(value)
            else:
                data[key] = process_polygons(value)  # Recursively apply to nested dicts
    elif isinstance(data, list):
        return [process_polygons(item) for item in data]
    return data


# Function to add page info if missing in label values
def add_page_to_labels(labels_data):
    if "labels" in labels_data:
        for label in labels_data["labels"]:
            for value in label.get("value", []):
                # If 'page' is not present, add "page": 1
                if "page" not in value:
                    value["page"] = 1
    return labels_data

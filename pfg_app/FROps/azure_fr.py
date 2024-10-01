import json
import time

import requests

from pfg_app.logger_module import logger


def get_fr_data(
    inputdata_list,
    file_type,
    apim_key,
    API_version,
    endpoint,
    model_type,
    inv_model_id="",
):
    output_data = []
    fr_model_status = 0
    fr_model_msg = ""
    fr_status = "Fail"
    isComposed = False
    template = ""

    customURL = (
        f"{endpoint}/formrecognizer/documentModels/{inv_model_id}:analyze"
        f"?api-version={API_version}"
    )
    prebuiltURL = (
        f"{endpoint}/formrecognizer/documentModels/prebuilt-invoice:analyze"
        f"?api-version={API_version}"
    )

    if model_type == "custom":
        form_recognizer_url = customURL
    elif model_type == "prebuilt":
        form_recognizer_url = prebuiltURL
    else:
        form_recognizer_url = prebuiltURL

    for input_data in inputdata_list:
        data = {}
        fr_status = "Fail"
        fr_model_msg = "Azure get data error!"
        succeeded = False
        maxTry = 20
        cnt = 1
        maxTry1 = 15
        cnt1 = 1
        while True:
            headers = {
                "Content-Type": "application/pdf",
                "Ocp-Apim-Subscription-Key": apim_key,
            }
            response = requests.post(
                form_recognizer_url, headers=headers, data=input_data, timeout=60
            )

            logger.info(f"response.status_code:ln 23 {response.status_code}")

            if "Operation-Location" in response.headers:
                operation_location = response.headers["Operation-Location"]
                break
            time.sleep(5)
            cnt1 += 1
            if cnt1 == maxTry1:
                break

        while True:
            response = requests.get(operation_location, headers=headers, timeout=60)
            result = response.json()
            logger.info(f"response.status_code:{response.status_code}")
            if response.status_code == 429:
                time.sleep(3)
                cnt += 1
                continue
            if response.status_code == 200:
                if result["status"] == "succeeded":
                    succeeded = True
                    break
                if result["status"] == "running":
                    time.sleep(2)
                    cnt += 1
                    continue
                if result["status"] == "failed":
                    break
                cnt += 1
                if cnt == maxTry:
                    break
                continue
            break

        if succeeded:
            data = response.json()
            try:
                data = json.loads(json.dumps(response.json()).replace("null", '""'))
            except BaseException:
                pass
            output_data.append(data)
            if model_type == "custom":
                doctype = data["analyzeResult"]["documents"][0]["docType"]
                if doctype.split(":")[0] != "custom":
                    isComposed = True
                    template = doctype.split(":")[-1]
                else:
                    isComposed = False
                    template = doctype.split(":")[-1]
        else:
            fr_model_status = 0
            fr_model_msg = "(Bad Request).Failed to download file from input URL."

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

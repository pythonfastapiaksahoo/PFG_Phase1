import base64
import json
from io import BytesIO

import requests
from pdf2image import convert_from_bytes

from pfg_app import settings
from pfg_app.core.utils import get_credential
from pfg_app.logger_module import logger


def get_open_ai_token():
    credential = get_credential()
    token = credential.get_token("https://cognitiveservices.azure.com/.default")
    access_token = token.token
    return access_token


def stampDataFn(blob_data, prompt):
    image_content = []
    pdf_img = convert_from_bytes(blob_data)
    # pdf_img = convert_from_bytes(
    #     blob_data, poppler_path=r"C:\\poppler-24.07.0\\Library\\bin"
    # )
    for page in pdf_img:

        buffered = BytesIO()
        page.save(buffered, format="JPEG")
        encoded_image = base64.b64encode(buffered.getvalue()).decode("ascii")
        image_content.append(
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{encoded_image}"},
            }
        )
    data = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    *image_content,
                ],
            }
        ],
        "temperature": 0.7,
        "top_p": 0.95,
        "max_tokens": 800,
    }

    # Make the API call to Azure OpenAI
    access_token = get_open_ai_token()

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    response = requests.post(
        settings.open_ai_endpoint, headers=headers, json=data, timeout=600
    )

    # Check and process the response

    if response.status_code == 200:
        result = response.json()
        for choice in result["choices"]:
            content = choice["message"]["content"].strip()
            logger.info(f"Content: {content}")
            # TODO not sure how do we handle when we get multiple choices
    else:
        logger.error(f"Error: {response.status_code}, {response.text}")
    cl_data = (
        content.replace("json", "")
        .replace("\n", "")
        .replace("'''", "")
        .replace("```", "")
    )
    try:
        stampData = json.loads(cl_data)
    except BaseException:
        try:
            cl_data_corrected = cl_data.replace("'", '"')
            stampData = json.loads(cl_data_corrected)
        except BaseException:
            stampData = cl_data

    return stampData


def VndMatchFn(metaVendorName, doc_VendorName, metaVendorAdd, doc_VendorAddress):
    vndMth_ck = 0
    vndMth_address_ck = 0

    data = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            f"vendor1 = {metaVendorName}, vendor2 = {doc_VendorName}, "
                            + f"vendor1Address = {metaVendorAdd}, "
                            + f"vendor2Address = {doc_VendorAddress}.You are given "
                            + "vendor data from two sources: vendor1 from master data"
                            + "and vendor2 from an OCR model. Your task is to"
                            + "confirm if both vendor names and their addresses are"
                            + " matching based on location,because in few cases it "
                            + "would be mentioned in short."
                            + ". Compare the vendor names, ignoring case"
                            + "sensitivity and trimming extra spaces.For addresses, "
                            + "normalize the text by handling common abbreviations"
                            + "like 'Road' and 'RD'.Return response in JSON format as"
                            + "{'vendorMatching': 'yes/no','addressMatching': 'yes/no'}"
                            + "only with two keys: vendorMatching and addressMatching,"
                            + "each having a value of either 'yes' or 'no' based on"
                            + "the comparison without any explanation.Give me response"
                            + "in Json Format in {'vendorMatching': 'yes/no',"
                            + "'addressMatching': 'yes/no'} without any explanation"
                        ),
                    }
                ],
            }
        ]
    }

    # Make the API call to Azure OpenAI
    access_token = get_open_ai_token()

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    response = requests.post(
        settings.open_ai_endpoint, headers=headers, json=data, timeout=60
    )

    # Check and process the response
    if response.status_code == 200:
        result = response.json()
        for choice in result["choices"]:
            content = choice["message"]["content"].strip()
            logger.info(f"Content: {content}")
            # TODO not sure how do we handle when we get multiple choices
    else:
        logger.error(f"Error: {response.status_code}, {response.text}")

    cl_mtch = (
        content.replace("json", "")
        .replace("\n", "")
        .replace("'''", "")
        .replace("```", "")
    )
    try:
        vndMth = json.loads(cl_mtch)
        if vndMth["vendorMatching"] == "yes":
            vndMth_ck = 1
        if vndMth["addressMatching"] == "yes":
            vndMth_address_ck = 1
    except BaseException:
        try:
            cl_data_corrected = cl_mtch.replace("'", '"')
            vndMth = json.loads(cl_data_corrected)
            if vndMth["vendorMatching"] == "yes":
                vndMth_ck = 1
            if vndMth["addressMatching"] == "yes":
                vndMth_address_ck = 1

        except BaseException:
            vndMth = cl_mtch
            if '"vendorMatching": "yes"' in content:
                vndMth_ck = 1
            if '"addressMatching": "yes"' in content:
                vndMth_address_ck = 1

    return vndMth_ck, vndMth_address_ck

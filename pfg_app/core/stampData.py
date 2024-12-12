import base64
import json
import random
import re
import time
import traceback
from io import BytesIO

import requests
from dateutil import parser
from pdf2image import convert_from_bytes

from pfg_app import settings
from pfg_app.core.azure_fr import analyze_form
from pfg_app.core.utils import get_credential
from pfg_app.logger_module import logger


def get_open_ai_token():
    credential = get_credential()
    token = credential.get_token("https://cognitiveservices.azure.com/.default")
    access_token = token.token
    return access_token


def stampDataFn(blob_data, prompt):
    try:
        time.sleep(0.3)
        image_content = []

        # # Read the PDF file as bytes
        # with open(blob_data, "rb") as pdf_file:
        #     pdf_data = pdf_file.read()

        pdf_img = convert_from_bytes(blob_data)
        # pdf_img = convert_from_bytes(
        #     blob_data, poppler_path=r"C:\\poppler-24.07.0\\Library\\bin"
        # )
        for page in pdf_img:

            buffered = BytesIO()
            page.save(buffered, format="PNG")
            encoded_image = base64.b64encode(buffered.getvalue()).decode("ascii")
            image_content.append(
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{encoded_image}"},
                }
            )
        endpoint = settings.form_recognizer_endpoint
        resp = analyze_form(blob_data, endpoint, "2023-07-31", "prebuilt-read")
        if "message" not in resp:
            ocr_text = resp["content"]
        else:
            ocr_text = ""

        # print("ocr_text: ", ocr_text)

        # Define the regex pattern
        pattern = r"(STR#.*?Receive.{0,10})"

        # Search for the pattern
        match = re.search(pattern, ocr_text, re.DOTALL)

        # Extract and print the matched portion
        if match:
            extracted_data = match.group(1)
            logger.info("Extracted Data:")
            logger.info(extracted_data)
            data = {
                "messages": [
                    {"role": "system", "content": [{"type": "text", "text": prompt}]},
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": extracted_data},
                            *image_content,
                        ],
                    },
                ],
                "temperature": 0.1,
                "top_p": 0.95,
                # "max_tokens": 800,
            }
        else:
            logger.info("No match found.")
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
                "temperature": 0.1,
                "top_p": 0.95,
                # "max_tokens": 800,
            }

        # Make the API call to Azure OpenAI
        access_token = get_open_ai_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        retry_count = 0
        max_retries = 5
        while retry_count < max_retries:
            response = requests.post(
                settings.open_ai_endpoint, headers=headers, json=data, timeout=600
            )

            # Check and process the response
            if response.status_code == 200:
                result = response.json()
                for choice in result["choices"]:
                    content = choice["message"]["content"].strip()
                    logger.info(f"Content: {content}")
                break
            elif response.status_code == 429:  # Handle rate limiting
                retry_after = int(response.headers.get("Retry-After", 5))
                logger.info(f"Rate limit hit. Retrying after {retry_after} seconds...")
                time.sleep(retry_after)
            else:
                logger.info(f"Error: {response.status_code}, {response.text}")
                retry_count += 1
                wait_time = 2**retry_count + random.uniform(0, 1)  # noqa: S311
                logger.info(f"Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
        if retry_count == max_retries:
            logger.error("Max retries reached. Exiting.")
            content = json.dumps(
                {
                    "StampFound": "Max retries reached",
                    "NumberOfPages": "Max retries reached",
                    "MarkedDept": "Max retries reached",
                    "MarkedStore": "Max retries reached",
                    "MarkedInvoice": "Max retries reached",
                    "Confirmation": "Max retries reached",
                    "ReceivingDate": "Max retries reached",
                    "Receiver": "Max retries reached",
                    "Department": "Max retries reached",
                    "Store Number": "Max retries reached",
                    "VendorName": "Max retries reached",
                    "InvoiceID": "Max retries reached",
                    "Currency": "CAD",
                }
            )

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
    except Exception:
        logger.info(traceback.format_exc())
        stampData = {
            "StampFound": "Response not found",
            "NumberOfPages": "Response not found",
            "MarkedDept": "Response not found",
            "MarkedStore": "Response not found",
            "MarkedInvoice": "Response not found",
            "Confirmation": "Response not found",
            "ReceivingDate": "Response not found",
            "Receiver": "Response not found",
            "Department": "Response not found",
            "Store Number": "Response not found",
            "VendorName": "Response not found",
            "InvoiceID": "Response not found",
            "Currency": "CAD",
            "CreditNote": "NA",

        }

    return stampData


# def stampDataFn(blob_data, prompt):
#     try:
#         time.sleep(0.3)
#         image_content = []

#         pdf_img = convert_from_bytes(blob_data)
#         # pdf_img = convert_from_bytes(
#         #     blob_data, poppler_path=r"C:\\poppler-24.07.0\\Library\\bin"
#         # )
#         for page in pdf_img:

#             buffered = BytesIO()
#             page.save(buffered, format="PNG")
#             encoded_image = base64.b64encode(buffered.getvalue()).decode("ascii")
#             image_content.append(
#                 {
#                     "type": "image_url",
#                     "image_url": {"url": f"data:image/png;base64,{encoded_image}"},
#                 }
#             )
#         data = {
#             "messages": [
#                 {
#                     "role": "user",
#                     "content": [
#                         {"type": "text", "text": prompt},
#                         *image_content,
#                     ],
#                 }
#             ],
#             "temperature": 0.7,
#             "top_p": 0.95,
#             "max_tokens": 800,
#         }

#         # Make the API call to Azure OpenAI
#         access_token = get_open_ai_token()

#         headers = {
#             "Authorization": f"Bearer {access_token}",
#             "Content-Type": "application/json",
#         }
#         response = requests.post(
#             settings.open_ai_endpoint, headers=headers, json=data, timeout=600
#         )

#         # Check and process the response

#         if response.status_code == 200:
#             result = response.json()
#             for choice in result["choices"]:
#                 content = choice["message"]["content"].strip()
#                 logger.info(f"Content: {content}")
#                 # TODO not sure how do we handle when we get multiple choices
#         else:
#             logger.error(f"Error: {response.status_code}, {response.text}")
#         cl_data = (
#             content.replace("json", "")
#             .replace("\n", "")
#             .replace("'''", "")
#             .replace("```", "")
#         )
#         try:
#             stampData = json.loads(cl_data)
#         except BaseException:
#             try:
#                 cl_data_corrected = cl_data.replace("'", '"')
#                 stampData = json.loads(cl_data_corrected)
#             except BaseException:
#                 stampData = cl_data
#     except Exception:
#         logger.error(traceback.format_exc())
#         stampData = {
#             "StampFound": "Response not found",
#             "NumberOfPages": "Response not found",
#             "MarkedDept": "Response not found",
#             "MarkedStore": "Response not found",
#             "MarkedInvoice": "Response not found",
#             "Confirmation": "Response not found",
#             "ReceivingDate": "Response not found",
#             "Receiver": "Response not found",
#             "Department": "Response not found",
#             "Store Number": "Response not found",
#             "VendorName": "Response not found",
#             "InvoiceID": "Response not found",
#             "Currency": "Response not found",
#         }

#     return stampData


def VndMatchFn(metaVendorName, doc_VendorName, metaVendorAdd, doc_VendorAddress):
    vndMth_ck = 0
    vndMth_address_ck = 0
    try:
        data = {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                f"vendor1={metaVendorName}, vendor2={doc_VendorName}, "
                                + f"vendor1Address={metaVendorAdd}, vendor2Address={doc_VendorAddress}. "  # noqa: E501
                                + "You are given vendor data from two sources: 'vendor1' from master data "  # noqa: E501
                                + "and 'vendor2' from an OCR model. Your task is to: "
                                + "1) Check if the vendor names match, ignoring case "
                                + "sensitivity and trimming extra spaces. "
                                + "2) Verify if the vendor2 address contains the vendor"
                                + " store information from the master address (vendor1Address), "  # noqa: E501
                                + "including postal code and country if they are present. "  # noqa: E501
                                + "3) Confirm if the addresses correspond to the same"
                                + "location, even if vendor2Address is in a shorter form. "  # noqa: E501
                                + "Use AI to normalize the text, handle abbreviations "
                                + "(e.g., 'Road' vs. 'Rd'), and account for possible "
                                + "formatting differences. "
                                + "Provide your response strictly in JSON format as: "
                                + "{'vendorMatching': 'yes/no', 'addressMatching': 'yes/no'},"  # noqa: E501
                                + "where 'vendorMatching' "
                                + "indicates if the names match, and 'addressMatching' "
                                + "verifies if the addresses match, including postal code and country. "  # noqa: E501
                                + "Return the JSON response only, without any explanation or additional information."  # noqa: E501
                            ),
                        }
                    ],
                }
            ]
        }

        # data = {
        #     "messages": [
        #         {
        #             "role": "user",
        #             "content": [
        #                 {
        #                     "type": "text",
        #                     "text": (
        #                         f"vendor1={metaVendorName},vendor2 = {doc_VendorName}, "
        #                         + f"vendor1Address = {metaVendorAdd}, "
        #                         + f"vendor2Address = {doc_VendorAddress}.You are given "
        #                         + "vendor data from two sources: vendor1 from master data"  # noqa: E501
        #                         + "and vendor2 from an OCR model. Your task is to"
        #                         + "confirm if both vendor names and their addresses are"
        #                         + " matching based on location,because in few cases it "
        #                         + "would be mentioned in short."
        #                         + ". Compare the vendor names, ignoring case"
        #                         + "sensitivity and trimming extra spaces.For addresses, "  # noqa: E501
        #                         + "normalize the text by handling common abbreviations"
        #                         + "like 'Road' and 'RD'.Return response in JSON format as"  # noqa: E501
        #                         + "{'vendorMatching': 'yes/no','addressMatching': 'yes/no'}"  # noqa: E501
        #                         + "only with two keys: vendorMatching and addressMatching,"  # noqa: E501
        #                         + "each having a value of either 'yes' or 'no' based on"
        #                         + "the comparison without any explanation.Give me response"  # noqa: E501
        #                         + "in Json Format in {'vendorMatching': 'yes/no',"
        #                         + "'addressMatching': 'yes/no'} without any explanation",

        #                     ),
        #                 }
        #             ],
        #         }
        #     ]
        # }

        # Make the API call to Azure OpenAI
        access_token = get_open_ai_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        retry_count = 0
        max_retries = 5
        while retry_count < max_retries:
            response = requests.post(
                settings.open_ai_endpoint, headers=headers, json=data, timeout=60
            )

            # Check and process the response
            if response.status_code == 200:
                result = response.json()
                for choice in result["choices"]:
                    content = choice["message"]["content"].strip()
                    logger.info(f"Content: {content}")
                break
            elif response.status_code == 429:  # Handle rate limiting
                retry_after = int(response.headers.get("Retry-After", 5))
                logger.info(f"Rate limit hit. Retrying after {retry_after} seconds...")
                time.sleep(retry_after)
            else:
                logger.info(f"Error: {response.status_code}, {response.text}")
                retry_count += 1
                wait_time = 2**retry_count + random.uniform(0, 1)  # noqa: S311
                logger.info(f"Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)

        if retry_count == max_retries:
            logger.error("Max retries reached. Exiting.")
            content = json.dumps(
                {
                    "vendorMatching": "Max retries reached",
                    "addressMatching": "Max retries reached",
                }
            )

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
    except Exception:
        logger.error(f"{traceback.format_exc()}")
        vndMth_ck = 0
        vndMth_address_ck = 0

    return vndMth_ck, vndMth_address_ck


def VndMatchFn_2(doc_VendorAddress, metaVendorAdd):
    vndMth_address_ck = 0  # Indicates if address matching was successful
    matched_id_vendor = None  # To store the matched idVendor if found

    try:
        # prompt to only match addresses and return idVendor
        data = {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                f"vendorAddresses = {metaVendorAdd}, documentAddress = {doc_VendorAddress}. "  # noqa: E501
                                + f"You are given vendor data from two sources: vendorAddresses from master data "  # noqa: E501
                                + f"and documentAddress from an openai model. Your task is to confirm "  # noqa: E501
                                + f"if any address from vendorAddresses matches the documentAddress based on location, "  # noqa: E501
                                + f"normalize the addresses by handling common abbreviations such as 'Road' and 'RD'. Normalize addresses and check for matches. "  # noqa: E501
                                + f"If a match is found, return strictly only the JSON response as {{'addressMatching': 'yes', 'idVendor': 'matched_idVendor'}}. "  # noqa: E501
                                + f"If no match is found, return the JSON response as {{'addressMatching': 'no', 'idVendor': None}}. "  # noqa: E501
                                + f"Give me response in JSON format as {{'addressMatching': 'yes/no', 'idVendor': 'matched_idVendor'}} with two keys: 'addressMatching' and 'idVendor', "  # noqa: E501
                                + f"each having a value of either 'yes' or 'no' based on the comparison without any explanation."  # noqa: E501
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
        retry_count = 0
        max_retries = 5
        while retry_count < max_retries:
            response = requests.post(
                settings.open_ai_endpoint, headers=headers, json=data, timeout=60
            )

            # Check and process the response
            if response.status_code == 200:
                result = response.json()
                for choice in result["choices"]:
                    content = choice["message"]["content"].strip()
                    logger.info(f"Content: {content}")
                break
            elif response.status_code == 429:  # Handle rate limiting
                retry_after = int(response.headers.get("Retry-After", 5))
                logger.info(f"Rate limit hit. Retrying after {retry_after} seconds...")
                time.sleep(retry_after)
            else:
                logger.info(f"Error: {response.status_code}, {response.text}")
                retry_count += 1
                wait_time = 2**retry_count + random.uniform(0, 1)  # noqa: S311
                logger.info(f"Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)

        if retry_count == max_retries:
            logger.error("Max retries reached. Exiting.")
            content = json.dumps(
                {
                    "addressMatching": "Max retries reached",
                    "idVendor": "Max retries reached",
                }
            )

        # Process JSON response
        cl_mtch = (
            content.replace("json", "")
            .replace("\n", "")
            .replace("'''", "")
            .replace("```", "")
        )
        try:
            vndMth = json.loads(cl_mtch)
            if vndMth.get("addressMatching") == "yes":
                vndMth_address_ck = 1
                matched_id_vendor = vndMth.get("idVendor")
        except BaseException:
            try:
                cl_data_corrected = cl_mtch.replace("'", '"')
                vndMth = json.loads(cl_data_corrected)
                if vndMth.get("addressMatching") == "yes":
                    vndMth_address_ck = 1
                    matched_id_vendor = vndMth.get("idVendor")
            except BaseException:
                vndMth = cl_mtch
                if '"addressMatching": "yes"' in content:
                    vndMth_address_ck = 1
                    matched_id_vendor = (
                        vndMth.get("idVendor") if "idVendor" in vndMth else None
                    )

    except Exception:
        logger.error(f"{traceback.format_exc()}")
        vndMth_address_ck = 0
        matched_id_vendor = None

    return vndMth_address_ck, matched_id_vendor


def is_valid_date(date_string):
    try:
        parser.parse(date_string)
        return True
    except (ValueError, OverflowError):
        return False

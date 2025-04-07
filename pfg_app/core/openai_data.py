import base64
import json
import random
import re
import time
import traceback
from io import BytesIO
import pytesseract
from PIL import Image
import cv2
import numpy as np
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

def correct_orientation(image):
    """Check and correct image orientation to 0 degrees if needed."""
    # Convert PIL image to OpenCV format
    open_cv_image = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
    
    # OCR to detect text orientation
    osd = pytesseract.image_to_osd(open_cv_image)
    
    # Extract rotation angle
    rotation_angle = int(osd.split("\n")[2].split(":")[-1].strip())

    if rotation_angle == 0:
        logger.info(f"Image is already at 0 degrees. No rotation needed.")
        return image, rotation_angle  # No rotation needed

    # Rotate image to 0 degrees based on detected angle
    if rotation_angle == 90:
        image = image.rotate(-90, expand=True)  # Counterclockwise
    elif rotation_angle == 180:
        image = image.rotate(-180, expand=True)  # Upside down
    elif rotation_angle == 270:
        image = image.rotate(-270, expand=True)  # Clockwise

    return image, rotation_angle

def extract_invoice_details_using_openai(blob_data):
    try:
        logger.info(f"OpenAI Extracting invoice details started")
        prompt = """
                The provided image contains invoice ID, vendor name, vendor address and other details. Extract the relevant information and format it as a list of JSON objects, adhering strictly to the structure provided below:

                {
                    "NumberOfPages": "Total number of pages in the document",
                    "CreditNote": "Yes/No",
                    "VendorName": "Extracted vendor name",
                    "VendorAddress": "Extracted vendor address",
                    "InvoiceID": "Extracted invoice ID",
                    "InvoiceDate": "Extracted invoice date",
                    "SubTotal": "Extracted subtotal",
                    "invoicetotal": "Extracted invoice total",
                    "GST": "Extracted GST or Goods and Services Tax or Tax",
                    "PST": "Extracted PST",
                    "PST-SK": "Extracted PST-SK",
                    "PST-BC": "Extracted PST-BC",
                    "Bottle Deposit": "Extracted Bottle Deposit",
                    "Shipping Charges": "Extracted shipping charges",
                    "Litter Deposit": "Extracted Litter Deposit",
                    "Ecology Fee": "Extracted Ecology or Ecology Fee",
                    "Fuel Surcharge": "Extracted Fuel Surcharge",
                    "Freight Charges": "Extracted Fright Charges",
                    "misc": "Extracted miscellaneous charges",
                    "Currency": "Extracted currency
                }

                ### Instructions:
                1. **Orientation Correction**: Check if the invoice orientation is portrait or landscape. If its landscape, rotate it to portrait to extract stamp data correctly.
                2. **Data Extraction**: Extract only the information specified:
                - **Invoice Document**: Yes/No
                - **CreditNote**: Yes/No
                - **Invoice ID**: Extracted Invoice ID from invoice document (excluding 'Sold To', 'Ship To', or 'Bill To' sections)
                - **Vendor Name**:  Extracted vendor name from invoice document (excluding 'Sold To', 'Ship To', or 'Bill To' sections).
                                    Ensure to capture the primary vendor name typically found at the top of the document. 
                                    Return "N/A" if the vendor name is not present in the invoice document.
                - **Vendor Address**: Extracted vendor address from invoice document.
                                    Ensure to capture the primary vendor address typically found in the invoice document (including 'remit payment to' if present).
                                    Return "N/A" if the vendor address is not present in the invoice document.
                - **InvoiceDate**: Extract the invoice date only from the invoice document and exclude time if present. for example, '01/27/2025 15:53:19' then extract '01/27/2025'.
                - **Currency**: Identified by currency symbols (e.g., CAD, USD). If the currency is not explicitly identified as USD, default to CAD.
                - **GST**: Extracted 'GST' or 'Goods and Services Tax' or 'Tax' from invoice document if present else return "N/A", For example- 'Tax $2.23' or 'GST 2.23' or 'Goods and Services Tax' 2.23 then extract 2.23.
                - **PST**: Extracted PST from invoice document if present else return "N/A".
                - **PST-SK**: Extracted PST-SK from invoice document if present else return "N/A".
                - **PST-BC**: Extracted PST-BC from invoice document if present else return "N/A".
                - **Bottle Deposit**: Extracted bottle deposit from invoice document if present else return "N/A".
                - **Shipping Charges**: Extracted shipping charges from invoice document if present else return "N/A".
                - **Fuel Surcharge**: Extracted Fuel Surcharge from invoice document if present else return "N/A".
                - **Freight Charges**: Extracted Freight charges from invoice document if present else return "N/A".
                - **Litter Deposit**: Extracted litter deposit from invoice document if present else return "N/A".
                - **Ecology Fee**: Extracted Ecology Fee from invoice document if present else return "N/A".
                - **misc**: Extracted miscellaneous charges from invoice document if present else return "N/A".
                - **Invoice Date**: Extracted invoice date from invoice document.
                3. **Special Notes**:
                    - **Vendor Name:** : Don't consider the vendor name from 'Sold To' or 'Ship To' or 'Bill To' section.
                        - Ensure to capture the primary vendor name typically found at the top of the document (Excluding 'Pattison Food Group').
                        - Sometime vendor name may be name of person, so ensure to capture name of person with prefix 'Name:', for example 'Barry Smith', then return 'Barry Smith'.
                        - If the vendor name is not present at the top of the invoice document,then check if its present at the bottom with prefix 'please remit payment to:' or 'pay to:'
                        - Return "N/A" if the vendor name is not present in the invoice document.
                    - **Currency**: Must be three character only as 'CAD' or 'USD'. If it's unclear kept it as 'CAD' as default.
                    - **Vendor Addreess:** : Don't consider the vendor address from 'Sold To' or 'Ship To' or 'Bill To' section
                        - Ensure to capture the primary vendor address typically found in the top of the invoice document.
                        - If the vendor address is  not present at the top of the invoice document,then check if its present at the bottom with prefix 'please remit payment to:' or 'pay to:'.
                        - if the vendor address is not present in the invoice document, return "N/A".
                    - **CreditNote** : if "Credit Memo" or Credit Note" is present in the invoice document, then return "Yes".
                        - if the invoice total fields is in negative or in braces, then return "Yes".for example, '-123.45' or '123.45-' or (123.45) return "Yes".
                    - Ensure that the amounts(Subtotal,invoicetotal,GST,PST and other charges) to be extracted from last page only if  multiple amounts details are present in line items of all the pages. 
                4. **Output Format**: Ensure that the JSON output is precise and clean, without any extra text or commentary like ```json```,  it will be processed using json.loads.

                ### Example Output:
                If the extracted text includes:
                - VendorName: "ABC Company"
                - VendorAddress: "123 Main St, Anytown CANADA"
                - InvoiceID: "INV-12345"
                - InvoiceDate: "May 1, 2023"
                - SubTotal: "123.45"
                - invoicetotal: "123.45"
                - GST: "0.5"
                - PST: "2.23"
                - PST-SK: "N/A"
                - PST-BC: "N/A"
                - Bottle Deposit: "N/A"
                - Shipping Charges: "N/A"
                - Litter Deposit: "N/A"
                - Ecology Fee: "N/A"
                - Fuel Surcharge: "N/A"
                - Freight Charges: "N/A"
                - misc: "N/A"
                - Currency: "CAD"

                The expected output should be:
                {
                    "NumberOfPages": "3"
                    "CreditNote": "No",
                    "VendorName": "ABC Company",
                    "VendorAddress": "123 Main St, Anytown USA",
                    "InvoiceID": "INV-12345",
                    "InvoiceDate": "May 1, 2023",
                    "SubTotal": "123.45",
                    "invoicetotal": "123.45",
                    "GST": "0.5",
                    "PST": "2.23",
                    "PST-SK": "N/A",
                    "PST-BC": "N/A",
                    "Bottle Deposit": "N/A",
                    "Shipping Charges": "N/A",
                    "Litter Deposit": "N/A",
                    "Ecology Fee": "N/A",
                    "Fuel Surcharge": "N/A",
                    "Freight": "N/A",
                    "misc": "N/A",
                    "Currency": "CAD"
                }

                """
        # Set Tesseract OCR path (Windows users only, update path accordingly)
        # pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
        
        # # Set Tesseract OCR path (Linux users only, update path accordingly)
        pytesseract.pytesseract.tesseract_cmd = "/usr/bin/tesseract"
        image_content = []
        # Convert PDF to image
        pdf_img = convert_from_bytes(blob_data)
        # pdf_img = convert_from_bytes(
        #     blob_data, poppler_path=r"C:\\poppler-24.07.0\\Library\\bin"
        # )

        # Get total number of pages
        total_pages = len(pdf_img)
        print("Total pages:", total_pages)
        
        # Get file size in bytes, KB, or MB
        file_size_bytes = len(blob_data)
        file_size_kb = file_size_bytes / 1024
        file_size_mb = file_size_kb / 1024
        
        # Check if total pages are more than 30
        if total_pages > 5:
            # Append only the first page
            pages_to_process = [pdf_img[0]]
        else:
            # Process all pages
            pages_to_process = pdf_img

        for i, page in enumerate(pages_to_process, start=1):
            # Correct orientation if necessary
            corrected_page, angle = correct_orientation(page)
            buffered = BytesIO()
            corrected_page.save(buffered, format="PNG")
            # Encode image to base64
            encoded_image = base64.b64encode(buffered.getvalue()).decode("ascii")
            
            logger.info(f"Page {i}: Rotated {angle}° -> Corrected to 0°" if angle != 0 else f"Page {i}: Already 0°")
            # Append image to image_content
            image_content.append(
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{encoded_image}"},
                }
            )
        # Construct messages with both the text prompt and the encoded image
        data = {"messages" : [
            {
                "role": "system",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            },{
                "role": "user",
                "content": [
                        *image_content
                        
                ]
            }
        ],
        "temperature": 0.1
        }
        
        # Make the API call to Azure OpenAI
        access_token = get_open_ai_token()

        headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                }
        retry_count = 0
        max_retries = 50
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
                # Parse response immediately and exit retry loop
                cl_data = (
                    content.replace("json", "")
                    .replace("\n", "")
                    .replace("'''", "")
                    .replace("```", "")
                )

                try:
                    cleaned_json = json.loads(cl_data)
                except BaseException:
                    try:
                        cleaned_json = json.loads(cl_data.replace("'", '"'))
                    except BaseException:
                        cleaned_json = data
                status = "OpenAI Details Extracted"
                return cleaned_json, total_pages, file_size_mb, status
                # break
            elif response.status_code == 429:  # Handle rate limiting
                logger.info(f"Error: {response.status_code}, {response.text}")
                # retry_after = int(response.headers.get("Retry-After", 5))
                logger.info(f"Rate limit hit. Retrying after {10} seconds...")
                time.sleep(10)
            else:
                logger.info(f"Error: {response.status_code}, {response.text}")
                retry_count += 1
                # wait_time = 2**retry_count + random.uniform(0, 1)  # noqa: S311
                # logger.info(f"Retrying in {wait_time:.2f} seconds...")
                logger.info(f"Retrying in 10 seconds...")
                time.sleep(10)
        
        # If max retries are reached, return failure response
        logger.error("Max retries reached. Exiting.")
        cleaned_json = {
            "NumberOfPages": "",
            "CreditNote": "",
            "VendorName": "",
            "VendorAddress": "",
            "InvoiceID": "",
            "InvoiceDate": "",
            "SubTotal": "",
            "invoicetotal": "",
            "GST": "",
            "PST": "",
            "PST-SK": "",
            "PST-BC": "",
            "Bottle Deposit": "",
            "Shipping Charges": "",
            "Litter Deposit": "",
            "Ecology Fee": "",
            "Fuel Surcharge": "",
            "Freight": "",
            "misc": "",
            "Currency": "CAD"
        }
        status = "OpenAI - Max retries reached"
        return cleaned_json, total_pages, file_size_mb, status

    except Exception:
        logger.info(traceback.format_exc())
        cleaned_json = {
            "NumberOfPages": "",
            "CreditNote": "",
            "VendorName": "",
            "VendorAddress": "",
            "InvoiceID": "",
            "InvoiceDate": "",
            "SubTotal": "",
            "invoicetotal": "",
            "GST": "",
            "PST": "",
            "PST-SK": "",
            "PST-BC": "",
            "Bottle Deposit": "",
            "Shipping Charges": "",
            "Litter Deposit": "",
            "Ecology Fee": "",
            "Fuel Surcharge": "",
            "Freight": "",
            "misc": "",
            "Currency": "CAD"
        }
    status = "OpenAI - Response not found"
    return cleaned_json, total_pages, file_size_mb, status

def extract_approver_details_using_openai(msg):
    try:
        logger.info(f"OpenAI Extracting approver details started")
        status = None
        max_length = 30000
        content = msg.get_body(preferencelist=('html', 'plain')).get_content()
        
        # Initialize email_content to an empty string or the full content by default
        email_content = content
        
        if max_length and len(content) > max_length:
            email_content = content[:max_length]
        prompt = """ 
            Below is an example of an email chain having the following structure just for reference:  

            From: Sender Name <sender@email.com>
            Sent: Date 
            To: Recipient Name <recipient@email.com>
            Subject: Subject of the email


            approved  

            Approver Name
            Approver Designation



            Using the above example email chain, extract details only from the attached email content and ensure that no values from the reference example are used.  

            ### Extraction Criteria:  
            - Extract the email address of the sender from the last email sent. Otherwise, return "NA"..  
            - Extract the sent date of the last email and convert it to YYYY-MM-DD format. Otherwise, return "NA".  
            - Extract the recipient’s email address from the "To" field of the last email sent. Otherwise, return "NA".  
            - Extract the approver name only if explicitly mentioned below the "approved" phrase. Otherwise, return "NA".  
            - Extract the approver's designation only if explicitly stated. Otherwise, return "NA".  
            - Identify if the keyword **"approved"** exists in the approver's email. If it does, set **"Approved keyword"** to `"Approved"`. If a negative phrase such as `"not approved"`, `"cannot be approved"`, or similar is found, set **"Approved keyword"** to `"Not Approved"` and **"Approved keyword exists"** to `"No"`. If neither is present, return `"No"`.  

            ### Output Format:  
            If the required details are found in the extracted email, return them in the JSON structure below. If any field is missing, default to `"NA"` instead of using any values from the reference example.  
            {  
                "from": "Sender email address or NA",  
                "sent": "Sent date in YYYY-MM-DD or NA",  
                "to": "Recipient email address or NA",  
                "Approver": "Approver name or NA",  
                "Designation": "Approver designation or NA",  
                "Approved keyword": "Approved" or "Not Approved" or "NA",  
                "Approved keyword exists": "Yes" or "No"  
            }  
        """
        # Construct messages with both the text prompt and the email content
        data = {
            "messages": [
                {
                    "role": "system",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": email_content
                        }
                    ]
                }
            ],
            "temperature": 0.1
        }
        # Make the API call to Azure OpenAI
        access_token = get_open_ai_token()

        headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                }
        
        retry_count = 0
        max_retries = 20
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
                status = "Approval detail extracted"
                break
            elif response.status_code == 429:  # Handle rate limiting
                logger.info(f"Error in Corp OpenAI: {response.status_code}, {response.text}")
                # retry_after = int(response.headers.get("Retry-After", 5))
                logger.info(f"Rate limit hit. Retrying after {10} seconds...")
                time.sleep(10)
            else:
                logger.info(f"Error in Corp OpenAI: {response.status_code}, {response.text}")
                retry_count += 1
                # wait_time = 2**retry_count + random.uniform(0, 1)  # noqa: S311
                # logger.info(f"Retrying in {wait_time:.2f} seconds...")
                logger.info(f"Retrying in 10 seconds...")
                time.sleep(10)
        
        if retry_count == max_retries:
            logger.error("Max retries reached. Exiting.")
            status = "OpenAI - max retry reached"
            content = json.dumps(
                {
                "from": "",
                "sent": "",
                "to": "",
                "Approver": "",
                "Designation": "",
                "Approved keyword": "",
                "Approved keyword exists": ""
            }
            )
        cl_data = (
                content.replace("json", "")
                .replace("\n", "")
                .replace("'''", "")
                .replace("```", "")
            )

        try:
            # Parse the JSON
            cleaned_json = json.loads(cl_data)
        except BaseException:
            try:
                cl_data_corrected = cl_data.replace("'", '"')
                cleaned_json = json.loads(cl_data_corrected)
            except BaseException:
                cleaned_json = data

    except Exception:
        logger.info(traceback.format_exc())
        status = "OpenAI - Response not found"
        cleaned_json = {
                "from": "",
                "sent": "",
                "to": "",
                "Approver": "",
                "Designation": "",
                "Approved keyword": "",
                "Approved keyword exists": ""
            }
    return cleaned_json, status


# def extract_invoice_details_using_openai(blob_data):
#     try:
#         logger.info(f"OpenAI Extracting invoice details started")
#         prompt = """
#                 The provided image contains invoice ID, vendor name, vendor address and other details. Extract the relevant information and format it as a list of JSON objects, adhering strictly to the structure provided below:

#                 {
#                     "NumberOfPages": "Total number of pages in the document",
#                     "CreditNote": "Yes/No",
#                     "VendorName": "Extracted vendor name",
#                     "VendorAddress": "Extracted vendor address",
#                     "InvoiceID": "Extracted invoice ID",
#                     "InvoiceDate": "Extracted invoice date",
#                     "SubTotal": "Extracted subtotal",
#                     "invoicetotal": "Extracted invoice total",
#                     "GST": "Extracted GST or Goods and Services Tax or Tax",
#                     "PST": "Extracted PST",
#                     "PST-SK": "Extracted PST-SK",
#                     "PST-BC": "Extracted PST-BC",
#                     "Bottle Deposit": "Extracted Bottle Deposit",
#                     "Shipping Charges": "Extracted shipping charges",
#                     "Litter Deposit": "Extracted Litter Deposit",
#                     "Ecology Fee": "Extracted Ecology or Ecology Fee",
#                     "Fuel Surcharge": "Extracted Fuel Surcharge",
#                     "Freight Charges": "Extracted Fright Charges",
#                     "misc": "Extracted miscellaneous charges",
#                     "Currency": "Extracted currency
#                 }

#                 ### Instructions:
#                 1. **Orientation Correction**: Check if the invoice orientation is portrait or landscape. If its landscape, rotate it to portrait to extract stamp data correctly.
#                 2. **Data Extraction**: Extract only the information specified:
#                 - **Invoice Document**: Yes/No
#                 - **CreditNote**: Yes/No
#                 - **Invoice ID**: Extracted Invoice ID from invoice document (excluding 'Sold To', 'Ship To', or 'Bill To' sections)
#                 - **Vendor Name**:  Extracted vendor name from invoice document (excluding 'Sold To', 'Ship To', or 'Bill To' sections).
#                                     Ensure to capture the primary vendor name typically found at the top of the document. 
#                                     Return "N/A" if the vendor name is not present in the invoice document.
#                 - **Vendor Address**: Extracted vendor address from invoice document.
#                                     Ensure to capture the primary vendor address typically found in the invoice document (including 'remit payment to' if present).
#                                     Return "N/A" if the vendor address is not present in the invoice document.
#                 - **Currency**: Identified by currency symbols (e.g., CAD, USD). If the currency is not explicitly identified as USD, default to CAD.
#                 - **GST**: Extracted 'GST' or 'Goods and Services Tax' or 'Tax' from invoice document if present else return "N/A", For example- 'Tax $2.23' or 'GST 2.23' or 'Goods and Services Tax' 2.23 then extract 2.23.
#                 - **PST**: Extracted PST from invoice document if present else return "N/A".
#                 - **PST-SK**: Extracted PST-SK from invoice document if present else return "N/A".
#                 - **PST-BC**: Extracted PST-BC from invoice document if present else return "N/A".
#                 - **Bottle Deposit**: Extracted bottle deposit from invoice document if present else return "N/A".
#                 - **Shipping Charges**: Extracted shipping charges from invoice document if present else return "N/A".
#                 - **Fuel Surcharge**: Extracted Fuel Surcharge from invoice document if present else return "N/A".
#                 - **Freight Charges**: Extracted Freight charges from invoice document if present else return "N/A".
#                 - **Litter Deposit**: Extracted litter deposit from invoice document if present else return "N/A".
#                 - **Ecology Fee**: Extracted Ecology Fee from invoice document if present else return "N/A".
#                 - **misc**: Extracted miscellaneous charges from invoice document if present else return "N/A".
#                 - **Invoice Date**: Extracted invoice date from invoice document.
#                 3. **Special Notes**:
#                     - **Vendor Name:** : Don't consider the vendor name from 'Sold To' or 'Ship To' or 'Bill To' section.
#                         - Ensure to capture the primary vendor name typically found at the top of the document (Excluding 'Pattison Food Group').
#                         - Sometime vendor name may be name of person, so ensure to capture name of person with prefix 'Name:', for example 'Barry Smith', then return 'Barry Smith'.
#                         - If the vendor name is not present at the top of the invoice document,then check if its present at the bottom with prefix 'please remit payment to:' or 'pay to:'
#                         - Return "N/A" if the vendor name is not present in the invoice document.
#                     - **Currency**: Must be three character only as 'CAD' or 'USD'. If it's unclear kept it as 'CAD' as default.
#                     - **Vendor Addreess:** : Don't consider the vendor address from 'Sold To' or 'Ship To' or 'Bill To' section
#                         - Ensure to capture the primary vendor address typically found in the top of the invoice document.
#                         - If the vendor address is  not present at the top of the invoice document,then check if its present at the bottom with prefix 'please remit payment to:' or 'pay to:'.
#                         - if the vendor address is not present in the invoice document, return "N/A".
#                     - **CreditNote** : if "Credit Memo" or Credit Note" is present in the invoice document, then return "Yes".
#                         - if any of the amount fields are in negative, then return "Yes".for example, '-123.45' or '123.45-' return "Yes".
#                         - Ensure that if it's CreditNote than amounts(Subtotal, invoicetotal, GST, PST, PST-SK, PST-BC, Bottle Deposit, Shipping Charges, Litter Deposit, misc) are in negative.
#                     - Ensure that the amounts(Subtotal,invoicetotal,GST,PST and other charges) to be extracted from last page only if  multiple amounts details are present in line items of all the pages. 
#                 4. **Output Format**: Ensure that the JSON output is precise and clean, without any extra text or commentary like ```json```,  it will be processed using json.loads.

#                 ### Example Output:
#                 If the extracted text includes:
#                 - VendorName: "ABC Company"
#                 - VendorAddress: "123 Main St, Anytown CANADA"
#                 - InvoiceID: "INV-12345"
#                 - InvoiceDate: "May 1, 2023"
#                 - SubTotal: "123.45"
#                 - invoicetotal: "123.45"
#                 - GST: "0.5"
#                 - PST: "2.23"
#                 - Bottle Deposit: "N/A"
#                 - Shipping Charges: "N/A"
#                 - Ecology Fee: "N/A"
#                 - Fuel Surcharge: "N/A"
#                 - Freight Charges: "N/A"
#                 - Litter Deposit: "N/A"
#                 - Currency: "CAD"

#                 The expected output should be:
#                 {
#                     "NumberOfPages": "3"
#                     "CreditNote": "No",
#                     "VendorName": "ABC Company",
#                     "VendorAddress": "123 Main St, Anytown USA",
#                     "InvoiceID": "INV-12345",
#                     "InvoiceDate": "May 1, 2023",
#                     "SubTotal": "123.45",
#                     "invoicetotal": "123.45",
#                     "GST": "0.5",
#                     "PST": "2.23",
#                     "PST-SK": "N/A",
#                     "PST-BC": "N/A",
#                     "Bottle Deposit": "N/A",
#                     "Shipping Charges": "N/A",
#                     "Ecology Fee": "N/A",
#                     "Fuel Surcharge": "N/A",
#                     "Freight Charges": "N/A",
#                     "Litter Deposit": "N/A",
#                     "Currency": "CAD"
#                 }

#                 """
#         image_content = []
#         # Convert PDF to image
#         pdf_img = convert_from_bytes(blob_data)
#         # pdf_img = convert_from_bytes(
#         #     blob_data, poppler_path=r"C:\\poppler-24.07.0\\Library\\bin"
#         # )

#         # Get total number of pages
#         total_pages = len(pdf_img)
#         print("Total pages:", total_pages)
        
#         # Get file size in bytes, KB, or MB
#         file_size_bytes = len(blob_data)
#         file_size_kb = file_size_bytes / 1024
#         file_size_mb = file_size_kb / 1024
        
#         # Check if total pages are more than 30
#         if total_pages > 5:
#             # Append only the first page
#             pages_to_process = [pdf_img[0]]
#         else:
#             # Process all pages
#             pages_to_process = pdf_img

#         for i, page in enumerate(pages_to_process, start=1):
#             buffered = BytesIO()
#             page.save(buffered, format="PNG")
#             # Encode image to base64
#             encoded_image = base64.b64encode(buffered.getvalue()).decode("ascii")
#             # Append image to image_content
#             image_content.append(
#                 {
#                     "type": "image_url",
#                     "image_url": {"url": f"data:image/png;base64,{encoded_image}"},
#                 }
#             )
#         # Construct messages with both the text prompt and the encoded image
#         data = {"messages" : [
#             {
#                 "role": "system",
#                 "content": [
#                     {
#                         "type": "text",
#                         "text": prompt
#                     }
#                 ]
#             },{
#                 "role": "user",
#                 "content": [
#                         *image_content
                        
#                 ]
#             }
#         ],
#         "temperature": 0.1
#         }
        
#         # Make the API call to Azure OpenAI
#         access_token = get_open_ai_token()

#         headers = {
#                     "Authorization": f"Bearer {access_token}",
#                     "Content-Type": "application/json",
#                 }
#         retry_count = 0
#         max_retries = 50
#         while retry_count < max_retries:
#             response = requests.post(
#                 settings.open_ai_endpoint, headers=headers, json=data, timeout=60
#             )
#             # Check and process the response
#             if response.status_code == 200:
#                 result = response.json()
#                 for choice in result["choices"]:
#                     content = choice["message"]["content"].strip()
#                     logger.info(f"Content: {content}")
#                 # Parse response immediately and exit retry loop
#                 cl_data = (
#                     content.replace("json", "")
#                     .replace("\n", "")
#                     .replace("'''", "")
#                     .replace("```", "")
#                 )

#                 try:
#                     cleaned_json = json.loads(cl_data)
#                 except BaseException:
#                     try:
#                         cleaned_json = json.loads(cl_data.replace("'", '"'))
#                     except BaseException:
#                         cleaned_json = data

#                 return cleaned_json, total_pages, file_size_mb 
#                 # break
#             elif response.status_code == 429:  # Handle rate limiting
#                 logger.info(f"Error: {response.status_code}, {response.text}")
#                 # retry_after = int(response.headers.get("Retry-After", 5))
#                 logger.info(f"Rate limit hit. Retrying after {10} seconds...")
#                 time.sleep(10)
#             else:
#                 logger.info(f"Error: {response.status_code}, {response.text}")
#                 retry_count += 1
#                 # wait_time = 2**retry_count + random.uniform(0, 1)  # noqa: S311
#                 # logger.info(f"Retrying in {wait_time:.2f} seconds...")
#                 logger.info(f"Retrying in 10 seconds...")
#                 time.sleep(10)
        
#         # If max retries are reached, return failure response
#         logger.error("Max retries reached. Exiting.")
#         cleaned_json = {
#             "NumberOfPages": "Max retries reached",
#             "CreditNote": "Max retries reached",
#             "VendorName": "Max retries reached",
#             "VendorAddress": "Max retries reached",
#             "InvoiceID": "Max retries reached",
#             "InvoiceDate": "Max retries reached",
#             "SubTotal": "Max retries reached",
#             "invoicetotal": "Max retries reached",
#             "GST": "Max retries reached",
#             "PST": "Max retries reached",
#             "PST-SK": "Max retries reached",
#             "PST-BC": "Max retries reached",
#             "Bottle Deposit": "Max retries reached",
#             "Shipping Charges": "Max retries reached",
#             "Ecology Fee": "Max retries reached",
#             "Fuel Surcharge": "Max retries reached",
#             "Freight": "Max retries reached",
#             "Litter Deposit": "Max retries reached",
#             "Currency": "CAD"
#         }
    
#         return cleaned_json, total_pages, file_size_mb

#     except Exception:
#         logger.info(traceback.format_exc())
#         cleaned_json = {
#             "NumberOfPages": "Response not found",
#             "CreditNote": "NA",
#             "VendorName": "Response not found",
#             "VendorAddress": "Response not found",
#             "InvoiceID": "Response not found",
#             "InvoiceDate": "Response not found",
#             "SubTotal": "Response not found",
#             "invoicetotal": "Response not found",
#             "GST": "Response not found",
#             "PST": "Response not found",
#             "PST-SK": "Response not found",
#             "PST-BC": "Response not found",
#             "Bottle Deposit": "Response not found",
#             "Shipping Charges": "Response not found",
#             "Ecology Fee": "Response not found",
#             "Fuel Surcharge": "Response not found",
#             "Freight": "Response not found",
#             "Litter Deposit": "Response not found",
#             "Currency": "CAD"
#         }
#     return cleaned_json, total_pages, file_size_mb
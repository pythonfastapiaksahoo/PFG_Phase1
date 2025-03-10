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


def extract_invoice_details_using_openai(blob_data):
    try:
        
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
                        - if any of the amount fields are in negative, then return "Yes".for example, '-123.45' or '123.45-' return "Yes".
                        - Ensure that if it's CreditNote than amounts(Subtotal, invoicetotal, GST, PST, PST-SK, PST-BC, Bottle Deposit, Shipping Charges, Litter Deposit, misc) are in negative.
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
                - Bottle Deposit: "N/A"
                - Shipping Charges: "N/A"
                - Ecology Fee: "N/A"
                - Fuel Surcharge: "N/A"
                - Freight Charges: "N/A"
                - Litter Deposit: "N/A"
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
                    "Ecology Fee": "N/A",
                    "Fuel Surcharge": "N/A",
                    "Freight Charges": "N/A",
                    "Litter Deposit": "N/A",
                    "Currency": "CAD"
                }

                """
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
            buffered = BytesIO()
            page.save(buffered, format="PNG")
            # Encode image to base64
            encoded_image = base64.b64encode(buffered.getvalue()).decode("ascii")
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

                return cleaned_json, total_pages, file_size_mb 
                # break
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
        
        # If max retries are reached, return failure response
        logger.error("Max retries reached. Exiting.")
        cleaned_json = {
            "NumberOfPages": "Max retries reached",
            "CreditNote": "Max retries reached",
            "VendorName": "Max retries reached",
            "VendorAddress": "Max retries reached",
            "InvoiceID": "Max retries reached",
            "InvoiceDate": "Max retries reached",
            "SubTotal": "Max retries reached",
            "invoicetotal": "Max retries reached",
            "GST": "Max retries reached",
            "PST": "Max retries reached",
            "PST-SK": "Max retries reached",
            "PST-BC": "Max retries reached",
            "Bottle Deposit": "Max retries reached",
            "Shipping Charges": "Max retries reached",
            "Ecology Fee": "Max retries reached",
            "Fuel Surcharge": "Max retries reached",
            "Freight": "Max retries reached",
            "Litter Deposit": "Max retries reached",
            "Currency": "CAD"
        }
    
        return cleaned_json, total_pages, file_size_mb

    except Exception:
        logger.info(traceback.format_exc())
        cleaned_json = {
            "NumberOfPages": "Response not found",
            "CreditNote": "NA",
            "VendorName": "Response not found",
            "VendorAddress": "Response not found",
            "InvoiceID": "Response not found",
            "InvoiceDate": "Response not found",
            "SubTotal": "Response not found",
            "invoicetotal": "Response not found",
            "GST": "Response not found",
            "PST": "Response not found",
            "PST-SK": "Response not found",
            "PST-BC": "Response not found",
            "Bottle Deposit": "Response not found",
            "Shipping Charges": "Response not found",
            "Ecology Fee": "Response not found",
            "Fuel Surcharge": "Response not found",
            "Freight": "Response not found",
            "Litter Deposit": "Response not found",
            "Currency": "CAD"
        }
    return cleaned_json, total_pages, file_size_mb

def extract_approver_details_using_openai(msg):
    try:
        
        max_length = 80000
        content = msg.get_body(preferencelist=('html', 'plain')).get_content()
        
        # Initialize email_content to an empty string or the full content by default
        email_content = content
        
        if max_length and len(content) > max_length:
            email_content = content[:max_length]
        prompt = """
            Email chain:
            approved


            Kathy March
            Senior Finance Manager
            E. Kathy_march@pattisonfoodgroup.com

            From: Ryan Doak (Office Services Representative) <ryan_doak@pattisonfoodgroup.com>
            Sent: Thursday, November 21, 2024 9:44 AM
            To: Kathy March (Senior Manager, Finance) <Kathy_March@pattisonfoodgroup.com>
            Subject: Com Pro AR226369 $668.19

            Hi Kathy,

            Please review, add approval and forward to ap_auto_expense.

            Invoice#        AR226369        *must be same as attachment
                    GST:    29.83
            Grand Total:    668.19
            Approver Name:    Kathy March
            Approver TM ID:   350161
            Approval Title:   Senior Manager

            Store Dept  Account  SL  Project Activity Amount

            8000  0003  71999                         638.36

            Thanks,

            Ryan Doak
            Office Services Representative | Mailroom
            Phone: 604-882-7830


            The provided email chain contains the details of the approver before the 'From' clause:   
            - Approved or approved keyword  and just below that the Approval Details: Approver name, Designation, email

            Extract the relevant information from last email sent only and format it as a JSON objects, adhering strictly to the  sample structure provided below:

            {
                "from": "from email address",
                "sent": "sent date",
                "to": "to email address",
                "Approver": "Approver name",
                "Designation": "approver designation",
                "Approved keyword": "approved",
                "Approved keyword exists": "yes"
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
                "from": "Max retries reached",
                "sent": "Max retries reached",
                "to": "Max retries reached",
                "Approver": "Max retries reached",
                "Designation": "Max retries reached",
                "Approved keyword": "Max retries reached",
                "Approved keyword exists": "Max retries reached"
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
        cleaned_json = {
                "from": "Response not found",
                "sent": "Response not found",
                "to": "Response not found",
                "Approver": "Response not found",
                "Designation": "Response not found",
                "Approved keyword": "Response not found",
                "Approved keyword exists": "Response not found"
            }
    return cleaned_json
import email
import json
import base64
import os
import traceback
from uuid import uuid4
import imgkit
import re
from PIL import Image
from io import BytesIO
from bs4 import BeautifulSoup
import requests
from pfg_app import settings
from pfg_app import model
# from pfg_app.FROps.corp_postpro import postProInvoiceData
from pfg_app.core.openai_data import extract_invoice_details_using_openai
from pfg_app.core.utils import get_blob_securely, get_credential, upload_blob_securely
from pfg_app.crud.ERPIntegrationCrud import read_invoice_file_voucher
from pfg_app.logger_module import logger, set_operation_id
from sqlalchemy import String, case, func, or_, and_, desc, text, distinct
from sqlalchemy.orm import Load, load_only
from datetime import datetime, timedelta
from fastapi import Response
from azure.storage.blob import BlobServiceClient

from pfg_app.schemas.pfgtriggerSchema import InvoiceVoucherSchema
from pfg_app.session.session import get_db
# def parse_eml(file_path):
#     with open(file_path, 'rb') as file:
#         msg = BytesParser(policy=policy.default).parse(file)
#     return msg

# Function to extract table data and skip empty rows
def extract_table_data(table):
    rows = table.find_all("tr")
    table_data = []
    for row in rows:
        cells = row.find_all(["td", "th"])  # Include both header and data cells
        row_data = [cell.get_text(strip=True) for cell in cells]
        
        # Skip the row if all cells are empty or contain only whitespace
        if any(cell.strip() for cell in row_data):  # At least one cell has content
            table_data.append(row_data)
    return table_data


def extract_content_from_eml_file(email_msg):
    email_metadata = {
        "from": email_msg['From'],
        "sent": email_msg['date'],
        "to": email_msg['To']
    }
    # Get the HTML content from the email body
    html_content = email_msg.get_body(preferencelist=('html', 'plain')).get_content()
    # logger.info(html_content)
    soup = BeautifulSoup(html_content, 'html.parser')
        
    # Find all tables in the HTML
    tables = soup.find_all('table')

    # Extract data from all tables
    all_tables_data = [extract_table_data(table) for table in tables]

    # Combine metadata and table data
    output_data = {
        "email_metadata": email_metadata,
        "tables_data": all_tables_data
    }
    # Convert to JSON
    final_json = json.dumps(output_data, indent=4)
    # # print the JSON result
    # print(final_json) 
    return final_json
    


def format_data_for_template1(parsed_data):
    try:
        
        # Extract metadata
        email_metadata = parsed_data["email_metadata"]

        # Extract table data
        tables = parsed_data["tables_data"]

        # Initialize dictionaries
        invoice_data = {
            "store": [],
            "dept": [],
            "account": [],
            "SL": [],
            "project": [],
            "activity": [],
            "amount": [],
            "invoice#": '',
            "GST": '',
            "invoicetotal": '',
        }
        # approver_details = {}
        approver_details = {
            "approverName": '',
            "TMID": '',
            "title": ''
        }

        # Process the table to differentiate cases
        for table in tables:
            for i, row in enumerate(table):
                # Identify case where "Invoice#" is present in a row
                if "Invoice#" in row[0]:
                    # invoice_number = row[1]  # Extract invoice number
                    # invoice_data["invoice#"] = invoice_number
                    # invoice_data["invoice#"] = row[1] if len(row) > 1 else ""
                    invoice_data["invoice#"] = next((value for value in row[1:] if value.strip()), "")
                # Check if header matches expected columns without "Invoice" keyword
                elif row == ["Store", "Dept", "Account", "SL", "Project", "Activity", "Subtotal"]:
                    headers = row
                    for data_row in table[i + 1:]:
                        # Stop processing if GST or Grand Total rows are reached
                        if "GST:" in data_row[0] or "Grand Total:" in data_row[0]:
                            break

                        # Map each column of data to the correct header
                        invoice_data["store"].append(data_row[0] if len(data_row) > 0 else "")
                        invoice_data["dept"].append(data_row[1] if len(data_row) > 1 else "")
                        invoice_data["account"].append(data_row[2] if len(data_row) > 2 else "")
                        invoice_data["SL"].append(data_row[3] if len(data_row) > 3 else "")
                        invoice_data["project"].append(data_row[4] if len(data_row) > 4 else "")
                        invoice_data["activity"].append(data_row[5] if len(data_row) > 5 else "")
                        invoice_data["amount"].append(data_row[6] if len(data_row) > 6 else "")

                # Extract GST and Grand Total
                elif "GST:" in row[0]:
                    # invoice_data["GST"] = row[1] if len(row) > 1 else ""
                    invoice_data["GST"] = next((value for value in row[1:] if value.strip()), "")
                elif "Grand Total:" in row[0]:
                    # invoice_data["invoicetotal"] = row[1] if len(row) > 1 else "" 
                    invoice_data["invoicetotal"] = next((value for value in row[1:] if value.strip()), "")
                # Extract approver details
                elif "Approver Name:" in row[0]:
                    # approver_details["approverName"] = row[1] if len(row) > 1 else ""
                    approver_details["approverName"] = next((value for value in row[1:] if value.strip()), "")
                elif "Approver TM ID:" in row[0]:
                    # approver_details["TMID"] = row[1] if len(row) > 1 else ""
                    approver_details["TMID"] = next((value for value in row[1:] if value.strip()), "")
                elif "Approval Title:" in row[0]:
                    # approver_details["title"] = row[1] if len(row) > 1 else ""
                    approver_details["title"] = next((value for value in row[1:] if value.strip()), "")

        # Combine into final structured JSON
        structured_output = {
            "email_metadata": email_metadata,
            "invoiceDetails": invoice_data,
            "approverDetails": approver_details
        }

        # Convert to JSON and print
        final_json = json.dumps(structured_output, indent=4)
        return final_json

    except Exception:
        logger.info(f"Error while extracting coding details for template 1:{traceback.format_exc()}")
        # Combine into final structured JSON
        structured_output = {
                        "email_metadata": email_metadata,
                        "invoiceDetails": { 
                            "store": [''], 
                            "dept": [''], 
                            "account": [''], 
                            "SL": [''],
                            "project": [''],
                            "activity": [''], 
                            "amount": [''], 
                            "invoice#": '', 
                            "GST": '', 
                            "invoicetotal": '', 
                        }, 
                        "approverDetails": { 
                            "approverName": '', 
                            "TMID": '', 
                            "title": '',
                        }
                    }
        # Convert to JSON and print
        final_json = json.dumps(structured_output, indent=4)
        return final_json
    
    
# def format_data_for_template2(parsed_data):
#     try:
#         # Extract metadata
#         email_metadata = parsed_data["email_metadata"]

#         # Extract table data
#         tables = parsed_data["tables_data"]

#         # Initialize dictionaries
#         invoice_data = {
#             "invoice#": [],
#             "store": [],
#             "dept": [],
#             "account": [],
#             "SL": [],
#             "project": [],
#             "activity": [],
#             "GST": [],
#             "invoicetotal": [],
#             "subtotal": None
#         }
#         # approver_details = {}
#         approver_details = {
#             "approverName": '',
#             "TMID": '',
#             "title": ''
#         }
#         # Flatten the tables_data list if nested improperly
#         cleaned_tables = []
#         for table in tables:
#             if isinstance(table, list) and all(isinstance(row, list) for row in table):
#                 cleaned_tables.extend(table)  # Flatten nested lists
#             else:
#                 cleaned_tables.append(table)

#         # Process the table
#         for i, row in enumerate(cleaned_tables):
#             if isinstance(row, list) and len(row) > 1:
#                 if "Approver Name:" in row[0]:
#                     approver_details["approverName"] = row[1]
#                 elif "Approver TM ID:" in row[0]:
#                     approver_details["TMID"] = row[1]
#                 elif "Approval Title:" in row[0]:
#                     approver_details["title"] = row[1]
#                 elif "Invoice #" in row[0]:
#                     headers = row  # Identify header row
#                 elif len(row) >= 9:  # Ensure valid invoice row with enough columns
#                     invoice_data["invoice#"].append(row[0])
#                     invoice_data["store"].append(row[1])
#                     invoice_data["dept"].append(row[2])
#                     invoice_data["account"].append(row[3])
#                     invoice_data["SL"].append(row[4])
#                     invoice_data["project"].append(row[5])
#                     invoice_data["activity"].append(row[6])
#                     invoice_data["GST"].append(row[7])
#                     invoice_data["invoicetotal"].append(row[8])

#         # Combine into final structured JSON
#         structured_output = {
#             "email_metadata": email_metadata,
#             "invoiceDetails": invoice_data,
#             "approverDetails": approver_details
#         }

#         # Convert to JSON and print
#         final_json = json.dumps(structured_output, indent=4)
#         # print(final_json)
#         return final_json

#     except Exception:
#         logger.info(f"Error while extracting coding details for template 2:{traceback.format_exc()}")
#         # Combine into final structured JSON
#         structured_output = {
#                         "email_metadata": email_metadata, 
#                         "invoiceDetails": { 
#                             "store": [''], 
#                             "dept": [''], 
#                             "account": [''], 
#                             "SL": [''],
#                             "project": [''],
#                             "activity": [''], 
#                             "amount": [''], 
#                             "invoice#": '', 
#                             "GST": '', 
#                             "invoicetotal": '', 
#                         }, 
#                         "approverDetails": { 
#                             "approverName": '', 
#                             "TMID": '', 
#                             "title": '',
#                         }
#                     }
#         # Convert to JSON and print
#         final_json = json.dumps(structured_output, indent=4)
#         return final_json

def format_data_for_template2(parsed_data):
    try:
        # Extract metadata
        email_metadata = parsed_data["email_metadata"]

        # Extract table data
        tables = parsed_data["tables_data"]

        # Initialize dictionaries
        invoice_data = {
            "invoice#": [],
            "store": [],
            "dept": [],
            "account": [],
            "SL": [],
            "project": [],
            "activity": [],
            "GST": [],
            "invoicetotal": [],
            "subtotal": None
        }
        
        approver_details = {
            "approverName": '',
            "TMID": '',
            "title": ''
        }

        # Flatten the tables_data list if nested improperly
        cleaned_tables = []
        for table in tables:
            if isinstance(table, list) and all(isinstance(row, list) for row in table):
                cleaned_tables.extend(table)  # Flatten nested lists
            else:
                cleaned_tables.append(table)

        # Process the table
        for i, row in enumerate(cleaned_tables):
            if isinstance(row, list) and len(row) > 1:
                # Extract key-value pair
                key = row[0].strip()  # Key is the first element
                value = next((value for value in row[1:] if value.strip()), "")  # Get first non-empty value

                if "Approver Name" in key:
                    approver_details["approverName"] = value
                elif "Approver TM ID" in key:
                    approver_details["TMID"] = value
                elif "Approval Title" in key:
                    approver_details["title"] = value
                elif "Invoice #" in key:
                    headers = row  # Identify header row
                elif len(row) >= 9:  # Ensure valid invoice row with enough columns
                    invoice_data["invoice#"].append(row[0])
                    invoice_data["store"].append(row[1])
                    invoice_data["dept"].append(row[2])
                    invoice_data["account"].append(row[3])
                    invoice_data["SL"].append(row[4])
                    invoice_data["project"].append(row[5])
                    invoice_data["activity"].append(row[6])
                    invoice_data["GST"].append(row[7])
                    invoice_data["invoicetotal"].append(row[8])

        # Combine into final structured JSON
        structured_output = {
            "email_metadata": email_metadata,
            "invoiceDetails": invoice_data,
            "approverDetails": approver_details
        }

        # Convert to JSON and return
        final_json = json.dumps(structured_output, indent=4)
        return final_json

    except Exception:
        logger.info(f"Error while extracting coding details for template 2:{traceback.format_exc()}")
        # Combine into final structured JSON
        structured_output = {
            "email_metadata": email_metadata, 
            "invoiceDetails": { 
                "store": [''], 
                "dept": [''], 
                "account": [''], 
                "SL": [''],
                "project": [''],
                "activity": [''], 
                "amount": [''], 
                "invoice#": '', 
                "GST": '', 
                "invoicetotal": '', 
            }, 
            "approverDetails": { 
                "approverName": '', 
                "TMID": '', 
                "title": '',
            }
        }
        # Convert to JSON and return
        final_json = json.dumps(structured_output, indent=4)
        return final_json
    
def format_data_for_template3(parsed_data):
    try:
        # Extract metadata
        email_metadata = parsed_data["email_metadata"]

        # Extract table data
        tables = parsed_data["tables_data"]

        # Initialize dictionaries
        invoice_data = {
            "store": [],
            "dept": [],
            "account": [],
            "SL": [],
            "project": [],
            "activity": [],
            "amount": [],
            "invoice#": '',
            "GST": '',
            "invoicetotal": '',
        }
        # approver_details = {}
        approver_details = {
            "approverName": '',
            "TMID": '',
            "title": ''
        }
        # Process the table to differentiate cases
        for table in tables:
            for i, row in enumerate(table):
                # Extract invoice number (from first row in new format)
                if "Invoice #" in row[0]:
                    # invoice_data["invoice#"] = row[1] if len(row) > 1 else ""  # Default to empty if index doesn't exist
                    invoice_data["invoice#"] = next((value for value in row[1:] if value.strip()), "")
                # Extract GST and Grand Total
                elif "GST:" in row[0]:
                    # invoice_data["GST"] = row[1] if len(row) > 1 else ""  # Default to empty if index doesn't exist
                    invoice_data["GST"] = next((value for value in row[1:] if value.strip()), "")
                elif "Grand Total:" in row[0]:
                    invoice_data["invoicetotal"] =  next((value for value in row[1:] if value.strip()), "")
                    # invoice_data["invoicetotal"] = row[1] if len(row) > 1 else ""  # Default to empty if index doesn't exist


                # Extract approver details
                elif "Approver Name:" in row[0]:
                    # approver_details["approverName"] = row[1] if len(row) > 1 else ""
                    approver_details["approverName"] = next((value for value in row[1:] if value.strip()), "")
                elif "Approver TM ID:" in row[0]:
                    # approver_details["TMID"] = row[1] if len(row) > 1 else ""
                    approver_details["TMID"] = next((value for value in row[1:] if value.strip()), "")
                elif "Approval Title:" in row[0]:
                    # approver_details["title"] = row[1] if len(row) > 1 else ""
                    approver_details["title"] = next((value for value in row[1:] if value.strip()), "")

                # Check if header matches expected columns
                elif row == ["Store", "Dept", "Account", "SL", "Project", "Activity", "Amount"]:
                    headers = row
                    for data_row in table[i + 1:]:
                        # Stop processing if GST or Grand Total rows are reached
                        if "GST:" in data_row[0] or "Grand Total:" in data_row[0]:
                            break

                        # # Map each column of data to the correct header
                        # invoice_data["store"].append(data_row[0])
                        # invoice_data["dept"].append(data_row[1])
                        # invoice_data["account"].append(data_row[2])
                        # invoice_data["SL"].append(data_row[3])
                        # invoice_data["project"].append(data_row[4])
                        # invoice_data["activity"].append(data_row[5])
                        # invoice_data["amount"].append(data_row[6])  # Ensure 'Amount' remains
                        
                        # Map each column of data to the correct header
                        invoice_data["store"].append(data_row[0] if len(data_row) > 0 else "")
                        invoice_data["dept"].append(data_row[1] if len(data_row) > 1 else "")
                        invoice_data["account"].append(data_row[2] if len(data_row) > 2 else "")
                        invoice_data["SL"].append(data_row[3] if len(data_row) > 3 else "")
                        invoice_data["project"].append(data_row[4] if len(data_row) > 4 else "")
                        invoice_data["activity"].append(data_row[5] if len(data_row) > 5 else "")
                        invoice_data["amount"].append(data_row[6] if len(data_row) > 6 else "")

        # Combine into final structured JSON
        structured_output = {
            "email_metadata": email_metadata,
            "invoiceDetails": invoice_data,
            "approverDetails": approver_details
        }
        

        # Convert to JSON and return
        final_json = json.dumps(structured_output, indent=4)
        return final_json
    
    except Exception:
        logger.info(f"Error while extracting coding details for template 3: {traceback.format_exc()}")
        # Combine into final structured JSON
        # Combine into final structured JSON
        structured_output = {
                        "email_metadata": email_metadata, 

                        "invoiceDetails": { 
                            "store": [''], 
                            "dept": [''], 
                            "account": [''], 
                            "SL": [''],
                            "project": [''],
                            "activity": [''], 
                            "amount": [''], 
                            "invoice#": '', 
                            "GST": '', 
                            "invoicetotal": '', 
                        }, 
                        "approverDetails": { 
                            "approverName": '', 
                            "TMID": '', 
                            "title": '',
                        }
                    }
        # Convert to JSON and print
        final_json = json.dumps(structured_output, indent=4)
        return final_json

def extract_content_from_msg_file(msg):
    email_metadata = {
        "from": msg.sender,
        "sent": msg.date.utcnow().strftime('%Y-%m-%d %H:%M:%S') if msg.date else None,  # Convert datetime to string
        "to": msg.to
    }

    # Extract email content
    email_content = ""

    # Extract HTML body if available
    if msg.htmlBody:
        email_content += msg.htmlBody.decode("utf-8", errors="replace")  # Decode to string

    # Parse HTML using BeautifulSoup
    soup = BeautifulSoup(email_content, "html.parser")

    # Find all tables
    tables = soup.find_all("table")

    # Function to extract table data and skip empty rows
    def extract_table_data(table):
        rows = table.find_all("tr")
        table_data = []
        for row in rows:
            cells = row.find_all(["td", "th"])  # Include both header and data cells
            row_data = [cell.get_text(strip=True) for cell in cells]
            
            # Skip the row if all cells are empty or contain only whitespace
            if any(cell.strip() for cell in row_data):  # At least one cell has content
                table_data.append(row_data)
        return table_data

    # Extract data from all tables
    all_tables_data = [extract_table_data(table) for table in tables]

    # Combine metadata and table data
    output_data = {
        "email_metadata": email_metadata,
        "tables_data": all_tables_data
    }

    # Convert to JSON
    final_json = json.dumps(output_data, indent=4)
    parsed_data = json.loads(final_json)
    return parsed_data


def identify_template(parsed_data):
    try:
        
        # Extract tables_data from parsed_data
        tables_data = parsed_data.get("tables_data", [])
        
        # Define the headers for each template
        template_1_header = ['Store', 'Dept', 'Account', 'SL', 'Project', 'Activity', 'Subtotal']
        template_2_header = ['Invoice #', 'Store', 'Dept', 'Account', 'SL', 'Project', 'Activity', 'GST', 'Invoice Total']
        template_3_header = ['Store', 'Dept', 'Account', 'SL', 'Project', 'Activity', 'Amount']
        
        # Iterate through tables_data to check for headers
        for table in tables_data:
            for row in table:  # Check each row in the table
                # print(row)
                if row == template_1_header:
                    return "Template 1"
                elif row == template_2_header:
                    return "Template 2"
                elif row == template_3_header:
                    return "Template 3"
        
        return "Unknown Template"
    
    except Exception:
        logger.info(f"Error while determining template type: {traceback.format_exc()}")
        return "Unknown Template"


def clean_parsed_data(parsed_data):
    """
    Recursively clean all occurrences of '$\xa0' from the parsed_data.
    """
    try:
        if isinstance(parsed_data, dict):
            # If the data is a dictionary, recursively clean each value
            return {key: clean_parsed_data(value) for key, value in parsed_data.items()}
        elif isinstance(parsed_data, list):
            # If the data is a list, recursively clean each element
            return [clean_parsed_data(item) for item in parsed_data]
        elif isinstance(parsed_data, str):
            # If the data is a string, replace '$\xa0' and any extra whitespace
            # return parsed_data.replace('$\xa0', '').strip()
            return parsed_data.replace('\xa0', ' ').replace('$', '').strip()
        else:
            # If it's neither dict, list, nor string, return it as is
            return parsed_data
    except Exception as e:
        logger.info(f"Error while cleaning parsed data: {traceback.format_exc()}")
        return parsed_data

def has_extra_empty_strings(parsed_data):
    """
    Check if any row in tables_data contains the specific header with an extra empty string at the end.
    """
    tables_data = parsed_data.get("tables_data", [])
    target_header_with_extra = ['Store', 'Dept', 'Account', 'SL', 'Project', 'Activity', 'Amount', '']
    
    for table in tables_data:
        for row in table:
            if row == target_header_with_extra:
                return True
    return False

def clean_tables_data(parsed_data):
    """
    Clean the tables_data in parsed_data by removing extra empty strings at the end of each row.
    """
    tables_data = parsed_data.get("tables_data", [])
    
    # Iterate over each table and row to remove trailing empty strings
    cleaned_tables_data = []
    for table in tables_data:
        cleaned_table = []
        for row in table:
            cleaned_row = row
            while cleaned_row and cleaned_row[-1] == '':
                cleaned_row = cleaned_row[:-1]  # Remove the last element if it's an empty string
            cleaned_table.append(cleaned_row)
        cleaned_tables_data.append(cleaned_table)
    
    # Update the parsed_data with cleaned tables_data
    parsed_data["tables_data"] = cleaned_tables_data
    return parsed_data



def extract_eml_to_html(blob_data):
    try:
        # Parse the email message from binary blob data
        msg = email.message_from_bytes(blob_data)

        html_content = ""
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                charset = part.get_content_charset()  # Detect encoding
                if charset is None:
                    charset = "utf-8"  # Fallback encoding
                try:
                    html_content = part.get_payload(decode=True).decode(charset, errors="replace")
                except UnicodeDecodeError:
                    html_content = part.get_payload(decode=True).decode("latin1")  # Fallback for Windows-1252
            elif part.get_content_maintype() == "image":
                # Handle image attachments
                image_name = part.get_filename()
                image_data = part.get_payload(decode=True)

                # Encode image in Base64
                if image_data:
                    image_base64 = base64.b64encode(image_data).decode('utf-8')
                    image_type = part.get_content_subtype()  # e.g., "jpeg", "png"

                    # Replace cid: references in HTML with Base64 data URL
                    cid = part.get("Content-ID")
                    if cid:
                        cid = cid.strip("<>")
                        data_url = f"data:image/{image_type};base64,{image_base64}"
                        html_content = html_content.replace(f"cid:{cid}", data_url)
                    # cid = part.get("Content-ID").strip("<>")
                    # data_url = f"data:image/{image_type};base64,{image_base64}"
                    # html_content = html_content.replace(f"cid:{cid}", data_url)

        return html_content
    
    except Exception as e:
        logger.info(f"Error while extracting HTML from email: {traceback.format_exc()}")

def html_to_base64_image(html_content):
    
    try:
        # Set up the config for wkhtmltoimage
        # config = imgkit.config(wkhtmltoimage=config_path)
        config = imgkit.config(wkhtmltoimage=r"/usr/bin/wkhtmltoimage")
        # config = imgkit.config(wkhtmltoimage=r"C:\Program Files\wkhtmltopdf\bin\wkhtmltoimage.exe")
        # Options for imgkit
        options = {
            "format": "png",  # Output format
            "quality": "95",  # Image quality
            "width": 800      # Set the width of the output image
        }

        # Generate image in memory using imgkit and get binary data
        image_data = imgkit.from_string(html_content, False, config=config, options=options)

        # Encode the binary image data to base64
        encoded_image = base64.b64encode(image_data).decode("ascii")

        return encoded_image
    
    except Exception as e:
        logger.error(f"An error occurred in wkhtmltoimage: {traceback.format_exc()}")


def dynamic_split_and_convert_to_pdf(encoded_image, eml_file_path):
    """
    Dynamically splits an image based on its height and converts it to a PDF.
    The PDF is directly uploaded to Azure Blob Storage in the same directory as the input .eml file.

    :param encoded_image: Base64-encoded string of the input PNG image.
    :param eml_file_path: Path to the original .eml file in the blob container.
    :param container_name: Name of the Azure Blob Storage container.
    """
    try:
        new_container_name = "email-pdf-container"
        # Extract directory and base name from .eml file path
        eml_directory = os.path.dirname(eml_file_path)  # Directory path in the blob container
        eml_base_name = os.path.splitext(os.path.basename(eml_file_path))[0]  # File name without extension
        blob_name = f"{eml_directory}/{eml_base_name}.pdf"
        
        # Decode the base64 image data
        image_data = base64.b64decode(encoded_image)
        img = Image.open(BytesIO(image_data))
        width, height = img.size

        # Define thresholds for splitting
        threshold_small = 1000    # Height for small emails
        threshold_medium = 3000  # Height for medium emails

        # Determine number of splits dynamically
        if height <= threshold_small:
            n_splits = 1  # Single page for small emails
        elif height <= threshold_medium:
            n_splits = 2  # Two pages for medium emails
        else:
            n_splits = max(3, height // 1500)  # Split proportionally for large emails

        logger.info(f"Image height: {height}, Splitting into {n_splits} pages.")

        # Calculate the height of each split
        split_height = height // n_splits

        # List to hold images for the PDF
        images_for_pdf = []

        # Split the image and save each part
        for i in range(n_splits):
            upper = i * split_height
            lower = (i + 1) * split_height if i < n_splits - 1 else height
            cropped_img = img.crop((0, upper, width, lower))
            
            # Append to PDF list (convert to RGB if needed)
            images_for_pdf.append(cropped_img.convert("RGB"))

        # Save images to an in-memory PDF
        pdf_bytes_io = BytesIO()
        if images_for_pdf:
            images_for_pdf[0].save(pdf_bytes_io, format="PDF", save_all=True, append_images=images_for_pdf[1:])
            pdf_bytes_io.seek(0)  # Reset the stream position

            # Upload the PDF using the secure upload function
            upload_blob_securely(
                container_name=new_container_name,
                blob_path=blob_name,
                data=pdf_bytes_io.getvalue(),
                content_type="application/pdf"
            )

    except Exception as e:
        logger.error(f"An error occurred converting image to pdf: {traceback.format_exc()}")


def create_or_update_corp_metadata(u_id, v_id, metadata, db):
    try:
        existing_record = (
            db.query(model.corp_metadata)
            .filter(model.corp_metadata.vendorid == v_id)
            .first()
        )
        # Check if the vendor exists
        vendor = db.query(model.Vendor).filter(model.Vendor.idVendor == v_id).first()
        if not vendor:
            return (f"Vendor with id {v_id} does not exist", 404)
        if existing_record:
            # Update existing record
            update_data = {}
            if metadata.synonyms_name is not None:
                # update_data["synonyms_name"] = metadata.synonyms_name  # Directly store list
                update_data["synonyms_name"] = json.dumps(metadata.synonyms_name)
            if metadata.synonyms_address is not None:
                # update_data["synonyms_address"] = metadata.synonyms_address  # Directly store list
                update_data["synonyms_address"] = json.dumps(metadata.synonyms_address)
            if metadata.dateformat:
                update_data["dateformat"] = metadata.dateformat
            update_data["updated_on"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            db.query(model.corp_metadata).filter(
                model.corp_metadata.vendorid == v_id
            ).update(update_data)

            db.commit()
            return {"result": "Updated", "record": update_data}
        else:
            # Insert new record
            new_metadata = model.corp_metadata(
                vendorid=v_id,
                synonyms_name=json.dumps(metadata.synonyms_name) if metadata.synonyms_name else [],
                synonyms_address=json.dumps(metadata.synonyms_address) if metadata.synonyms_address else [],
                dateformat=metadata.dateformat,
                status="Onboarded" if metadata.dateformat != "Not Onboarded" else "Not Onboarded",
                created_on=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                vendorcode= vendor.VendorCode,
                vendorname=vendor.VendorName,
                vendoraddress=vendor.Address,
                currency=vendor.currency,
            )
            db.add(new_metadata)
            db.commit()
            db.refresh(new_metadata)
            return {"result": "Inserted", "record": new_metadata}

    except Exception as e:
        db.rollback()
        logger.error(f"Error processing corp_metadata: {traceback.format_exc()}")
        return {"result": "Failed", "error": str(e)}
    finally:
        db.close()


async def readpaginatedcorpvendorlist(
    u_id,
    db,
    pagination,
    api_filter,
    ven_status
):
    """
    Retrieve a paginated list of vendors with onboarding status based on existence in corp_metadata.

    :param vendor_type: Optional filter for vendor type.
    :param db: Database session.
    :param pagination: Tuple containing (offset, limit).
    :param filters: Dictionary containing optional filters (ven_code, onb_status).
    :param ven_status: Optional filter for vendor status.
    :return: List of vendor details with computed onboarding status.
    """

    try:
        # Subquery to determine onboarding status correctly
        subquery = (
            db.query(
                model.Vendor.idVendor,
                case(
                    (func.count(model.corp_metadata.vendorid) > 0, "Onboarded"),
                    else_="Not-Onboarded",
                ).label("OnboardedStatus"),
            )
            .outerjoin(
                model.corp_metadata,
                (model.Vendor.idVendor == model.corp_metadata.vendorid) &
                (model.corp_metadata.status == "Onboarded")  # Ensures only valid onboarded vendors
            )
            .group_by(model.Vendor.idVendor)
            .subquery()
        )

        # Main query to get vendor details along with onboarding status
        data = (
            db.query(
                model.Vendor,
                subquery.c.OnboardedStatus,
            )
            .options(
                Load(model.Vendor).load_only(
                    "VendorName", "VendorCode", "vendorType", "Address", "City"
                ),
            )
            .outerjoin(subquery, model.Vendor.idVendor == subquery.c.idVendor)
        )

        def normalize_string(input_str):
            return func.lower(func.regexp_replace(input_str, r"[^a-zA-Z0-9]", "", "g"))

        # Apply additional filters
        for key, val in api_filter.items():
            if key == "ven_code" and val:
                normalized_filter = re.sub(r"[^a-zA-Z0-9]", "", val.lower())
                pattern = f"%{normalized_filter}%"
                data = data.filter(
                    or_(
                        normalize_string(model.Vendor.VendorName).ilike(pattern),
                        normalize_string(model.Vendor.VendorCode).ilike(pattern),
                    )
                )
            if key == "onb_status" and val:
                data = data.filter(subquery.c.OnboardedStatus == val)

        # Apply vendor status filter
        if ven_status:
            if ven_status in ["A", "I"]:
                data = data.filter(
                    func.jsonb_extract_path_text(
                        model.Vendor.miscellaneous, "VENDOR_STATUS"
                    ) == ven_status
                )
            else:
                return {"error": f"Invalid vendor status: {ven_status}"}

        # Total count query (with filters applied)
        total_count = data.distinct(model.Vendor.idVendor).count()

        # Pagination
        offset, limit = pagination
        off_val = (offset - 1) * limit
        if off_val < 0:
            return Response(
                status_code=403,
                headers={"ClientError": "Please provide a valid offset value."},
            )

        # Execute paginated query
        vendors = data.distinct().limit(limit).offset(off_val).all()

        # Prepare result
        result = {"data": [], "total_count": total_count}
        for row in vendors:
            row_dict = {}
            for idx, col in enumerate(row):
                if isinstance(col, model.Vendor):
                    row_dict["Vendor"] = {
                        "idVendor": col.idVendor,
                        "VendorName": col.VendorName,
                        "VendorCode": col.VendorCode,
                        "vendorType": col.vendorType,
                        "Address": col.Address,
                        "City": col.City,
                    }
                elif isinstance(col, str):
                    row_dict["OnboardedStatus"] = col
                elif col is None:
                    row_dict[f"col{idx}"] = None
            result["data"].append(row_dict)

        return result
    except Exception:
        logger.error(traceback.format_exc())
        return Response(
            status_code=500, headers={"Error": "Server error", "Desc": "Invalid result"}
        )
    finally:
        db.close()
        

async def download_corp_vendor_list(
    u_id,
    db,
    api_filter,
    ven_status
):
    """
    Retrieve a paginated list of vendors with onboarding status based on existence in corp_metadata.

    :param vendor_type: Optional filter for vendor type.
    :param db: Database session.
    :param pagination: Tuple containing (offset, limit).
    :param filters: Dictionary containing optional filters (ven_code, onb_status).
    :param ven_status: Optional filter for vendor status.
    :return: List of vendor details with computed onboarding status.
    """

    try:
        # Subquery to determine onboarding status correctly
        subquery = (
            db.query(
                model.Vendor.idVendor,
                case(
                    (func.count(model.corp_metadata.vendorid) > 0, "Onboarded"),
                    else_="Not-Onboarded",
                ).label("OnboardedStatus"),
            )
            .outerjoin(
                model.corp_metadata,
                (model.Vendor.idVendor == model.corp_metadata.vendorid) &
                (model.corp_metadata.status == "Onboarded")  # Ensures only valid onboarded vendors
            )
            .group_by(model.Vendor.idVendor)
            .subquery()
        )

        # Main query to get vendor details along with onboarding status
        data = (
            db.query(
                model.Vendor,
                subquery.c.OnboardedStatus,
            )
            .options(
                Load(model.Vendor).load_only(
                    "VendorName", "VendorCode", "vendorType", "Address", "City"
                ),
            )
            .outerjoin(subquery, model.Vendor.idVendor == subquery.c.idVendor)
        )

        def normalize_string(input_str):
            return func.lower(func.regexp_replace(input_str, r"[^a-zA-Z0-9]", "", "g"))

        # Apply additional filters
        for key, val in api_filter.items():
            if key == "ven_code" and val:
                normalized_filter = re.sub(r"[^a-zA-Z0-9]", "", val.lower())
                pattern = f"%{normalized_filter}%"
                data = data.filter(
                    or_(
                        normalize_string(model.Vendor.VendorName).ilike(pattern),
                        normalize_string(model.Vendor.VendorCode).ilike(pattern),
                    )
                )
            if key == "onb_status" and val:
                data = data.filter(subquery.c.OnboardedStatus == val)

        # Apply vendor status filter
        if ven_status:
            if ven_status in ["A", "I"]:
                data = data.filter(
                    func.jsonb_extract_path_text(
                        model.Vendor.miscellaneous, "VENDOR_STATUS"
                    ) == ven_status
                )
            else:
                return {"error": f"Invalid vendor status: {ven_status}"}

        # Total count query (with filters applied)
        total_count = data.distinct(model.Vendor.idVendor).count()
        
        # Execute paginated query
        vendors = data.distinct().all()

        # Prepare result
        result = {"data": [], "total_count": total_count}
        for row in vendors:
            row_dict = {}
            for idx, col in enumerate(row):
                if isinstance(col, model.Vendor):
                    row_dict["Vendor"] = {
                        "idVendor": col.idVendor,
                        "VendorName": col.VendorName,
                        "VendorCode": col.VendorCode,
                        "vendorType": col.vendorType,
                        "Address": col.Address,
                        "City": col.City,
                    }
                elif isinstance(col, str):
                    row_dict["OnboardedStatus"] = col
                elif col is None:
                    row_dict[f"col{idx}"] = None
            result["data"].append(row_dict)

        return result
    except Exception:
        logger.error(traceback.format_exc())
        return Response(
            status_code=500, headers={"Error": "Server error", "Desc": "Invalid result"}
        )
    finally:
        db.close()
        
async def get_metadata_data(u_id, v_id, db):
    """
    Retrieve corp metadata record filtered by vendor ID.

    Parameters:
    ----------
    user_id : int
        ID of the user making the request.
    v_id : int
        Vendor ID used to filter metadata records.
    db : Session
        Database session object.

    Returns:
    -------
    corp_metadata instance or None
    """
    return db.query(model.corp_metadata).filter(model.corp_metadata.vendorid == v_id).first()


async def delete_metadata_values(u_id, v_id, delmetadata, db):
    """
    Delete specific values from the synonyms_name or synonyms_address column in corp_metadata.

    Parameters:
    ----------
    v_id : int
        Vendor ID used to identify the metadata record.
    delmetadata : CorpMetadataDelete
        Object containing lists of values to be removed from the columns.
    db : Session
        Database session object.

    Returns:
    -------
    corp_metadata instance or None
    """
    metadata_record = db.query(model.corp_metadata).filter(model.corp_metadata.vendorid == v_id).first()
    
    if not metadata_record:
        return None
    
    if delmetadata.synonyms_name:
        existing_values = json.loads(metadata_record.synonyms_name) if isinstance(metadata_record.synonyms_name, str) else []
        updated_values = [val for val in existing_values if val not in delmetadata.synonyms_name]
        metadata_record.synonyms_name = json.dumps(updated_values)
    
    if delmetadata.synonyms_address:
        existing_values = json.loads(metadata_record.synonyms_address) if isinstance(metadata_record.synonyms_address, str) else []
        updated_values = [val for val in existing_values if val not in delmetadata.synonyms_address]
        metadata_record.synonyms_address = json.dumps(updated_values)
    
    metadata_record.updated_on = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    
    db.commit()
    return metadata_record


async def get_mail_row_key_summary(u_id, off_limit, db, uni_api_filter, date_range):
    try:
        # Extract offset and limit for pagination
        try:
            offset, limit = off_limit
            off_val = (offset - 1) * limit
        except (TypeError, ValueError):
            off_val, limit = 0, 10

        # Base query for unique mail_row_keys
        base_query = db.query(
            model.CorpQueueTask.mail_row_key,
            func.max(model.CorpQueueTask.created_at).label("latest_created_at"),
        ).filter(model.CorpQueueTask.mail_row_key.isnot(None))

        # Apply universal search filter if provided
        if uni_api_filter:
            search_terms = uni_api_filter.split(":")  # Split by colon
            filter_conditions = []
            
            for term in search_terms:
                # Normalize user input (remove special characters, lowercase)
                # normalized_term = re.sub(r"[^a-zA-Z0-9@. ]", "", term).strip().lower()
                # pattern = f"%{normalized_term}%"
                pattern = f"%{term}%"
                # Add OR conditions for each search term
                filter_conditions.append(
                    or_(
                        func.lower(model.CorpQueueTask.mail_row_key).ilike(pattern),
                        func.lower(model.CorpQueueTask.request_data["subject"].astext).ilike(pattern),
                        func.lower(model.CorpQueueTask.request_data["sender"].astext).ilike(pattern),
                        func.to_char(model.CorpQueueTask.created_at, "YYYY-MM-DD").ilike(pattern),
                        func.lower(model.CorpQueueTask.status).ilike(pattern),
                    )
                )
            
            # Apply filter conditions to the query
            base_query = base_query.filter(or_(*filter_conditions))

        # Apply date range filter if provided
        if date_range:
            try:
                frdate, todate = date_range.lower().split("to")
                frdate = datetime.strptime(frdate.strip(), "%Y-%m-%d")
                
                # Set `todate` to the end of the day (23:59:59)
                todate = datetime.strptime(todate.strip(), "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)

                base_query = base_query.filter(model.CorpQueueTask.created_at.between(frdate, todate))
            except ValueError:
                return {"error": "Invalid date range format. Use YYYY-MM-DD to YYYY-MM-DD", "total_items": 0}
            
        # # Get total count after applying filters
        # total_items = db.query(func.count(func.distinct(model.CorpQueueTask.mail_row_key))).scalar()
        # Get correct total count after applying filters
        total_items = db.query(func.count(func.distinct(model.CorpQueueTask.mail_row_key)))\
                        .filter(or_(*filter_conditions) if uni_api_filter else True)\
                        .filter(model.CorpQueueTask.created_at.between(frdate, todate) if date_range else True)\
                        .scalar()
        # Execute query with pagination
        latest_mail_row_keys = (
            base_query
            .group_by(model.CorpQueueTask.mail_row_key)
            .order_by(func.max(model.CorpQueueTask.created_at).desc())
            .offset(off_val)
            .limit(limit)
            .all()
        )

        data = []
        for row in latest_mail_row_keys:
            data_to_insert = {
                "mail_number": row.mail_row_key,
                "created_at": row.latest_created_at,
                "associated_invoice_files": [],
                "total_attachment_count": 0,  # Default to 0
            }

            # Get related attachments
            related_attachments = (
                db.query(model.corp_trigger_tab)
                .filter(model.corp_trigger_tab.mail_row_key == row.mail_row_key)
                .all()
            )

            if related_attachments:
                for attachment in related_attachments:
                    associated_invoice_files = {
                        "filepath": attachment.blobpath,
                        "type": attachment.blobpath.split(".")[-1] if attachment.blobpath else None,
                        "document_id": attachment.documentid,
                        "status": attachment.status,
                        "file_size": attachment.filesize,
                        "vendor_id": attachment.vendor_id,
                        "page_count": attachment.pagecount,
                    }
                    data_to_insert["associated_invoice_files"].append(associated_invoice_files)
                data_to_insert["total_attachment_count"] = len(related_attachments)
            else:
                # Default structure when no invoice is found
                data_to_insert["associated_invoice_files"].append({
                    "filepath": None,
                    "type": None,
                    "document_id": None,
                    "status": "Invoice Missing",
                    "file_size": None,
                    "vendor_id": None,
                    "page_count": 0,
                })
                data_to_insert["total_attachment_count"] = 0
            # Get email metadata
            queue_task = (
                db.query(model.CorpQueueTask)
                .filter(text("(request_data->>'mail_row_key') = :mail_row_key"))
                .params(mail_row_key=data_to_insert["mail_number"])
                .first()
            )

            if queue_task and queue_task.request_data:
                data_to_insert["email_path"] = queue_task.request_data.get("eml_path")
                data_to_insert["sender"] = queue_task.request_data.get("sender")
                data_to_insert["subject"] = queue_task.request_data.get("subject")
                data_to_insert["status"] = queue_task.status
                data_to_insert["queue_task_id"] = queue_task.id
            else:
                data_to_insert["email_path"] = None
                data_to_insert["sender"] = None
                data_to_insert["subject"] = None
                data_to_insert["status"] = queue_task.status
                data_to_insert["queue_task_id"] = queue_task.id


            data.append(data_to_insert)

        return {"data": data, "total_items": total_items}

    except Exception as e:
        logger.info(traceback.format_exc())
        return {"error": str(e), "total_items": 0}
    
    
def read_corp_doc_invoice_file(u_id, inv_id, db):
    """Function to read the invoice file and return its base64 encoded content
    along with the content type.

    Parameters:
    ----------
    u_id : int
        User ID of the requester.
    inv_id : int
        Invoice ID for which the file is to be retrieved.
    db : Session
        Database session object used to interact with the backend database.

    Returns:
    -------
    dict
        A dictionary containing the file path in base64 format and its content type.
    """
    try:
        content_type = "application/pdf"
        file_name = None
        file_size_mb = None
        # getting invoice data for later operation
        invdat = (
            db.query(model.corp_document_tab)
            .options(load_only("invo_filepath"))
            .filter_by(corp_doc_id=inv_id)
            .one()
        )
        # check if file path is present and give base64 coded image url
        if invdat.invo_filepath:
            try:
                account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
                blob_service_client = BlobServiceClient(
                    account_url=account_url, credential=get_credential()
                )
                # container = settings.container_name
                container = "email-pdf-container"
                # if invdat.vendor_id is None:
                blob_client = blob_service_client.get_blob_client(
                    container=container, blob=invdat.invo_filepath
                )
                
                # Get file name
                file_name = os.path.basename(invdat.invo_filepath)

                # Get file size in MB
                properties = blob_client.get_blob_properties()
                file_size = round(properties.size / (1024 * 1024), 2)  # Convert bytes to MB
                file_size_mb = f"{file_size} MB"
                # invdat.docPath = str(list(blob_client.download_blob().readall()))
                try:
                    filetype = os.path.splitext(invdat.invo_filepath)[1].lower()
                    if filetype == ".png":
                        content_type = "image/png"
                    elif filetype == ".jpg" or filetype == ".jpeg":
                        content_type = "image/jpg"
                    else:
                        content_type = "application/pdf"
                except Exception:
                    print(f"Error in file type : {traceback.format_exc()}")
                invdat.invo_filepath = base64.b64encode(blob_client.download_blob().readall())
            except Exception:
                logger.error(traceback.format_exc())
                invdat.invo_filepath = f"Blob does not exist: {invdat.invo_filepath}"

        return {
            "result": {
                "filepath": invdat.invo_filepath,
                "content_type": content_type,
                "file_name": file_name,
                "file_size_mb": file_size_mb
            }
        }
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500, headers={"codeError": "Server Error"})
    finally:
        db.close()
        
    
    
def read_corp_invoice_file(u_id, inv_id, db):
    """Function to read the invoice file and return its base64 encoded content
    along with the content type.

    Parameters:
    ----------
    u_id : int
        User ID of the requester.
    inv_id : int
        Invoice ID for which the file is to be retrieved.
    db : Session
        Database session object used to interact with the backend database.

    Returns:
    -------
    dict
        A dictionary containing the file path in base64 format and its content type.
    """
    try:
        content_type = "application/pdf"
        file_name = None
        file_size_mb = None
        # getting invoice data for later operation
        invdat = (
            db.query(model.CorpVoucherData)
            .options(load_only("INVOICE_FILE_PATH"))
            .filter_by(DOCUMENT_ID=inv_id)
            .one()
        )
        # check if file path is present and give base64 coded image url
        if invdat.INVOICE_FILE_PATH:
            try:
                account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
                blob_service_client = BlobServiceClient(
                    account_url=account_url, credential=get_credential()
                )
                # container = settings.container_name
                container = "email-pdf-container"
                # if invdat.vendor_id is None:
                blob_client = blob_service_client.get_blob_client(
                    container=container, blob=invdat.INVOICE_FILE_PATH
                )
                
                # Get file name
                file_name = os.path.basename(invdat.INVOICE_FILE_PATH)

                # Get file size in MB
                properties = blob_client.get_blob_properties()
                file_size = round(properties.size / (1024 * 1024), 2)  # Convert bytes to MB
                file_size_mb = f"{file_size} MB"
                # invdat.docPath = str(list(blob_client.download_blob().readall()))
                try:
                    filetype = os.path.splitext(invdat.INVOICE_FILE_PATH)[1].lower()
                    if filetype == ".png":
                        content_type = "image/png"
                    elif filetype == ".jpg" or filetype == ".jpeg":
                        content_type = "image/jpg"
                    else:
                        content_type = "application/pdf"
                except Exception:
                    print(f"Error in file type : {traceback.format_exc()}")
                invdat.INVOICE_FILE_PATH = base64.b64encode(blob_client.download_blob().readall())
            except Exception:
                logger.error(traceback.format_exc())
                invdat.INVOICE_FILE_PATH = f"Blob does not exist: {invdat.INVOICE_FILE_PATH}"

        return {
            "result": {
                "filepath": invdat.INVOICE_FILE_PATH,
                "content_type": content_type,
                "file_name": file_name,
                "file_size_mb": file_size_mb
            }
        }
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500, headers={"codeError": "Server Error"})
    finally:
        db.close()

async def read_corp_invoice_data(u_id, inv_id, db):
    """
    This function reads the invoice list and contains the following parameters:

    Parameters:
    -----------
    u_id : int
        The user ID provided as a function parameter.
    inv_id : int
        The invoice ID provided as a function parameter.
    db : Session
        A session object that interacts with the backend database.

    Returns:
    --------
    dict
        A dictionary containing the result of the vendordata, invoice header,
        line items and upload time .
    """
    try:
        vendordata = ""
        # Fetching invoice data along with DocumentStatus using correct join
        invdat = (
            db.query(model.corp_document_tab, model.DocumentStatus.status)
            .join(
                model.DocumentStatus,
                model.corp_document_tab.documentstatus
                == model.DocumentStatus.idDocumentstatus,
                isouter=True,
            )
            .filter(model.corp_document_tab.corp_doc_id == inv_id)  # Use correct field in filter
            .one()
        )

        # provide vendor details
        if invdat.corp_document_tab.vendor_id:
            vendordata = (
                db.query(model.Vendor)
                .options(
                    Load(model.Vendor).load_only(
                        "VendorName",
                        "VendorCode",
                        "vendorType",
                        "Address",
                        "City",
                        "miscellaneous",
                    )   
                )
                .filter(
                    model.Vendor.idVendor
                    == invdat.corp_document_tab.vendor_id
                )
                .all()
            )
        # provide header deatils of invoce
        headerdata = (
            db.query(model.corp_docdata)
            .filter(model.corp_docdata.corp_doc_id == inv_id)
            .options(
                Load(model.corp_docdata).load_only(
                    "invoice_id",
                    "invoice_date",
                    "vendor_name",
                    "vendoraddress",
                    "currency",
                    "gst",
                    "pst",
                    "invoicetotal",
                    "subtotal",
                    "doc_updates",
                    "document_type",
                    "approver",
                    "approver_title"
                )
            )
        )
        headerdata = headerdata.all()
        
        # provide header deatils of invoce
        codingdata = (
            db.query(model.corp_coding_tab)
            .filter(model.corp_coding_tab.corp_doc_id == inv_id)
            .options(
                Load(model.corp_coding_tab).load_only(
                    "invoice_id",
                    "coding_details",
                    "approver_name",
                    "tmid",
                    "approver_title",
                    "invoicetotal",
                    "gst",
                    "approval_status",
                    "sender_name",
                    "sender_email",
                    "approver_email",
                    "approved_on",
                    "approval_status",
                    "mail_rw_key",
                    "map_type",
                    "sender_title",
                    
                )
            )
        )
        codingdata = codingdata.all()

        return {
            "ok": {
                "vendordata": vendordata,
                "headerdata": headerdata,
                "uploadtime": invdat.corp_document_tab.uploaded_date,
                "codingdata": codingdata,
                "documentstatusid": invdat.corp_document_tab.documentstatus,
                "documentsubstatusid": invdat.corp_document_tab.documentsubstatus,
            }
        }

    except Exception:
        logger.error(f"Error in line item :{traceback.format_exc()}")
        return Response(status_code=500, headers={"codeError": "Server Error"})
    finally:
        db.close()
        

async def update_corp_column_pos(u_id, tabtype, col_data, db):
    """Function to update the column position of a specified tab.

    Parameters:
    ----------
    u_id : int
        User ID provided as a function parameter.
    tabtype : str
        Tab type used to identify which tab's column position to update.
    col_data : PydanticModel
        Pydantic model containing the column data for updating the column position.
    bg_task : BackgroundTasks
        Background task manager for handling asynchronous tasks.
    db : Session
        Database session object, used to interact with the backend database.

    Returns:
    -------
    dict
        A dictionary containing the result of the update operation.
    """
    try:
        updated_on = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        for items in col_data:
            items = dict(items)
            items["updated_on"] = updated_on
            items["document_column_pos"] = items.pop("column_pos")

            db.query(model.CorpDocumentColumnPos).filter_by(
                id_document_column=items.pop("id_tab_column")
            ).update(items)

        db.commit()
        return {"result": "updated"}
    except Exception:
        logger.error(traceback.format_exc())
        return Response(
            status_code=403,
            headers={f"{traceback.format_exc()}clientError": "update failed"},
        )
    finally:
        db.close()
        
async def read_corp_column_pos(user_id, tab_type, db):
    """Function to retrieve the column position based on the tab type.

    Parameters:
    ----------
    u_id : int
        User ID provided as a function parameter.
    tabtype : str
        Tab type used to filter and retrieve the column positions.
    db : Session
        Database session object, used to interact with the backend database.

    Returns:
    -------
    dict
        A dictionary containing the column positions for the specified tab type.
    """
    try:
        # Query to retrieve column data based on userID and tab type
        column_data = (
            db.query(model.CorpDocumentColumnPos, model.CorpColumnNameDef)
            .filter_by()
            .options(
                Load(model.CorpDocumentColumnPos).load_only(
                    "document_column_pos", "is_active"
                ),
                Load(model.CorpColumnNameDef).load_only(
                    "column_name", "column_description", "db_columnname"
                ),
            )
            .filter(
                model.CorpDocumentColumnPos.column_name_def_id == model.CorpColumnNameDef.id_column,
                model.CorpDocumentColumnPos.user_id == user_id,
                model.CorpDocumentColumnPos.tab_type == tab_type,
            )
            .all()
        )
        # If no column data is found, copy default settings from the admin (userID=1)
        if len(column_data) == 0:
            allcolumns = (
                db.query(model.CorpDocumentColumnPos)
                .filter(model.CorpDocumentColumnPos.user_id == 1)
                .all()
            )
            # Insert default column positions for the current user
            for ac in allcolumns:
                to_insert = {
                    "column_name_def_id": ac.column_name_def_id,
                    "document_column_pos": ac.document_column_pos,
                    "is_active": ac.is_active,
                    "tab_type": ac.tab_type,
                    "user_id": user_id,
                }
                db.add(model.CorpDocumentColumnPos(**to_insert))
                db.commit()
            # Fetch column data again after inserting defaults
            column_data = (
                db.query(model.CorpDocumentColumnPos, model.CorpColumnNameDef)
                .filter_by()
                .options(
                    Load(model.CorpDocumentColumnPos).load_only(
                        "document_column_pos", "is_active"
                    ),
                    Load(model.CorpColumnNameDef).load_only(
                        "column_name", "column_description", "db_columnname"
                    ),
                )
                .filter(
                    model.CorpDocumentColumnPos.column_name_def_id
                    == model.CorpColumnNameDef.id_column,
                    model.CorpDocumentColumnPos.user_id == user_id,
                    model.CorpDocumentColumnPos.tab_type == tab_type,
                )
                .all()
            )
        # Convert the query result (a tuple of two models) to a list of dictionaries
        column_data_list = []
        for row in column_data:
            row_dict = {}
            for idx, col in enumerate(row):
                if isinstance(col, model.CorpDocumentColumnPos):
                    row_dict["DocumentColumnPos"] = {
                        "document_column_pos": col.document_column_pos,
                        "is_active": col.is_active,
                        "id_document_column": col.id_document_column,
                    }
                elif isinstance(col, model.CorpColumnNameDef):
                    row_dict["ColumnPosDef"] = {
                        "column_name": col.column_name,
                        "column_description": col.column_description,
                        "db_columnname": col.db_columnname,
                        "id_column": col.id_column,
                    }
            column_data_list.append(row_dict)
        return {"col_data": column_data_list}
    except Exception:
        # Log any exceptions and return a 500 response
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        # Ensure the database session is closed
        db.close()
        

async def update_corp_docdata(user_id, corp_doc_id, updates, db):
    try:
        docStatus_id, docSubStatus_id = db.query(
            model.corp_document_tab.documentstatus, model.corp_document_tab.documentsubstatus
        ).filter(model.corp_document_tab.corp_doc_id == corp_doc_id).first()

        # Fetch the existing record
        corp_doc = db.query(model.corp_docdata).filter_by(corp_doc_id=corp_doc_id).first()
        if not corp_doc:
            return {"message": "No record found for the given corp_doc_id", "status": "error"}


        consolidated_updates = []
        
        corp_doc_tab = db.query(model.corp_document_tab).filter_by(corp_doc_id=corp_doc_id).first()
        if not corp_doc_tab:
            return {"message": "No record found in corp_document_tab for the given corp_doc_id", "status": "error"}
        any_updates = False
        vendor_updated = False
        # Iterate through the list of updates
        for update in updates:
            field = update.field
            old_value = update.OldValue
            new_value = update.NewValue
            if field == "vendor_name":
                vendor_code = update.vendorCode
                vendor_record = db.query(model.Vendor).filter_by(VendorCode=vendor_code).first()
                # vendor_record = db.query(model.corp_metadata).filter_by(vendorname=new_value, vendorcode=vendor_code).first()
                # vendor_record = db.query(model.corp_metadata).filter_by(vendorname=new_value, vendorcode=vendor_code).first()
                if vendor_record:
                    # return {"message": "Vendor not exist in Vendor Master"}
                    corp_doc_tab.vendor_code = vendor_record.VendorCode
                    corp_doc_tab.vendor_id = vendor_record.idVendor
                    any_updates = True
                    vendor_updated = True
                    consolidated_updates.append(f"vendor_code: {vendor_record.VendorCode}, vendor_id: {vendor_record.idVendor}")
                    continue
                else:
                    return {"message": "Vendor not exist in Vendor Master"}
            # Ensure the field exists in the model
            if hasattr(corp_doc, field) and field != "vendor_name":
                field_type = type(getattr(corp_doc, field))  # Get the current field's type
                
                # Convert new & old values to the correct data type
                if field_type == int:
                    old_value = int(old_value) if old_value is not None else None
                    new_value = int(new_value) if new_value is not None else None
                elif field_type == float:
                    old_value = float(old_value) if old_value is not None else None
                    new_value = float(new_value) if new_value is not None else None
                elif field_type == str:
                    old_value = str(old_value) if old_value is not None else ""
                    new_value = str(new_value) if new_value is not None else ""

                current_value = getattr(corp_doc, field)  # Get current DB value

                # Only update if the value is actually changing
                if current_value != new_value:
                    setattr(corp_doc, field, new_value)  # Update the field
                    any_updates = True  # Mark that an update has been made
                    
                    # Log the update in CorpDocumentUpdates with the new logic
                    inv_up_data_id = (
                        db.query(model.CorpDocumentUpdates.iddocumentupdates)
                        .filter_by(doc_id=corp_doc_id)
                        .all()
                    )
                    if len(inv_up_data_id) > 0:
                        # If present, set the active status to false for the old row
                        if corp_doc_id:
                            db.query(model.CorpDocumentUpdates).filter_by(
                                doc_id=corp_doc_id, is_active=1
                            ).update({"is_active": 0})
                        
                        db.flush()
                    
                    data = {
                        "doc_id": corp_doc_id,
                        "updated_field": field,
                        "old_value": old_value,  # Keep as original type
                        "new_value": new_value,  # Keep as original type
                        "created_on": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        "user_id": user_id,
                        "is_active": 1
                    }
                    
                    update_log = model.CorpDocumentUpdates(**data)
                    db.add(update_log)
                    db.flush()
                    consolidated_updates.append(f"{field}: {old_value} -> {new_value}")

                    # If the field is one of the specified ones, update corp_document_tab as well
                    if field in ["invoice_id", "invoicetotal", "invoice_date", "document_type"]:
                        setattr(corp_doc_tab, field, new_value)
                        consolidated_updates.append(f"{field} (corp_document_tab): {old_value} -> {new_value}")
        # Updating the consolidated history log for updated fields
        if any_updates:
            try:
                corp_update_docHistory(
                    corp_doc_id,
                    user_id,
                    docStatus_id,  
                    "; ".join(consolidated_updates),
                    db,
                    docSubStatus_id  
                )
            except Exception as e:
                logger.info(f"Error updating document history: {traceback.format_exc()}")

            # Commit changes
            db.commit()
            if vendor_updated:
                return {"message": "Vendorcode and vendor_id is updated in corp_document_tab table", "status": "success"}
            return {"message": "Field(s) updated successfully", "status": "success"}
        else:
            return {"message": "Field(s) already exist or are the same", "status": "no_change"}
    except Exception as e:
        logger.info(f"Error updating corp_docdata: {traceback.format_exc()}")
        db.rollback()
        return {"message": "An error occurred while updating", "status": "error"}

# async def upsert_coding_line_data(user_id, corp_doc_id, updates, db):
#     try:
#         # Fetch document status
#         docStatus_id, docSubStatus_id = db.query(
#             model.corp_document_tab.documentstatus, model.corp_document_tab.documentsubstatus
#         ).filter(model.corp_document_tab.corp_doc_id == corp_doc_id).first() or (None, None)

#         # Fetch or create corp_coding record
#         corp_coding = db.query(model.corp_coding_tab).filter_by(corp_doc_id=corp_doc_id).first()

#         if not corp_coding:
#             # If no record exists, create a new one
#             corp_coding = model.corp_coding_tab(corp_doc_id=corp_doc_id)
#             db.add(corp_coding)
#             is_new_record = True
#         else:
#             is_new_record = False

#         consolidated_updates = []
        
#         corp_doc_tab = db.query(model.corp_document_tab).filter_by(corp_doc_id=corp_doc_id).first()
#         if not corp_doc_tab:
#             return {"message": "No record found in corp_document_tab for the given corp_doc_id", "status": "error"}
#         any_updates = False
#         # Process each update
#         for update in updates:
#             field = update.field
#             old_value = update.OldValue
#             new_value = update.NewValue
#             field_type = type(getattr(corp_coding, field))
#             # Ensure the field exists in the model
#             if field_type == dict:
#                 # Compare JSON objects
#                 if old_value != new_value:
#                     setattr(corp_coding, field, new_value)  # Store as JSON string
#                     # Convert old and new values to JSON string before storing
#                     old_value_str = json.dumps(old_value) if old_value is not None else None
#                     new_value_str = json.dumps(new_value) if new_value is not None else None
#                     any_updates = True
#                     # Log the update in CorpDocumentUpdates with the new logic
#                     inv_up_data_id = (
#                         db.query(model.CorpDocumentUpdates.iddocumentupdates)
#                         .filter_by(doc_id=corp_doc_id)
#                         .all()
#                     )
#                     if len(inv_up_data_id) > 0:
#                         # If present, set the active status to false for the old row
#                         if corp_doc_id:
#                             db.query(model.CorpDocumentUpdates).filter_by(
#                                 doc_id=corp_doc_id, is_active=1
#                             ).update({"is_active": 0})
                        
#                         db.flush()
                    
#                     data = {
#                         "doc_id": corp_doc_id,
#                         "updated_field": field,
#                         "old_value": old_value_str,  # Keep as original type
#                         "new_value": new_value_str,  # Keep as original type
#                         "created_on": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
#                         "user_id": user_id,
#                         "is_active": 1
#                     }
                    
#                     update_log = model.CorpDocumentUpdates(**data)
#                     db.add(update_log)
#                     db.flush()
#                     consolidated_updates.append(f"{field}: JSON Updated")

#             else:
#                 # Convert new & old values to the correct data type
#                 if field_type == int:
#                     old_value = int(old_value) if old_value not in [None, ""] else None
#                     new_value = int(new_value) if new_value not in [None, ""] else None
#                 elif field_type == float:
#                     old_value = float(old_value) if old_value not in [None, ""] else None
#                     new_value = float(new_value) if new_value not in [None, ""] else None
#                 elif field_type == str:
#                     old_value = str(old_value) if old_value is not None else ""
#                     new_value = str(new_value) if new_value is not None else ""
#                 # Check if the document update table already has rows present in it
            
#                 # Update only if value changes
#                 if old_value != new_value:
#                     setattr(corp_coding, field, new_value)
#                     any_updates = True
#                     # Log the update in CorpDocumentUpdates with the new logic
#                     inv_up_data_id = (
#                         db.query(model.CorpDocumentUpdates.iddocumentupdates)
#                         .filter_by(doc_id=corp_doc_id)
#                         .all()
#                     )
#                     if len(inv_up_data_id) > 0:
#                         # If present, set the active status to false for the old row
#                         if corp_doc_id:
#                             db.query(model.CorpDocumentUpdates).filter_by(
#                                 doc_id=corp_doc_id, is_active=1
#                             ).update({"is_active": 0})
                        
#                         db.flush()
                    
#                     data = {
#                         "doc_id": corp_doc_id,
#                         "updated_field": field,
#                         "old_value": old_value,  # Keep as original type
#                         "new_value": new_value,  # Keep as original type
#                         "created_on": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
#                         "user_id": user_id,
#                         "is_active": 1
#                     }
                    
#                     update_log = model.CorpDocumentUpdates(**data)
#                     db.add(update_log)
#                     db.flush()
#                     consolidated_updates.append(f"{field}: {old_value} -> {new_value}")

#                 # If the field is one of the specified ones, update corp_document_tab as well
#                 if field in ["invoice_id", "invoicetotal", "invoice_date","approver_title"]:
#                     corp_doc_field = "approved_by" if field == "approver_name" else field
#                     setattr(corp_doc_tab, corp_doc_field, new_value)
#                     consolidated_updates.append(f"{corp_doc_field} (corp_document_tab): {old_value} -> {new_value}")
#         # If it's a new record, insert it
#         if is_new_record:
#             db.add(corp_coding)

#         # Updating the consolidated history log
#         if any_updates:
#             try:
#                 corp_update_docHistory(
#                     corp_doc_id,
#                     user_id,
#                     docStatus_id,
#                     "; ".join(consolidated_updates),
#                     db,
#                     docSubStatus_id
#                 )
#             except Exception as e:
#                 logger.info(f"Error updating document history: {traceback.format_exc()}")
            
#             # Commit the transaction
#             db.commit()
#             return {"result": "updated", "updated_data": data}
#         else:
#             return {"message": "Field(s) already exist or are the same", "status": "no_change"}
        
#     except Exception as e:
#         logger.info(f"Error in upsert_coding_line_data: {str(e)}")
#         db.rollback()
#         return {"message": "An error occurred while updating", "status": "error"}


def upsert_coding_line_data(user_id, corp_doc_id, updates, db): 
    try:
        # Fetch document status
        docStatus_id, docSubStatus_id = db.query(
            model.corp_document_tab.documentstatus, model.corp_document_tab.documentsubstatus
        ).filter(model.corp_document_tab.corp_doc_id == corp_doc_id).first() or (None, None)
        
        # Fetch or create corp_coding record
        corp_coding = db.query(model.corp_coding_tab).filter_by(corp_doc_id=corp_doc_id).first()
        if not corp_coding:
            # Fetch or create corp_coding record
            # If no record exists, fetch mail_row_key and queue_task_id from corp_trigger_tab
            corp_trigger = db.query(model.corp_trigger_tab).filter_by(documentid=corp_doc_id).first()
            
            mail_row_key = corp_trigger.mail_row_key if corp_trigger else None
            queue_task_id = corp_trigger.corp_queue_id if corp_trigger else None

            corp_coding = model.corp_coding_tab(
                corp_doc_id=corp_doc_id,
                mail_rw_key=mail_row_key,
                queue_task_id=queue_task_id,
                map_type="manual_map"  # Set map_type
            )
            db.add(corp_coding)
            # is_new_record = True
            try:
                corp_update_docHistory(
                    corp_doc_id,
                    user_id,
                    docStatus_id,
                    "coding details added manually.",
                    db,
                    docSubStatus_id
                )
                db.commit()  # ✅ Commit immediately after updating history
            except Exception as e:
                logger.info(f"Error updating document history: {traceback.format_exc()}")
                db.rollback()
        # else:
        #     is_new_record = False

        consolidated_updates = []
        any_updates = False

        # Process each update
        for update in updates:
            field = update.field
            old_value = update.OldValue
            new_value = update.NewValue
            field_type = type(getattr(corp_coding, field))
            
            if field_type == dict:
                if old_value != new_value:
                    setattr(corp_coding, field, new_value)
                    # old_value_str = json.dumps(old_value) if old_value is not None else None
                    # new_value_str = json.dumps(new_value) if new_value is not None else None
                    any_updates = True
                    # Log the update in CorpDocumentUpdates with the new logic
                    inv_up_data_id = (
                        db.query(model.CorpDocumentUpdates.iddocumentupdates)
                        .filter_by(doc_id=corp_doc_id)
                        .all()
                    )
                    if len(inv_up_data_id) > 0:
                        # If present, set the active status to false for the old row
                        if corp_doc_id:
                            db.query(model.CorpDocumentUpdates).filter_by(
                                doc_id=corp_doc_id, is_active=1
                            ).update({"is_active": 0})
                        
                        db.flush()
                    old_value = json.dumps(old_value) if isinstance(old_value, dict) else str(old_value)
                    new_value = json.dumps(new_value) if isinstance(new_value, dict) else str(new_value)
                    data = {
                        "doc_id": corp_doc_id,
                        "updated_field": field,
                        "old_value": old_value,  # Convert dict to JSON string
                        "new_value": new_value,  # Convert dict to JSON string
                        "created_on": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        "user_id": user_id,
                        "is_active": 1
                    }
                    
                    update_log = model.CorpDocumentUpdates(**data)
                    db.add(update_log)
                    db.flush()
                    # consolidated_updates.append(f"{field}: JSON Updated")
                    consolidated_updates.append(f"{field}: {old_value} -> {new_value}")
            else:
                # Convert new & old values to the correct data type
                if field_type == int:
                    old_value = int(old_value) if old_value not in [None, ""] else None
                    new_value = int(new_value) if new_value not in [None, ""] else None
                elif field_type == float:
                    old_value = float(old_value) if old_value not in [None, ""] else None
                    new_value = float(new_value) if new_value not in [None, ""] else None
                elif field_type == str:
                    old_value = str(old_value) if old_value is not None else ""
                    new_value = str(new_value) if new_value is not None else ""
                # Check if the document update table already has rows present in it
            
                # Update only if value changes
                if old_value != new_value:
                    setattr(corp_coding, field, new_value)
                    any_updates = True
                    # Log the update in CorpDocumentUpdates with the new logic
                    inv_up_data_id = (
                        db.query(model.CorpDocumentUpdates.iddocumentupdates)
                        .filter_by(doc_id=corp_doc_id)
                        .all()
                    )
                    if len(inv_up_data_id) > 0:
                        # If present, set the active status to false for the old row
                        if corp_doc_id:
                            db.query(model.CorpDocumentUpdates).filter_by(
                                doc_id=corp_doc_id, is_active=1
                            ).update({"is_active": 0})
                        
                        db.flush()
                    
                    data = {
                            "doc_id": corp_doc_id,
                            "updated_field": field,
                            "old_value": json.dumps(old_value) if isinstance(old_value, dict) else str(old_value),  
                            "new_value": json.dumps(new_value) if isinstance(new_value, dict) else str(new_value),  
                            "created_on": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                            "user_id": user_id,
                            "is_active": 1
                        }
                    
                    update_log = model.CorpDocumentUpdates(**data)
                    db.add(update_log)
                    db.flush()
                    consolidated_updates.append(f"{field}: {old_value} -> {new_value}")

                # Only update corp_document_tab if NOT a new record
                if field in ["invoice_id", "invoicetotal", "invoice_date", "approver_title", "approver_name"]:
                    corp_doc_tab = db.query(model.corp_document_tab).filter_by(corp_doc_id=corp_doc_id).first()
                    if corp_doc_tab:
                        corp_doc_field = "approved_by" if field == "approver_name" else field
                        setattr(corp_doc_tab, corp_doc_field, new_value)
                        consolidated_updates.append(f"{corp_doc_field} (corp_document_tab): {old_value} -> {new_value}")

        # if is_new_record:
        #     db.add(corp_coding)

        if any_updates:
            try:
                corp_update_docHistory(
                    corp_doc_id,
                    user_id,
                    docStatus_id,
                    "; ".join(consolidated_updates),
                    db,
                    docSubStatus_id
                )
            except Exception as e:
                logger.info(f"Error updating document history: {traceback.format_exc()}")
            
            # Commit the transaction
            db.commit()
            return {"result": "updated", "updated_data": data}
        else:
            return {"message": "Field(s) already exist or are the same", "status": "no_change"}
        
    except Exception as e:
        logger.info(f"Error in upsert_coding_line_data: {traceback.format_exc()}")
        db.rollback()
        return {"message": "An error occurred while updating", "status": "error"}

def corp_update_docHistory(documentID, userID, documentstatus, documentdesc, db,docsubstatus=0):
    """Function to update the document history by inserting a new record into
    the DocumentHistoryLogs table.

    Parameters:
    ----------
    documentID : int
        The ID of the document for which history is being updated.
    userID : int
        The ID of the user who is making the update.
    documentstatus : int
        The current status of the document being recorded in the history.
    documentdesc : str
        A description or reason for the status change.
    db : Session
        Database session object to interact with the backend.
    docsubstatus : int (optional)
        The sub-status of the document being recorded in the history.


    Returns:
    -------
    None or dict
        Returns None on success or an error message on failure.
    """
    try:
        docHistory = {}
        docHistory["document_id"] = documentID
        docHistory["user_id"] = userID
        docHistory["document_status"] = documentstatus
        docHistory["document_desc"] = documentdesc
        docHistory["created_on"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        if docsubstatus!=0:
            docHistory["document_substatus"] = docsubstatus
        db.add(model.corp_hist_logs(**docHistory))
        db.commit()
    except Exception:
        logger.error(traceback.format_exc())
        db.rollback()
        return {"DB error": "Error while inserting document history"}
    
# Function to insert timestamp before the file extension
def add_uniqueness_to_filename(filename, timestamp):
    if not filename:
        return f"unnamed_{timestamp}"
    parts = filename.rsplit(".", 1)
    if len(parts) == 2:
        return f"{parts[0]}_{timestamp}.{parts[1]}"
    else:
        return f"{filename}_{timestamp}"
    
# CRUD function to process the invoice voucher and send it to peoplesoft
def processCorpInvoiceVoucher(doc_id, db):
    try:
        # Fetch the invoice details from the voucherdata table
        corpvoucherdata = (
            db.query(model.CorpVoucherData)
            .filter(model.CorpVoucherData.DOCUMENT_ID == doc_id)
            .scalar()
        )
        if not corpvoucherdata:
            return {"message": "Voucherdata not found for document ID: {doc_id}"}
        
        # Generate a timestamp string, e.g., "20250404_153045"
        timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        invoice_dt = corpvoucherdata.INVOICE_DT
        invoice_file_name = corpvoucherdata.INVOICE_FILE_PATH.split("/")[-1] or ""
        email_pdf_file_name = corpvoucherdata.EMAIL_PATH.split("/")[-1] or ""
        
        unique_invoice_file_name = add_uniqueness_to_filename(invoice_file_name, invoice_dt)
        unique_email_pdf_file_name = add_uniqueness_to_filename(email_pdf_file_name, timestamp_str)
        
        # Save to DB
        corpvoucherdata.UNIQUE_FILENAME_INVOICE = unique_invoice_file_name
        corpvoucherdata.UNIQUE_FILENAME_EMAIL = unique_email_pdf_file_name

        db.commit()
        # Validate invoice date format (yyyy-mm-dd)
        date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        if not corpvoucherdata.INVOICE_DT or not date_pattern.match(corpvoucherdata.INVOICE_DT):

            return {
                "message": "Invalid Date Format",
                "data": {"Http Response": "408", "Status": "Invalid date format"},
            }
            
        try:
            file_data = read_corp_invoice_file(1, doc_id, db)
            if not file_data or "result" not in file_data:
                raise Exception("Error retrieving file: No result found in file data.")
            
            file_size_mb_1 = file_data["result"].get("file_size_mb", "0 MB").split(" ")[0]  # Extract numeric part
            file_size_mb_1 = float(file_size_mb_1)  # Convert to float
            
            # # Check if file size exceeds 5 MB
            # if file_size_mb_1 > 5:
            #     return {
            #         "message": "Failure: Invoice File Size more than 5 mb",
            #         "data": {"Http Response": "410", "Status": "Invoice File Size Exceeds 5 mb"},
            #     }
            base64file = file_data["result"]["filepath"] if file_data and "result" in file_data else None
            if isinstance(base64file, bytes):
                base64file = base64file.decode("utf-8")
            if not base64file:
                raise Exception("Error retrieving file: No result found in file data.")
        except Exception as e:
            logger.error(f"Error in read_invoice_file_voucher: {traceback.format_exc()}")
            return {
                "message": "Failure: File Attachment could not be loaded",
                "data": {"Http Response": "409", "Status": "Error retrieving file"},
            }
        
        try:
            eml_file_data = read_corp_email_pdf_file(1, doc_id, db)
            
            if not eml_file_data or "result" not in eml_file_data:
                raise Exception("Error retrieving file: No result found in file data.")
            
            file_size_mb_2  = eml_file_data["result"].get("file_size_mb", "0 MB").split(" ")[0]  # Extract numeric part
            file_size_mb_2  = float(file_size_mb_2 )  # Convert to float
            
            # # Check if file size exceeds 5 MB
            # if file_size_mb_2  > 5:
            #     return {
            #         "message": "Failure: Email pdf File Size more than 5 mb",
            #         "data": {"Http Response": "410", "Status": "Email pdf File Size Exceeds 5 mb"},
            #     }
                
            base64eml = eml_file_data["result"]["filepath"] if eml_file_data and "result" in eml_file_data else None
            if isinstance(base64eml, bytes):
                base64eml = base64eml.decode("utf-8")
            if not base64eml:
                raise Exception("Error retrieving email file: No result found in file data.")
        except Exception as e:
            logger.error(f"Error in read_corp_email_pdf_file: {traceback.format_exc()}")
            return {
                "message": "Failure: File Attachment could not be loaded",
                "data": {"Http Response": "409", "Status": "Error retrieving file"},
            }
        
        # Check if the combined file size exceeds 5 MB
        total_file_size_mb = file_size_mb_1 + file_size_mb_2
        if total_file_size_mb > 5:
            return {
                "message": "Combined file size exceeding 5mb",
                "data": {"Http Response": "410", "Status": "Combined file size exceeding 5mb"},
            }
    
        # logger.info(f"base64eml for doc id: {doc_id}: {base64eml}")
        
        if isinstance(corpvoucherdata.VCHR_DIST_STG, str):
            vchr_dist_stg = json.loads(corpvoucherdata.VCHR_DIST_STG)
        elif isinstance(corpvoucherdata.VCHR_DIST_STG, dict):
            vchr_dist_stg = corpvoucherdata.VCHR_DIST_STG
        else:
            vchr_dist_stg = {}
        distrib_data = [
            {
                "BUSINESS_UNIT": "NONPO",
                "VOUCHER_LINE_NUM": 1,
                "DISTRIB_LINE_NUM": int(key),
                "BUSINESS_UNIT_GL": "OFG01",
                "ACCOUNT": dist.get("account", ""),
                "DEPTID": dist.get("dept", ""),
                "OPERATING_UNIT": dist.get("store", ""),
                "CHARTFIELD1": dist.get("SL", ""),
                "MERCHANDISE_AMT": dist.get("amount", 0),
                "BUSINESS_UNIT_PC": "OFG01" if dist.get("project") and dist.get("activity") else "",
                "PROJECT_ID": dist.get("project", ""),
                "ACTIVITY_ID": dist.get("activity", "")
            }
            for key, dist in vchr_dist_stg.items()
        ]

        # Construct voucher payload
        voucher_payload = {
            "RequestBody": [
                {
                    "OF_VCHR_IMPORT_STG": [
                        {
                            "VCHR_HDR_STG": [
                                {
                                    "BUSINESS_UNIT": corpvoucherdata.BUSINESS_UNIT or "NONPO",
                                    "VOUCHER_STYLE": "REG",
                                    "INVOICE_ID": corpvoucherdata.INVOICE_ID or "",
                                    "INVOICE_DT": corpvoucherdata.INVOICE_DT or "",
                                    "VENDOR_SETID": corpvoucherdata.VENDOR_SETID or "GLOBL",
                                    "VENDOR_ID": corpvoucherdata.VENDOR_ID or "",
                                    "ORIGIN": corpvoucherdata.ORIGIN or "IDP",
                                    "ACCOUNTING_DT": corpvoucherdata.ACCOUNTING_DT or "",
                                    "VOUCHER_ID_RELATED": "",
                                    "GROSS_AMT": corpvoucherdata.GROSS_AMT or 0,
                                    "SALETX_AMT": 0,
                                    "FREIGHT_AMT": 0,
                                    "MISC_AMT": 0,
                                    "PYMNT_TERMS_CD": "",
                                    "TXN_CURRENCY_CD": corpvoucherdata.TXN_CURRENCY_CD or "CAD",
                                    "VAT_ENTRD_AMT": corpvoucherdata.VAT_ENTRD_AMT or 0,
                                    "VCHR_SRC": corpvoucherdata.VCHR_SRC or "CRP",
                                    "OPRID": corpvoucherdata.OPRID or "",
                                    "VCHR_LINE_STG": [
                                        {
                                            "BUSINESS_UNIT": "NONPO",
                                            "VOUCHER_LINE_NUM": 1,
                                            "DESCR": "",
                                            "MERCHANDISE_AMT": corpvoucherdata.MERCHANDISE_AMT or 0,
                                            "QTY_VCHR": 0.000,
                                            "UNIT_OF_MEASURE": "EA",
                                            "UNIT_PRICE": 0.000,
                                            "VAT_APPLICABILITY": corpvoucherdata.VAT_APPLICABILITY or "",
                                            "BUSINESS_UNIT_RECV": "",
                                            "RECEIVER_ID": "",
                                            "RECV_LN_NBR": 0,
                                            "SHIPTO_ID": corpvoucherdata.SHIPTO_ID or "8000",
                                            "VCHR_DIST_STG": distrib_data
                                        }
                                    ],
                                }
                            ],
                            "INV_METADATA_STG": [
                                {
                                    "BUSINESS_UNIT": "NONPO",
                                    "INVOICE_ID": corpvoucherdata.INVOICE_ID or "",
                                    "INVOICE_DT": corpvoucherdata.INVOICE_DT or "",
                                    "VENDOR_SETID": "GLOBL",
                                    "VENDOR_ID": corpvoucherdata.VENDOR_ID or "",
                                    "IMAGE_NBR": 1,
                                    "FILE_NAME": unique_invoice_file_name,
                                    "base64file": base64file
                                },
                                {
                                    "BUSINESS_UNIT": "NONPO",
                                    "INVOICE_ID": corpvoucherdata.INVOICE_ID or "",
                                    "INVOICE_DT": corpvoucherdata.INVOICE_DT or "",
                                    "VENDOR_SETID": "GLOBL",
                                    "VENDOR_ID": corpvoucherdata.VENDOR_ID or "",
                                    "IMAGE_NBR": 2,
                                    "FILE_NAME": unique_email_pdf_file_name,
                                    "base64file": base64eml
                                }
                            ],
                        }
                    ]
                }
            ]
        }
        
        # try:
        #     json.dumps(voucher_payload)
        # except TypeError as e:
        #     logger.error(f"JSON serialization error: {traceback.format_exc()}")
        #     return {"message": "Serialization Error",
        #             "data": {"Http Response": "106", "Error Message": {traceback.format_exc()}}}
            
        logger.info(f"Final voucher_payload for doc_id: {doc_id}: {json.dumps(voucher_payload, indent=4)}")
        
        # logger.info(f"request_payload for doc_id: {doc_id}: {voucher_payload}")
        # Make a POST request to the external API endpoint
        api_url = settings.erp_invoice_import_endpoint
        headers = {"Content-Type": "application/json"}
        username = settings.erp_user
        password = settings.erp_password
        responsedata = {}
        try:
            # Make the POST request with basic authentication
            response = requests.post(
                api_url,
                json=voucher_payload,
                headers=headers,
                auth=(username, password),
                timeout=60,  # Set a timeout of 60 seconds
            )
            response.raise_for_status()
            # Raises an HTTPError if the response was unsuccessful
            # Log full response details
            logger.info(f"Response Status : {response.status_code}")
            # logger.info(f"Response Text : {response.text}")
            logger.info(f"Response Headers : {response.headers}")
            # logger.info("Response Content: ", response.content.decode())  # Full content

            # Check for success
            # if response.status_code == 200:
            response_data = response.json() if response.content else {}
            return {"message": "Success", "data": response_data} if response_data else {"message": "Success, but response JSON is empty.", "data": response_data}
            
        except Exception:
            logger.info(f"ConnectionError occurred for doc_id: {doc_id}: {traceback.format_exc()}")
            responsedata = {
            "message": "ConnectionError",
            "data": {"Http Response": "500", "Error Message": {traceback.format_exc()}},
            }

    except Exception:
        responsedata = {
            "message": "InternalServerError",
            "data": {"Http Response": "500", "Status": "Fail"},
        }
        logger.error(
            f"Error while processing invoice voucher: {traceback.format_exc()}")
        # raise HTTPException(
        #     status_code=500,
        #     detail=f"Error processing invoice voucher: {str(traceback.format_exc())}",
        # )

    return responsedata

def read_corp_doc_email_pdf_file(u_id, inv_id, db):
    """Function to read the invoice file and return its base64 encoded content
    along with the content type.

    Parameters:
    ----------
    u_id : int
        User ID of the requester.
    inv_id : int
        Invoice ID for which the file is to be retrieved.
    db : Session
        Database session object used to interact with the backend database.

    Returns:
    -------
    dict
        A dictionary containing the file path in base64 format and its content type.
    """
    try:
        content_type = "application/pdf"
        file_name = None
        file_size_mb = None
        # getting invoice data for later operation
        invdat = (
            db.query(model.corp_document_tab)
            .options(load_only("email_filepath_pdf"))
            .filter_by(corp_doc_id=inv_id)
            .one()
        )
        # check if file path is present and give base64 coded image url
        if invdat.email_filepath_pdf:
            try:
                account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
                blob_service_client = BlobServiceClient(
                    account_url=account_url, credential=get_credential()
                )
                # container = settings.container_name
                container = "email-pdf-container"
                # if invdat.vendor_id is None:
                blob_client = blob_service_client.get_blob_client(
                    container=container, blob=invdat.email_filepath_pdf
                )
                # Get file name
                file_name = os.path.basename(invdat.email_filepath_pdf)

                # Get file size in MB
                properties = blob_client.get_blob_properties()
                file_size = round(properties.size / (1024 * 1024), 2)  # Convert bytes to MB
                file_size_mb = f"{file_size} MB"
                # invdat.docPath = str(list(blob_client.download_blob().readall()))
                try:
                    filetype = os.path.splitext(invdat.email_filepath_pdf)[1].lower()
                    if filetype == ".png":
                        content_type = "image/png"
                    elif filetype == ".jpg" or filetype == ".jpeg":
                        content_type = "image/jpg"
                    else:
                        content_type = "application/pdf"
                except Exception:
                    logger.info(f"Error in file type : {traceback.format_exc()}")
                invdat.email_filepath_pdf = base64.b64encode(blob_client.download_blob().readall())
            except Exception:
                logger.error(traceback.format_exc())
                invdat.email_filepath_pdf = f"Blob does not exist: {invdat.email_filepath_pdf}"

        return {
            "result": {
                "filepath": invdat.email_filepath_pdf,
                "content_type": content_type,
                "file_name": file_name,
                "file_size_mb": file_size_mb
            }
        }
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500, headers={"codeError": "Server Error"})
    finally:
        db.close()

def read_corp_email_pdf_file(u_id, inv_id, db):
    """Function to read the invoice file and return its base64 encoded content
    along with the content type.

    Parameters:
    ----------
    u_id : int
        User ID of the requester.
    inv_id : int
        Invoice ID for which the file is to be retrieved.
    db : Session
        Database session object used to interact with the backend database.

    Returns:
    -------
    dict
        A dictionary containing the file path in base64 format and its content type.
    """
    try:
        content_type = "application/pdf"
        file_name = None
        file_size_mb = None
        # getting invoice data for later operation
        invdat = (
            db.query(model.CorpVoucherData)
            .options(load_only("EMAIL_PATH"))
            .filter_by(DOCUMENT_ID=inv_id)
            .one()
        )
        # check if file path is present and give base64 coded image url
        if invdat.EMAIL_PATH:
            try:
                account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
                blob_service_client = BlobServiceClient(
                    account_url=account_url, credential=get_credential()
                )
                # container = settings.container_name
                container = "email-pdf-container"
                # if invdat.vendor_id is None:
                blob_client = blob_service_client.get_blob_client(
                    container=container, blob=invdat.EMAIL_PATH
                )
                # Get file name
                file_name = os.path.basename(invdat.EMAIL_PATH)

                # Get file size in MB
                properties = blob_client.get_blob_properties()
                file_size = round(properties.size / (1024 * 1024), 2)  # Convert bytes to MB
                file_size_mb = f"{file_size} MB"
                # invdat.docPath = str(list(blob_client.download_blob().readall()))
                try:
                    filetype = os.path.splitext(invdat.EMAIL_PATH)[1].lower()
                    if filetype == ".png":
                        content_type = "image/png"
                    elif filetype == ".jpg" or filetype == ".jpeg":
                        content_type = "image/jpg"
                    else:
                        content_type = "application/pdf"
                except Exception:
                    logger.info(f"Error in file type : {traceback.format_exc()}")
                invdat.EMAIL_PATH = base64.b64encode(blob_client.download_blob().readall())
            except Exception:
                logger.error(traceback.format_exc())
                invdat.EMAIL_PATH = f"Blob does not exist: {invdat.EMAIL_PATH}"

        return {
            "result": {
                "filepath": invdat.EMAIL_PATH,
                "content_type": content_type,
                "file_name": file_name,
                "file_size_mb": file_size_mb
            }
        }
    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500, headers={"codeError": "Server Error"})
    finally:
        db.close()
        

def updateCorpInvoiceStatus(u_id, doc_id, db):
    try:
        userID = u_id

        # Fetch document with status ID 7 (Sent to Peoplesoft)
        document = (
            db.query(model.corp_document_tab)
            .filter(
                model.corp_document_tab.corp_doc_id == doc_id,
            )
            .first()
        )

        if not document:
            logger.error(f"Document with ID {doc_id} not found.")
            

        # Fetch associated voucher data
        voucher_data = (
            db.query(model.CorpVoucherData)
            .filter(model.CorpVoucherData.DOCUMENT_ID == doc_id)
            .first()
        )

        if not voucher_data:
            logger.error(f"Voucher data for document ID {doc_id} not found.")

        # API credentials
        api_url = settings.erp_invoice_status_endpoint
        username, password = settings.erp_user, settings.erp_password
        auth = (username, password)
        headers = {"Content-Type": "application/json"}
        
        # Prepare the payload for the API request
        invoice_status_payload = {
            "RequestBody": {
                "INV_STAT_RQST": {
                    "BUSINESS_UNIT": "NONPO",
                    "INVOICE_ID": voucher_data.INVOICE_ID,
                    "INVOICE_DT": voucher_data.INVOICE_DT,
                    "VENDOR_SETID": voucher_data.VENDOR_SETID,
                    "VENDOR_ID": voucher_data.VENDOR_ID,
                }
            }
        }

        try:
            # Make a POST request to the external API
            response = requests.post(
                api_url,
                json=invoice_status_payload,
                headers=headers,
                auth=auth,
                timeout=60,  # Set a timeout of 60 seconds
            )
            response.raise_for_status()  # Raise an exception for HTTP errors
            logger.info(response.json())

            # Process the response if the status code is 200
            if response.status_code == 200:
                invoice_data = response.json()
                entry_status = invoice_data.get("ENTRY_STATUS")
                voucher_id = invoice_data.get("VOUCHER_ID")

                # Determine the new document status based on ENTRY_STATUS
                documentstatusid = None
                docsubstatusid = None
                dmsg = None
                if entry_status == "STG":
                    documentstatusid = 7
                    docsubstatusid = 43
                    dmsg = InvoiceVoucherSchema.SUCCESS_STAGED
                elif entry_status == "QCK":
                    documentstatusid = 14
                    docsubstatusid = 114
                    dmsg = InvoiceVoucherSchema.QUICK_INVOICE
                elif entry_status == "R":
                    documentstatusid = 14
                    docsubstatusid = 115
                    dmsg = InvoiceVoucherSchema.RECYCLED_INVOICE
                elif entry_status == "P":
                    documentstatusid = 14
                    docsubstatusid = 116
                    dmsg = InvoiceVoucherSchema.VOUCHER_CREATED
                elif entry_status == "NF":
                    documentstatusid = 14
                    docsubstatusid = 117
                    dmsg = InvoiceVoucherSchema.VOUCHER_NOT_FOUND
                elif entry_status == "X":
                    documentstatusid = 14
                    docsubstatusid = 119
                    dmsg = InvoiceVoucherSchema.VOUCHER_CANCELLED
                elif entry_status == "S":
                    documentstatusid = 14
                    docsubstatusid = 120
                    dmsg = InvoiceVoucherSchema.VOUCHER_SCHEDULED
                elif entry_status == "C":
                    documentstatusid = 14
                    docsubstatusid = 121
                    dmsg = InvoiceVoucherSchema.VOUCHER_COMPLETED
                elif entry_status == "D":
                    documentstatusid = 14
                    docsubstatusid = 122
                    dmsg = InvoiceVoucherSchema.VOUCHER_DEFAULTED
                elif entry_status == "E":
                    documentstatusid = 14
                    docsubstatusid = 123
                    dmsg = InvoiceVoucherSchema.VOUCHER_EDITED
                elif entry_status == "L":
                    documentstatusid = 14
                    docsubstatusid = 124
                    dmsg = InvoiceVoucherSchema.VOUCHER_REVIEWED
                elif entry_status == "M":
                    documentstatusid = 14
                    docsubstatusid = 125
                    dmsg = InvoiceVoucherSchema.VOUCHER_MODIFIED
                elif entry_status == "O":
                    documentstatusid = 14
                    docsubstatusid = 126
                    dmsg = InvoiceVoucherSchema.VOUCHER_OPEN
                elif entry_status == "T":
                    documentstatusid = 14
                    docsubstatusid = 127
                    dmsg = InvoiceVoucherSchema.VOUCHER_TEMPLATE

                # Update document status and commit the change if valid
                if documentstatusid:
                    document.documentstatus = documentstatusid
                    document.documentsubstatus = docsubstatusid
                    document.voucher_id = voucher_id
                    db.commit()

                    # Update document history
                    corp_update_docHistory(doc_id, userID, documentstatusid,  dmsg, db, docsubstatusid)

                return {
                    "response": response.json(),
                    "status": dmsg,
                    "message": "Invoice status updated successfully",
                }
        except Exception as e:
            logger.error(f"Error for doc_id {doc_id}: {str(e)}")
            

    except Exception as e:
        logger.error(f"Error: {traceback.format_exc()}")
        

def bulkupdateCorpInvoiceStatus():
    try:
        db = next(get_db())
        # Create an operation ID for the background job
        operation_id = uuid4().hex
        set_operation_id(operation_id)
        credential = get_credential()

        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=credential
        )
        container_client = blob_service_client.get_container_client("locks")

        # Update the blob with the latest operation ID and
        # timestamp after acquiring the lease
        blob_client = container_client.get_blob_client("corp-status-job-lock")
        lease = blob_client.acquire_lease()

        logger.info(f"[{datetime.now()}] Background job `Status` Started!")

        userID = 1
        # db = next(get_db())
        # Batch size for processing
        batch_size = 50  # Define a reasonable batch size

        # # Fetch all document IDs with status id 7 (Sent to Peoplesoft) in batches
        # doc_query = db.query(model.Document.idDocument).filter(
        #     model.Document.documentStatusID == 7
        # )
        doc_query = db.query(model.corp_document_tab.corp_doc_id).filter(
            model.corp_document_tab.documentstatus.in_([7, 14]),
            model.corp_document_tab.documentsubstatus.in_([43, 44, 114, 115, 117,]),
        )
        total_docs = doc_query.count()  # Total number of documents to process
        logger.info(f"Total documents to process: {total_docs}")

        # API credentials
        api_url = settings.erp_invoice_status_endpoint
        headers = {"Content-Type": "application/json"}
        auth = (settings.erp_user, settings.erp_password)

        # Success counter
        success_count = 0

        # Process in batches
        for start in range(0, total_docs, batch_size):
            doc_ids = doc_query.offset(start).limit(batch_size).all()

            # Fetch voucher data for each document in the batch
            voucher_data_list = (
                db.query(model.CorpVoucherData)
                .filter(
                    model.CorpVoucherData.DOCUMENT_ID.in_([doc_id[0] for doc_id in doc_ids])
                )
                .all()
            )

            # Prepare payloads and make API requests
            updates = []
            doc_history_updates = []  # Collect history updates in bulk for the batch
            for voucherdata in voucher_data_list:
                dmsg = None  # Initialize dmsg to ensure it's defined
                documentstatusid = 7
                docsubstatusid = 43
                # Prepare the payload for the API request
                invoice_status_payload = {
                    "RequestBody": {
                        "INV_STAT_RQST": {
                            "BUSINESS_UNIT": "NONPO",
                            "INVOICE_ID": voucherdata.INVOICE_ID,
                            "INVOICE_DT": voucherdata.INVOICE_DT,
                            "VENDOR_SETID": voucherdata.VENDOR_SETID,
                            "VENDOR_ID": voucherdata.VENDOR_ID,
                        }
                    }
                }
                logger.info(
                    f"invoice_status_payload for doc_id: {voucherdata.DOCUMENT_ID}: {invoice_status_payload}"
                )  # noqa: E501
                try:
                    # Make a POST request to the external API
                    response = requests.post(
                        api_url,
                        json=invoice_status_payload,
                        headers=headers,
                        auth=auth,
                        timeout=60,  # Set a timeout of 60 seconds
                    )
                    response.raise_for_status()  # Raise an exception for HTTP errors
                    logger.info(
                        f"fetching status for document id: {voucherdata.DOCUMENT_ID}"
                    )
                    logger.info(f"Response: {response.json()}")
                    # Process the response if the status code is 200
                    if response.status_code == 200:
                        invoice_data = response.json()
                        entry_status = invoice_data.get("ENTRY_STATUS")
                        voucher_id = invoice_data.get("VOUCHER_ID")

                        # Determine the new document status based on ENTRY_STATUS
                        if entry_status == "STG":
                            documentstatusid = 7
                            docsubstatusid = 43
                            dmsg = InvoiceVoucherSchema.SUCCESS_STAGED
                        elif entry_status == "QCK":
                            documentstatusid = 14
                            docsubstatusid = 114
                            dmsg = InvoiceVoucherSchema.QUICK_INVOICE
                        elif entry_status == "R":
                            documentstatusid = 14
                            docsubstatusid = 115
                            dmsg = InvoiceVoucherSchema.RECYCLED_INVOICE
                        elif entry_status == "P":
                            documentstatusid = 14
                            docsubstatusid = 116
                            dmsg = InvoiceVoucherSchema.VOUCHER_CREATED
                        elif entry_status == "NF":
                            documentstatusid = 14
                            docsubstatusid = 117
                            dmsg = InvoiceVoucherSchema.VOUCHER_NOT_FOUND
                        elif entry_status == "X":
                            documentstatusid = 14
                            docsubstatusid = 119
                            dmsg = InvoiceVoucherSchema.VOUCHER_CANCELLED
                        elif entry_status == "S":
                            documentstatusid = 14
                            docsubstatusid = 120
                            dmsg = InvoiceVoucherSchema.VOUCHER_SCHEDULED
                        elif entry_status == "C":
                            documentstatusid = 14
                            docsubstatusid = 121
                            dmsg = InvoiceVoucherSchema.VOUCHER_COMPLETED
                        elif entry_status == "D":
                            documentstatusid = 14
                            docsubstatusid = 122
                            dmsg = InvoiceVoucherSchema.VOUCHER_DEFAULTED
                        elif entry_status == "E":
                            documentstatusid = 14
                            docsubstatusid = 123
                            dmsg = InvoiceVoucherSchema.VOUCHER_EDITED
                        elif entry_status == "L":
                            documentstatusid = 14
                            docsubstatusid = 124
                            dmsg = InvoiceVoucherSchema.VOUCHER_REVIEWED
                        elif entry_status == "M":
                            documentstatusid = 14
                            docsubstatusid = 125
                            dmsg = InvoiceVoucherSchema.VOUCHER_MODIFIED
                        elif entry_status == "O":
                            documentstatusid = 14
                            docsubstatusid = 126
                            dmsg = InvoiceVoucherSchema.VOUCHER_OPEN
                        elif entry_status == "T":
                            documentstatusid = 14
                            docsubstatusid = 127
                            dmsg = InvoiceVoucherSchema.VOUCHER_TEMPLATE
                        # If there's a valid document status update,
                        # add it to the bulk update list
                        if documentstatusid:
                            updates.append(
                                {
                                    "corp_doc_id": voucherdata.DOCUMENT_ID,
                                    "documentstatus": documentstatusid,
                                    "documentsubstatus": docsubstatusid,
                                    "voucher_id": voucher_id,
                                }
                            )
                            # Collect doc history update data
                            doc_history_updates.append(
                                {
                                    "document_id": voucherdata.DOCUMENT_ID,
                                    "user_id": userID,
                                    "document_status": documentstatusid,
                                    "document_desc": dmsg,
                                    "created_on": datetime.utcnow().strftime(
                                        "%Y-%m-%d %H:%M:%S"
                                    ),  # noqa: E501
                                }
                            )
                            success_count += 1  # Increment success counter
                except requests.exceptions.RequestException as e:
                    # Log the error and skip this document,
                    # but don't interrupt the batch
                    logger.error(f"Error for doc_id {voucherdata.DOCUMENT_ID}: {str(e)}")

            try:
                # Perform bulk database update for the batch
                if updates:
                    db.bulk_update_mappings(model.corp_document_tab, updates)
                    db.commit()  # Commit the changes for this batch

                logger.info(f"Processed batch {start} to {start + batch_size}")
            except Exception:
                logger.error(f"Error: {traceback.format_exc()}")

            try:
                if doc_history_updates:
                    db.bulk_insert_mappings(
                        model.corp_hist_logs, doc_history_updates
                    )
                    db.commit()  # Commit the history log insertions for this batch

                logger.info(f"Update history log batch {start} to {start + batch_size}")
            except Exception as err:
                dmsg = InvoiceVoucherSchema.FAILURE_COMMON.format_message(err)
                logger.error(f"Error while update dochistlog: {traceback.format_exc()}")

        blob_client.append_block(operation_id + "\n", lease=lease)
        blob_metadata = blob_client.get_blob_properties().metadata
        blob_metadata["last_run_time"] = str(datetime.now())
        blob_client.set_blob_metadata(blob_metadata, lease=lease)
        data = {
            "message": "Bulk update run successfully",
            "total_docs count": total_docs,
            "success_count": success_count,
        }
        logger.info(
            f"[{datetime.now()}] Background job `Status` "
            + f"Completed! with data: {data}"
        )

    except Exception:
        logger.error(f"Error while updating invoice status: {traceback.format_exc()}")
        # raise HTTPException(
        #     status_code=500, detail=f"Error updating invoice status: {str(e)}"
        # )
        return False
    finally:
        db.close()
        if "lease" in locals():
            lease.break_lease()

async def read_corp_doc_history(inv_id, download, db):
    """Function to read invoice history logs.

    Parameters:
    ----------
    inv_id : int
        The ID of the invoice whose history is being retrieved.
    download : bool
        A flag to indicate if the request is for a downloadable version of
        the history logs.
    db : Session
        Database session object to interact with the backend.

    Returns:
    -------
    list
        A list of document history logs with associated details such as
        user and vendor info.
    """
    try:
        # If download is requested, fetch detailed information including vendor
        # and document info
        if download:
            return (
                db.query(
                    model.corp_hist_logs,
                    model.corp_document_tab.invoice_id,
                    model.corp_document_tab.invoice_date,
                    model.corp_document_tab.document_type,
                    model.Vendor.VendorName,
                    model.User.firstName,
                )
                .options(
                    load_only("document_desc", "document_status", "created_on")
                )
                .filter(
                    model.corp_hist_logs.document_id == model.corp_document_tab.corp_doc_id
                )
                .filter(model.corp_hist_logs.user_id == model.User.idUser)
                .join(
                    model.Vendor,
                    model.corp_document_tab.vendor_id == model.Vendor.idVendor,
                    isouter=True,
                )
                # .join(
                #     model.User,
                #     model.corp_hist_logs.user_id == model.User.idUser,
                #     isouter=True,
                # )
                .filter(model.corp_document_tab.corp_doc_id == inv_id)
                .order_by(model.corp_hist_logs.created_on)
                .all()
            )
        else:
            # If download is not requested, fetch only the essential history log details
            return (
                db.query(model.corp_hist_logs, model.User.firstName)
                .options(
                    load_only("document_desc", "document_status", "created_on")
                )
                .filter(
                    model.corp_hist_logs.document_id == model.corp_document_tab.corp_doc_id
                )
                .filter(model.corp_document_tab.corp_doc_id == inv_id)
                .join(model.User, model.corp_hist_logs.user_id == model.User.idUser)
                .order_by(model.corp_hist_logs.created_on)
                .all()
            )
    except Exception:
        # Log the error and return a server error response
        logger.error(traceback.format_exc())
        return Response(status_code=500, headers={"Error": "Server Error"})
    finally:
        # Ensure that the database session is closed after execution
        db.close()
        

async def uploadMissingFile(u_id, inv_id, file, db):
    try:
        # Fetch the invoice data from the database
        invdat = (
            db.query(model.corp_document_tab)
            .options(load_only("email_filepath","invo_filepath","mail_row_key"))
            .filter_by(corp_doc_id=inv_id)
            .one()
        )
        
        eml_filepath = invdat.email_filepath
        mail_row_key = invdat.mail_row_key
        if not eml_filepath:
            return "Email file path not found. Please upload the email file first and try again."
        
        # Extract directory path
        dir_path = eml_filepath.split(".eml")[0]
        
        # Define container and blob names
        container_name = "email-pdf-container"  # Replace with actual container
        blob_path = f"{dir_path}/{file.filename}"
        
        # Read file bytes
        file_bytes = await file.read()  # Awaiting file read
        pdf_bytes_io = BytesIO(file_bytes)
        
        # Upload the PDF using the secure upload function
        upload_blob_securely(
            container_name=container_name,
            blob_path=blob_path,
            data=pdf_bytes_io.getvalue(),
            content_type="application/pdf"
        )
        pdf_bytes_io.close()  # Free memory
        # **Update the email_filepath in the database**
        invdat.invo_filepath = blob_path
        db.commit()  # Commit the transaction to save changes
        return {"message": "File uploaded and path updated successfully", "blob_path": blob_path}
    
    except Exception as e:
        db.rollback()  # Rollback in case of an error
        logger.error(f"An error occurred while uploading the file: {traceback.format_exc()}")
        return {"error": "File upload failed"}

async def uploadMissingEmailFile(u_id, inv_id, file, db):
    try:
        # Fetch the invoice data from the database
        invdat = (
            db.query(model.corp_document_tab)
            .options(load_only("email_filepath_pdf","mail_row_key"))
            .filter_by(corp_doc_id=inv_id)
            .one()
        )
        
        email_filepath_pdf = invdat.email_filepath_pdf
        mail_row_key = invdat.mail_row_key
        if not email_filepath_pdf:
            # raise ValueError("Invalid invoice email pdf file path")
            dir_path = f"ap-portal-invoices/CORPORATE/{mail_row_key}"
        else:
            # Extract directory path
            dir_path = os.path.dirname(email_filepath_pdf)
        
        if not dir_path:
            raise ValueError("Failed to extract directory path from email_filepath")
        # Define container and blob names
        container_name = "email-pdf-container"  # Replace with actual container
        blob_path = f"{dir_path}/{file.filename}"
        
        # Read file bytes
        file_bytes = await file.read()  # Awaiting file read
        pdf_bytes_io = BytesIO(file_bytes)
        
        # Upload the PDF using the secure upload function
        upload_blob_securely(
            container_name=container_name,
            blob_path=blob_path,
            data=pdf_bytes_io.getvalue(),
            content_type="application/pdf"
        )
        pdf_bytes_io.close()  # Free memory
        
        # **Update the email_filepath in the database**
        invdat.email_filepath_pdf = blob_path
        db.commit()  # Commit the transaction to save changes

        return {"message": "File uploaded and path updated successfully", "blob_path": blob_path}
    
    except Exception as e:
        db.rollback()  # Rollback in case of an error
        logger.error(f"An error occurred while uploading the file: {traceback.format_exc()}")
        return {"error": "File upload failed"}
        
        
def processInvoiceFile(u_id, inv_id, blob_path, inv_file, db):
    try:
        try:
            blob_data = inv_file.file.read()
        except Exception as e:
            logger.error(f"Error reading file {inv_file.filename}: {traceback.format_exc()}")
            raise Exception("Failed to read the uploaded file.")

        new_trigger = db.query(model.corp_trigger_tab).filter(model.corp_trigger_tab.documentid == inv_id).first()
        
        if not new_trigger:
            raise Exception(f"No record found for invoice ID {inv_id} in corp_trigger_tab.")

        try:
            logger.info(f"Processing {blob_path} using OpenAI...")
            invoice_data, total_pages, file_size_mb = extract_invoice_details_using_openai(blob_data)

            # Update corp_trigger_tab record upon successful processing
            new_trigger.pagecount = total_pages
            new_trigger.filesize = file_size_mb
            new_trigger.status = "OpenAI Details Extracted"
            new_trigger.updated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            db.commit()

        except Exception as e:
            logger.error(f"Error processing {inv_file.filename}: {traceback.format_exc()}")
            new_trigger.status = "OpenAI Error"
            new_trigger.updated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            db.commit()
            raise Exception("Failed to process invoice details using OpenAI.")

        # postProInvoiceData(invoice_data, blob_path, inv_id)
        # return invoice_data

    except Exception as e:
        logger.error(f"Critical error in processInvoiceFile: {traceback.format_exc()}")
        raise Exception("An error occurred while processing the invoice file.")



async def read_corp_paginate_doc_inv_list(
    u_id,
    ven_id,
    stat,
    off_limit,
    db,
    uni_api_filter,
    ven_status,
    date_range,
    sort_column,
    sort_order,
):
    """Function to read the paginated document invoice list.

    Parameters:
    ----------
    ven_id : int
        The ID of the vendor to filter the invoice documents.
    inv_type : str
        The type of invoice to filter the results.
    stat : Optional[str]
        The status of the invoice for filtering purposes.
    off_limit : tuple
        A tuple containing offset and limit for pagination.
    db : Session
        Database session object used to interact with the backend database.
    uni_api_filter : Optional[str]
        A universal filter for API queries.
    ven_status : Optional[str]
        Status of the vendor to filter the results.

    Returns:
    -------
    list
        A list containing the filtered document invoice data.
    """
    try:
        # Mapping document statuses to IDs
        all_status = {
            "posted": 14,
            "rejected": 10,
            "exception": 4,
            "VendorNotOnboarded": 25,
            "VendorUnidentified": 26,
            "Duplicate Invoice": 32,
        }

        # # new subquery to increase the loading time
        # sub_query_desc = (
        #     db.query(
        #         model.corp_hist_logs.document_id,
        #         model.corp_hist_logs.histlog_id,
        #         model.corp_hist_logs.user_id
        #     )
        #     .distinct(model.corp_hist_logs.document_id)
        #     .order_by(model.corp_hist_logs.document_id, model.corp_hist_logs.histlog_id.desc())
        #     .subquery()
        # )

        # Initial query setup for fetching document, status, and related entities
        data_query = (
            db.query(
                model.corp_document_tab,
                model.DocumentStatus,
                model.DocumentSubStatus,
                model.Vendor,
                # model.corp_docdata,
                # model.User.firstName.label("last_updated_by"),
            )
            .options(
                Load(model.corp_document_tab).load_only(
                    "invoice_id",
                    "invoicetotal",
                    "documentstatus",
                    "updated_on",
                    "documentsubstatus",
                    "sender",
                    "document_type",
                    "invoice_date",
                    "voucher_id",
                    "mail_row_key",
                    "vendor_code",
                    "approved_by",
                    "approver_title",
                    "invoice_type",
                    "created_on",
                ),
                Load(model.DocumentSubStatus).load_only("status"),
                Load(model.DocumentStatus).load_only("status", "description"),
                # Load(model.corp_docdata).load_only("vendor_name", "vendoraddress"),
                Load(model.Vendor).load_only("VendorName", "Address", "VendorCode"),
                
            )
            .join(
                model.DocumentSubStatus,
                model.DocumentSubStatus.idDocumentSubstatus
                == model.corp_document_tab.documentsubstatus,
                isouter=True,
            )
            .join(
                model.Vendor,
                model.Vendor.idVendor == model.corp_document_tab.vendor_id,
                isouter=True,
            )
            .join(
                model.DocumentStatus,
                model.DocumentStatus.idDocumentstatus
                == model.corp_document_tab.documentstatus,
                isouter=True,
            )
            # .join(
            #     model.corp_docdata,
            #     model.corp_docdata.corp_doc_id == model.corp_document_tab.corp_doc_id,
            #     isouter=True,
            # )
            # .join(
            #     sub_query_desc,
            #     sub_query_desc.c.document_id == model.corp_document_tab.corp_doc_id,
            #     isouter=True,
            # )
            # .join(
            #     model.User,
            #     model.User.idUser == sub_query_desc.c.user_id,
            #     isouter=True,
            # )
            .filter(
                model.corp_document_tab.vendor_id.isnot(None),
            )
        )

        # Apply vendor ID filter if provided
        if ven_id:
            sub_query = db.query(model.Vendor.idVendor).filter_by(
                idVendor=ven_id
            )
            data_query = data_query.filter(
                model.corp_document_tab.vendor_id.in_(sub_query)
            )

        status_list = []
        if stat:
            # Split the status string by ':' to get a list of statuses
            status_list = stat.split(":")

            # Map status names to IDs
            status_ids = [all_status[s] for s in status_list if s in all_status]
            if status_ids:
                data_query = data_query.filter(
                    model.corp_document_tab.documentstatus.in_(status_ids)
                )
        # Apply vendor status filter if provided
        if ven_status:
            if ven_status == "A":
                data_query = data_query.filter(
                    func.jsonb_extract_path_text(
                        model.Vendor.miscellaneous, "VENDOR_STATUS"
                    )
                    == "A"
                )
            elif ven_status == "I":
                data_query = data_query.filter(
                    func.jsonb_extract_path_text(
                        model.Vendor.miscellaneous, "VENDOR_STATUS"
                    )
                    == "I"
                )

        # Apply date range filter for documentDate
        if date_range:
            frdate, todate = date_range.lower().split("to")
            frdate = datetime.strptime(frdate.strip(), "%Y-%m-%d")
            
            todate = datetime.strptime(todate, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)
            # Apply the filter
            data_query = data_query.filter(
                model.corp_document_tab.created_on.between(frdate, todate)
            )

        # Function to normalize strings by removing non-alphanumeric
        # characters and converting to lowercase
        def normalize_string(input_str):
            return func.lower(func.regexp_replace(input_str, r"[^a-zA-Z0-9]", "", "g"))

        # Apply universal API filter if provided, including line items
        if uni_api_filter:
            uni_search_param_list = uni_api_filter.split(":")
            for param in uni_search_param_list:
                # Normalize the user input filter
                normalized_filter = re.sub(r"[^a-zA-Z0-9]", "", param.lower())

                # Create a pattern for the search with wildcards
                pattern = f"%{normalized_filter}%"

                filter_condition = or_(
                    normalize_string(model.corp_document_tab.invoice_id).ilike(pattern),
                    normalize_string(model.corp_document_tab.invoice_date).ilike(pattern),
                    normalize_string(model.corp_document_tab.sender).ilike(pattern),
                    # cast(model.corp_document_tab.invoicetotal, String).ilike(
                    #     f"%{uni_api_filter}%"
                    # ),
                    func.to_char(model.corp_document_tab.created_on, "YYYY-MM-DD").ilike(
                        f"%{uni_api_filter}%"
                    ),  # noqa: E501
                    normalize_string(model.corp_document_tab.document_type).ilike(pattern),
                    normalize_string(model.corp_document_tab.voucher_id).ilike(pattern),
                    normalize_string(model.corp_document_tab.mail_row_key).ilike(pattern),
                    normalize_string(model.Vendor.VendorName).ilike(pattern),
                    normalize_string(model.Vendor.Address).ilike(pattern),
                    normalize_string(model.DocumentSubStatus.status).ilike(pattern),
                    normalize_string(model.DocumentStatus.status).ilike(pattern),
                    normalize_string(model.DocumentStatus.description).ilike(pattern),
                )
                data_query = data_query.filter(filter_condition)

        # Get the total count of records before applying limit and offset
        total_count = data_query.distinct(model.corp_document_tab.corp_doc_id).count()
        
        # Pagination
        offset, limit = off_limit
        off_val = (offset - 1) * limit
        if off_val < 0:
            return Response(
                status_code=403,
                headers={"ClientError": "Please provide a valid offset value."},
            )
        
        # Apply sorting
        sort_columns_map = {
            "Invoice Number": model.corp_document_tab.invoice_id,
            "Vendor Code": model.Vendor.VendorCode,
            "Vendor Name": model.Vendor.VendorName,
            "Status": model.DocumentStatus.status,
            "Sub Status": model.DocumentSubStatus.status,
            "Amount": model.corp_document_tab.invoicetotal,
            "Upload Date": model.corp_document_tab.created_on,
        }

        if sort_column in sort_columns_map:
            # sort_field = sort_columns_map.get(sort_column, model.Document.idDocument)
            sort_field = sort_columns_map[sort_column]
            if sort_order.lower() == "desc":
                # Apply descending order to sort_field
                data_query = data_query.order_by(sort_field.desc())
            else:
                # Apply ascending order to sort_field
                data_query = data_query.order_by(sort_field.asc())

            Documentdata = (data_query.limit(limit).offset(off_val).all())
            
        else:
            data_query = data_query.order_by(model.corp_document_tab.corp_doc_id.desc())
            # Apply pagination
            Documentdata = (
            data_query.distinct(model.corp_document_tab.corp_doc_id)
            .limit(limit)
            .offset(off_val)
            .all()
        )

        # Now fetch the last_updated_by field using the document IDs (after pagination)
        # document_ids = [doc.idDocument for doc in Documentdata]
        document_ids = [doc[0].corp_doc_id for doc in Documentdata if hasattr(doc[0], 'corp_doc_id')]
        if document_ids:
            latest_corp_hist_log_query = (
                db.query(
                    model.corp_hist_logs.document_id,
                    model.corp_hist_logs.user_id,
                )
                .filter(model.corp_hist_logs.document_id.in_(document_ids))
                .distinct(model.corp_hist_logs.document_id)  # Ensure distinct document_ids
                .order_by(
                    model.corp_hist_logs.document_id, 
                    model.corp_hist_logs.histlog_id.desc()  # Order by ID in descending order
                )
                .subquery()  # Convert to a subquery for joining later
            )

            # Join the latest history log subquery with User table to get the last_updated_by (firstName)
            user_query = (
                db.query(
                    model.User.firstName.label("last_updated_by"),
                    latest_corp_hist_log_query.c.document_id  # Access document_id from the subquery
                )
                .join(
                    model.User, 
                    model.User.idUser == latest_corp_hist_log_query.c.user_id  # Join condition on userID
                )
            )

            # Convert the result to a dictionary for fast lookup
            user_dict = {user.document_id: user.last_updated_by for user in user_query}

            # Add 'last_updated_by' to Documentdata
            # for doc in Documentdata:
            #     doc[0].last_updated_by = user_dict.get(doc[0].idDocument)
            response_data = []
            for doc in Documentdata:
                document_obj = {
                    "corp_document_tab": doc[0].__dict__,
                    "DocumentStatus": doc[1].__dict__ if doc[1] else {},
                    "DocumentSubStatus": doc[2].__dict__ if doc[2] else {},
                    "Vendor": doc[3].__dict__ if doc[3] else {},
                    "last_updated_by": user_dict.get(doc[0].corp_doc_id)
                }
                # Remove _sa_instance_state from each dictionary
                for k, v in document_obj.items():
                    if isinstance(v, dict):
                        v.pop("_sa_instance_state", None)
                response_data.append(document_obj)
                
        # Return paginated document data with line items
        return {"ok": {"Documentdata": response_data, "TotalCount": total_count}}

    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()
        


async def download_corp_paginate_doc_inv_list(
    u_id,
    ven_id,
    stat,
    date_range,
    db,
    uni_api_filter,
    ven_status,
    
):
    """Function to read the paginated document invoice list.

    Parameters:
    ----------
    ven_id : int
        The ID of the vendor to filter the invoice documents.
    inv_type : str
        The type of invoice to filter the results.
    stat : Optional[str]
        The status of the invoice for filtering purposes.
    off_limit : tuple
        A tuple containing offset and limit for pagination.
    db : Session
        Database session object used to interact with the backend database.
    uni_api_filter : Optional[str]
        A universal filter for API queries.
    ven_status : Optional[str]
        Status of the vendor to filter the results.

    Returns:
    -------
    list
        A list containing the filtered document invoice data.
    """
    try:
        # Mapping document statuses to IDs
        all_status = {
            "posted": 14,
            "rejected": 10,
            "exception": 4,
            "VendorNotOnboarded": 25,
            "VendorUnidentified": 26,
            "Duplicate Invoice": 32,
        }

        # new subquery to increase the loading time
        sub_query_desc = (
            db.query(
                model.corp_hist_logs.document_id,
                model.corp_hist_logs.histlog_id,
                model.corp_hist_logs.user_id
            )
            .distinct(model.corp_hist_logs.document_id)
            .order_by(model.corp_hist_logs.document_id, model.corp_hist_logs.histlog_id.desc())
            .subquery()
        )

        # Initial query setup for fetching document, status, and related entities
        data_query = (
            db.query(
                model.corp_document_tab,
                model.DocumentStatus,
                model.DocumentSubStatus,
                # model.Vendor,
                model.corp_docdata,
                model.User.firstName.label("last_updated_by"),
            )
            .options(
                Load(model.corp_document_tab).load_only(
                    "invoice_id",
                    "invoicetotal",
                    "documentstatus",
                    "created_on",
                    "documentsubstatus",
                    "sender",
                    "document_type",
                    "invoice_date",
                    "voucher_id",
                    "mail_row_key",
                    "vendor_code"
                ),
                Load(model.DocumentSubStatus).load_only("status"),
                Load(model.DocumentStatus).load_only("status", "description"),
                Load(model.corp_docdata).load_only("vendor_name", "vendoraddress"),
                # Load(model.Vendor).load_only("VendorName", "Address", "VendorCode"),
                
            )
            .join(
                model.DocumentSubStatus,
                model.DocumentSubStatus.idDocumentSubstatus
                == model.corp_document_tab.documentsubstatus,
                isouter=True,
            )
            # .join(
            #     model.Vendor,
            #     model.Vendor.idVendor == model.corp_document_tab.vendor_id,
            #     isouter=True,
            # )
            .join(
                model.DocumentStatus,
                model.DocumentStatus.idDocumentstatus
                == model.corp_document_tab.documentstatus,
                isouter=True,
            )
            .join(
                model.corp_docdata,
                model.corp_docdata.corp_doc_id == model.corp_document_tab.corp_doc_id,
                isouter=True,
            )
            # .join(
            #     sub_query_desc,
            #     sub_query_desc.c.document_id == model.corp_document_tab.corp_doc_id,
            #     isouter=True,
            # )
            .join(
                sub_query_desc,
                and_(
                    sub_query_desc.c.document_id == model.corp_document_tab.corp_doc_id,
                    sub_query_desc.c.histlog_id == db.query(func.max(model.corp_hist_logs.histlog_id)).filter(
                        model.corp_hist_logs.document_id == model.corp_document_tab.corp_doc_id
                    ).scalar_subquery(),
                ),
                isouter=True,
            )
            .join(
                model.User,
                model.User.idUser == sub_query_desc.c.user_id,
                isouter=True,
            )
            # .filter(
            #     model.corp_document_tab.vendor_id.isnot(None),
            # )
        )

        # # Apply vendor ID filter if provided
        # if ven_id:
        #     sub_query = db.query(model.corp_document_tab.vendor_id).filter_by(
        #         vendor_id=ven_id
        #     )
        #     data_query = data_query.filter(
        #         model.corp_document_tab.vendor_id.in_(sub_query)
        #     )

        status_list = []
        if stat:
            # Split the status string by ':' to get a list of statuses
            status_list = stat.split(":")

            # Map status names to IDs
            status_ids = [all_status[s] for s in status_list if s in all_status]
            if status_ids:
                data_query = data_query.filter(
                    model.corp_document_tab.documentstatus.in_(status_ids)
                )
        # # Apply vendor status filter if provided
        # if ven_status:
        #     if ven_status == "A":
        #         data_query = data_query.filter(
        #             func.jsonb_extract_path_text(
        #                 model.Vendor.miscellaneous, "VENDOR_STATUS"
        #             )
        #             == "A"
        #         )
        #     elif ven_status == "I":
        #         data_query = data_query.filter(
        #             func.jsonb_extract_path_text(
        #                 model.Vendor.miscellaneous, "VENDOR_STATUS"
        #             )
        #             == "I"
        #         )

        # Apply date range filter for documentDate
        if date_range:
            frdate, todate = date_range.lower().split("to")
            frdate = datetime.strptime(frdate.strip(), "%Y-%m-%d")
            
            todate = datetime.strptime(todate, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)
            # Apply the filter
            data_query = data_query.filter(
                model.corp_document_tab.created_on.between(frdate, todate)
            )

        # Function to normalize strings by removing non-alphanumeric
        # characters and converting to lowercase
        def normalize_string(input_str):
            return func.lower(func.regexp_replace(input_str, r"[^a-zA-Z0-9]", "", "g"))

        # Apply universal API filter if provided, including line items
        if uni_api_filter:
            uni_search_param_list = uni_api_filter.split(":")
            for param in uni_search_param_list:
                # Normalize the user input filter
                normalized_filter = re.sub(r"[^a-zA-Z0-9]", "", param.lower())

                # Create a pattern for the search with wildcards
                pattern = f"%{normalized_filter}%"

                filter_condition = or_(
                    normalize_string(model.corp_document_tab.invoice_id).ilike(pattern),
                    normalize_string(model.corp_document_tab.invoice_date).ilike(pattern),
                    normalize_string(model.corp_document_tab.sender).ilike(pattern),
                    # cast(model.corp_document_tab.invoicetotal, String).ilike(
                    #     f"%{uni_api_filter}%"
                    # ),
                    func.to_char(model.corp_document_tab.created_on, "YYYY-MM-DD").ilike(
                        f"%{uni_api_filter}%"
                    ),  # noqa: E501
                    normalize_string(model.corp_document_tab.document_type).ilike(pattern),
                    normalize_string(model.corp_document_tab.voucher_id).ilike(pattern),
                    normalize_string(model.corp_document_tab.mail_row_key).ilike(pattern),
                    normalize_string(model.corp_docdata.vendor_name).ilike(pattern),
                    normalize_string(model.corp_docdata.vendoraddress).ilike(pattern),
                    normalize_string(model.DocumentSubStatus.status).ilike(pattern),
                    normalize_string(model.DocumentStatus.status).ilike(pattern),
                    normalize_string(model.DocumentStatus.description).ilike(pattern),
                )
                data_query = data_query.filter(filter_condition)

        # Get the total count of records before applying limit and offset
        # total_count = data_query.distinct(model.corp_document_tab.corp_doc_id).count()
        total_count = db.query(func.count(distinct(model.corp_document_tab.corp_doc_id))).scalar()

        
        Documentdata = data_query.order_by(model.corp_document_tab.corp_doc_id).all()
            
        # Return paginated document data with line items
        return {"ok": {"Documentdata": Documentdata, "TotalCount": total_count}}

    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500)
    finally:
        db.close()
        

async def reject_corp_invoice(userID, invoiceID, reason, db):
    """Function to reject an invoice by updating its status and logging the change.

    Parameters:
    ----------
    userID : int
        The ID of the user rejecting the invoice.
    invoiceID : int
        The ID of the invoice being rejected.
    reason : str
        The reason provided for rejecting the invoice.
    db : Session
        The database session object used for interacting with the backend.

    Returns:
    -------
    str or dict
        Returns a success message or a dictionary with an error message
        if the operation fails.
    """
    try:
        # Mapping reasons to substatus IDs
        reason_to_substatus = {
            "Coding Error": 162,
            "Approval Missing": 161,
            "No Active Models/Templates": 158,
            "Vendor Not Onboarded": 157,
            "Duplicate": 156,
            "Missing Pages": 155,
            "Invalid Scan": 154,
            "Invoice Details Missing": 153,
        }

        # Determine the appropriate substatus ID, default to 159 if not found
        substatus_id = reason_to_substatus.get(reason, 159)

        # Fetching the first name of the user performing the rejection
        first_name = (
            db.query(model.User.firstName).filter(model.User.idUser == userID).scalar()
        )

        # Updating the document's status to rejected
        db.query(model.corp_document_tab).filter(model.corp_document_tab.corp_doc_id == invoiceID).update(
            {
                "documentstatus": 10,
                "documentsubstatus": substatus_id,
                "documentdescription": reason + "- rejected" + " by " + first_name,
                "updated_on": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

        # Commit the changes to the database
        db.commit()

        # Update document history with the new status change
        corp_update_docHistory(invoiceID, userID, 10, reason, db, substatus_id)

        return "success: document status changed to rejected!"

    except Exception:
        # Logging the error and rolling back any changes in case of failure
        logger.error(traceback.format_exc())
        db.rollback()
        return {"DB error": "Error while updating document status"}


def bulkProcessCorpVoucherData():
    try:
        logger.info(f"Starting bulkProcessCorpVoucherData function")
        db = next(get_db())

        # Create an operation ID for the background job
        operation_id = uuid4().hex
        set_operation_id(operation_id)
        credential = get_credential()

        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=credential
        )
        container_client = blob_service_client.get_container_client("locks")

        # Update the blob with the latest operation ID and timestamp
        # after acquiring the lease
        blob_client = container_client.get_blob_client("corp-creation-job-lock")
        lease = blob_client.acquire_lease()

        logger.info(f"[{datetime.now()}] Background job `Creation` Started!")

        userID = 1
        # Get the retry frequency from the SetRetryCount table
        frequency = db.query(model.SetRetryCount.frequency).filter(
            model.SetRetryCount.is_active==1,
            model.SetRetryCount.task_name=='retry_invoice_creation').first() 
        if frequency:
            frequency = frequency[0]  # Extract the integer value
            
        # Batch size for processing
        batch_size = 50  # Define a reasonable batch size
        # Fetch all document IDs with status id 7 (Sent to Peoplesoft) in batches
        doc_query = db.query(model.corp_document_tab.corp_doc_id).filter(
            model.corp_document_tab.documentstatus == 21,
            model.corp_document_tab.documentsubstatus.in_([152,112,143]),
            or_(model.corp_document_tab.retry_count < frequency, model.corp_document_tab.retry_count == None)  # Handle NULL values
        )

        total_docs = doc_query.count()  # Total number of documents to process
        logger.info(f"Total documents to process: {total_docs}")

        # If no documents to process, log and return
        if total_docs == 0:
            logger.info("No documents to send to Peoplesoft.")
            return {"message": "No documents to send to Peoplesoft."}

        # Success counter
        success_count = 0

        # Process in batches
        for start in range(0, total_docs, batch_size):
            doc_ids = doc_query.offset(start).limit(batch_size).all()
        for (docID,) in doc_ids:
            try:
                resp = processCorpInvoiceVoucher(docID, db)
                try:
                    if "data" in resp:
                        if "Http Response" in resp["data"]:
                            RespCode = resp["data"]["Http Response"]
                            if resp["data"]["Http Response"].isdigit():
                                RespCodeInt = int(RespCode)
                                if RespCodeInt == 201:
                                    dmsg = (
                                        InvoiceVoucherSchema.SUCCESS_STAGED  # noqa: E501
                                    )
                                    docStatus = 7
                                    docSubStatus = 43
                                    success_count += (
                                        1  # Increment on successful status change
                                    )
                                elif RespCodeInt == 400:
                                    dmsg = (
                                        InvoiceVoucherSchema.FAILURE_IICS  # noqa: E501
                                    )
                                    docStatus = 35
                                    docSubStatus = 149

                                elif RespCodeInt == 406:
                                    dmsg = (
                                        InvoiceVoucherSchema.FAILURE_INVOICE  # noqa: E501
                                    )
                                    docStatus = 35
                                    docSubStatus = 148

                                elif RespCodeInt == 408:
                                    dmsg = (
                                        InvoiceVoucherSchema.PAYLOAD_DATA_ERROR  # noqa: E501
                                    )
                                    docStatus = 4
                                    docSubStatus = 146
                                    
                                elif RespCodeInt == 409:
                                    dmsg = (
                                        InvoiceVoucherSchema.BLOB_STORAGE_ERROR  # noqa: E501
                                    )
                                    docStatus = 4
                                    docSubStatus = 147
                                
                                elif RespCodeInt == 410:
                                    dmsg = (
                                        InvoiceVoucherSchema.FILE_SIZE_EXCEEDED  # noqa: E501
                                    )
                                    docStatus = 4
                                    docSubStatus = 160
                                    
                                elif RespCodeInt == 422:
                                    dmsg = (
                                        InvoiceVoucherSchema.FAILURE_PEOPLESOFT  # noqa: E501
                                    )
                                    docStatus = 35
                                    docSubStatus = 150

                                elif RespCodeInt == 424:
                                    dmsg = (
                                        InvoiceVoucherSchema.FAILURE_FILE_ATTACHMENT  # noqa: E501
                                    )
                                    docStatus = 35
                                    docSubStatus = 151

                                elif RespCodeInt == 500:
                                    dmsg = (
                                        InvoiceVoucherSchema.INTERNAL_SERVER_ERROR  # noqa: E501
                                    )
                                    docStatus = 21
                                    docSubStatus = 152
                                    
                                elif RespCodeInt == 104:
                                    dmsg = (
                                        InvoiceVoucherSchema.FAILURE_CONNECTION_ERROR  # noqa: E501
                                    )
                                    docStatus = 21
                                    docSubStatus = 143

                                else:
                                    dmsg = (
                                        InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                                    )
                                    docStatus = 21
                                    docSubStatus = 112
                            else:
                                dmsg = (
                                    InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                                )
                                docStatus = 21
                                docSubStatus = 112
                        else:
                            dmsg = (
                                InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                            )
                            docStatus = 21
                            docSubStatus = 112
                    else:
                        dmsg = (
                            InvoiceVoucherSchema.FAILURE_RESPONSE_UNDEFINED  # noqa: E501
                        )
                        docStatus = 21
                        docSubStatus = 112
                except Exception as err:
                    logger.info(f"PopleSoftResponseError: {err}")
                    dmsg = InvoiceVoucherSchema.FAILURE_COMMON.format_message(  # noqa: E501
                        err
                    )
                    docStatus = 21
                    docSubStatus = 112

                try:
                    logger.info(f"Updating the document status for doc_id:{docID}")
                    db.query(model.corp_document_tab).filter(
                    model.corp_document_tab.corp_doc_id == docID
                    ).update(
                        {
                            model.corp_document_tab.documentstatus: docStatus,
                            model.corp_document_tab.documentsubstatus: docSubStatus,
                            model.corp_document_tab.retry_count: case(
                                (model.corp_document_tab.retry_count.is_(None), 1),  # If NULL, set to 1
                                else_=model.corp_document_tab.retry_count + 1        # Otherwise, increment
                            ) if docStatus == 21 and docSubStatus in [152, 143] else model.corp_document_tab.retry_count
                        }
                    )
                    db.commit()
                except Exception as err:
                    logger.info(f"ErrorUpdatingPostingData: {err}")
                try:
                    # userID = 1
                    corp_update_docHistory(docID, userID, docStatus, dmsg, db, docSubStatus)
                except Exception as e:
                    logger.error(f"pfg_sync 501: {str(e)}")
            except Exception as e:
                print(
                    "Error in ProcessInvoiceVoucher fun(): ",
                    traceback.format_exc(),
                )
                logger.info(f"PopleSoftResponseError: {e}")
                dmsg = InvoiceVoucherSchema.FAILURE_COMMON.format_message(e)
                docStatus = 21
                docSubStatus = 112

                try:
                    db.query(model.corp_document_tab).filter(
                    model.corp_document_tab.corp_doc_id == docID
                    ).update(
                        {
                            model.corp_document_tab.documentstatus: docStatus,
                            model.corp_document_tab.documentsubstatus: docSubStatus,
                        }
                    )
                    db.commit()
                except Exception as err:
                    logger.info(f"ErrorUpdatingPostingData 156: {err}")
                try:
                    documentstatus = 21
                    corp_update_docHistory(docID, userID, documentstatus, dmsg, db, docSubStatus)
                except Exception as e:
                    logger.error(f"ErrorUpdatingDocHistory 163: {str(e)}")
        data = {
            "message": "Voucher processing completed.",
            "Total docs processed": total_docs,
            "success_count": success_count,
        }
        logger.info(
            f"[{datetime.now()}] Background job `Creation` "
            + f"Completed! with data: {data}"
        )
    except Exception:
        logger.error(f"Error in schedule IDP to Peoplesoft : {traceback.format_exc()}")
        return False
    finally:
        db.close()
        if "lease" in locals():
            lease.break_lease()
            

def get_associated_coding_tab_details(u_id, invoice_id, db):
    """
    Retrieve all records from corp_coding_tab filtered by mail_rw_key and map_type = "unmapped".

    Args:
        u_id (int): The user ID.
        mail_rw_key (str): The mail_rw_key to filter records.
        db (Session): Database session dependency.

    Returns:
        List[corp_coding_tab]: A list of matching corp_coding_tab records.
    """
    try:
        mail_row_key = db.query(model.corp_document_tab.mail_row_key).filter(model.corp_document_tab.corp_doc_id == invoice_id).first()
        if mail_row_key:
            query = (
            db.query(model.corp_coding_tab)
            .filter(
                model.corp_coding_tab.mail_rw_key == mail_row_key[0],
                model.corp_coding_tab.map_type == "Unmapped",
                )
            )
            data = query.all()
            total_count = query.count()
            return {"total_count": total_count, "data": data}
        else:
            return {"No mail row key found for the invoice id": invoice_id}
    except Exception:
        logger.error(f"Error in get_associated_coding_tab_details : {traceback.format_exc()}")
        return {"total_count": 0, "data": []}
    

def map_coding_details_by_corp_doc_id(user_id, corp_doc_id, corp_coding_id, db): 
    try:
        # Fetch document status
        docStatus_id, docSubStatus_id = db.query(
            model.corp_document_tab.documentstatus, model.corp_document_tab.documentsubstatus
        ).filter(model.corp_document_tab.corp_doc_id == corp_doc_id).first() or (None, None)
        
        # Fetch or create corp_coding record
        corp_coding = db.query(model.corp_coding_tab).filter_by(corp_coding_id=corp_coding_id).first()
        if corp_coding.corp_doc_id == None and corp_coding.map_type == "Unmapped":
            corp_coding.corp_doc_id = corp_doc_id
            corp_coding.map_type = "user_map"  # Set map_type
            db.add(corp_coding)
            db.commit()
            db.refresh(corp_coding)

            dmsg = f"Coding details mapped by user."
        
            try:
                corp_update_docHistory(
                    corp_doc_id,
                    user_id,
                    docStatus_id,
                    dmsg,
                    db,
                    docSubStatus_id
                )
            except Exception as e:
                logger.info(f"Error updating document history: {traceback.format_exc()}")
        return {"message": "Coding details mapped successfully", "status": "success"}
    except Exception:
        logger.error(f"Error in map_coding_details : {traceback.format_exc()}")    
        return {"message": "An error occurred while mapping coding details", "status": "failure"}
    

def set_map_type_to_user_reviewed(user_id, corp_coding_id, db): 
    try:
        # Fetch or create corp_coding record
        corp_coding = db.query(model.corp_coding_tab).filter_by(corp_coding_id=corp_coding_id).first()
        if corp_coding.corp_doc_id == None and corp_coding.map_type == "Unmapped":
            corp_coding.map_type = "user_reviewed"  # Set map_type
            db.add(corp_coding)
            db.commit()
            db.refresh(corp_coding)

        return {"message": "Coding details mapped type updated to user_reviewed successfully", "status": "success"}
    except Exception:
        logger.error(f"Error in map_coding_details : {traceback.format_exc()}")    
        return {"message": "An error occurred while mapping coding details", "status": "failure"}

async def readcorpvendorname(u_id, db):
    """This function reads the list of VendorNames.

    It contains 2 parameters.
    :param u_id: The user ID for which to fetch vendor data.
    :param db: It provides a session to interact with the backend
        Database, that is of Session Object Type.
    :return: It returns a result of dictionary type.
    """
    try:
        # Extract VENDOR_STATUS from the JSONB column and include it in the result
        vendor_status_expr = func.jsonb_extract_path_text(
            model.Vendor.miscellaneous, "VENDOR_STATUS"
        )

        # Query to get vendor names, codes, and status for both A and I vendors
        query = db.query(
            model.Vendor.VendorName, 
            model.Vendor.VendorCode, 
            vendor_status_expr.label("VendorStatus")  # Extracted status
        ).filter(vendor_status_expr.in_(["A", "I"]))  # Fetch both active and inactive vendors
        
        data = query.all()
        return data

    except Exception:
        logger.error(traceback.format_exc())
        return Response(
            status_code=500, headers={"Error": "Server error", "Desc": "Invalid result"}
        )
    finally:
        db.close()
        

# 
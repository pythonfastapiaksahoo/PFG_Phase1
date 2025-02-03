from datetime import datetime
import email
import json
import base64
import os
import traceback
import imgkit
import re
from PIL import Image
from io import BytesIO
from bs4 import BeautifulSoup
import requests
from pfg_app import settings
from pfg_app import model
from pfg_app.core.utils import upload_blob_securely
from pfg_app.crud.ERPIntegrationCrud import read_invoice_file_voucher
from pfg_app.logger_module import logger

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
    # print(html_content)
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
        "subtotal": []
    }
    approver_details = {}

    # Process the table to differentiate cases
    for table in tables:
        for i, row in enumerate(table):
            # Identify case where "Invoice#" is present in a row
            if "Invoice#" in row[0]:
                # invoice_number = row[1]  # Extract invoice number
                # invoice_data["invoice#"] = invoice_number
                invoice_data["invoice#"] = row[1] if len(row) > 1 else ""

            # Check if header matches expected columns without "Invoice" keyword
            elif row == ["Store", "Dept", "Account", "SL", "Project", "Activity", "Subtotal"]:
                headers = row
                for data_row in table[i + 1:]:
                    # Stop processing if GST or Grand Total rows are reached
                    if "GST:" in data_row[0] or "Grand Total:" in data_row[0]:
                        break

                    # Map each column of data to the correct header
                    invoice_data["store"].append(data_row[0])
                    invoice_data["dept"].append(data_row[1])
                    invoice_data["account"].append(data_row[2])
                    invoice_data["SL"].append(data_row[3])
                    invoice_data["project"].append(data_row[4])
                    invoice_data["activity"].append(data_row[5])
                    invoice_data["subtotal"].append(data_row[6]) # Ensure subtotal remains null

            # Extract GST and Grand Total
            elif "GST:" in row[0]:
                # invoice_data["GST"] = row[1]
                invoice_data["GST"] = row[1] if len(row) > 1 else ""
            elif "Grand Total:" in row[0]:
                # invoice_data["grandTotal"] = row[1]
                invoice_data["invoiceTotal"] = row[1] if len(row) > 1 else "" 

            # Extract approver details
            elif "Approver Name:" in row[0]:
                # approver_details["approverName"] = row[1]
                approver_details["approverName"] = row[1] if len(row) > 1 else ""
            elif "Approver TM ID:" in row[0]:
                # approver_details["TMID"] = row[1]
                approver_details["TMID"] = row[1] if len(row) > 1 else ""
            elif "Approval Title:" in row[0]:
                # approver_details["title"] = row[1]
                approver_details["title"] = row[1] if len(row) > 1 else ""

    # Combine into final structured JSON
    structured_output = {
        "email_metadata": email_metadata,
        "invoiceDetails": invoice_data,
        "approverDetails": approver_details
    }

    # Convert to JSON and print
    final_json = json.dumps(structured_output, indent=4)
    return final_json


def format_data_for_template2(parsed_data):
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
        "invoiceTotal": [],
        "subtotal": None
    }
    approver_details = {}

    # Process the single table
    if tables:
        table = tables[0]  # There's only one table
        for i, row in enumerate(table):
            if i == 0 and "Approver Name:" in row[0]:  # Approver details
                for detail_row in table[:3]:  # First three rows contain approver details
                    if "Approver Name:" in detail_row[0]:
                        approver_details["approverName"] = detail_row[1]
                    elif "Approver TM ID:" in detail_row[0]:
                        approver_details["TMID"] = detail_row[1]
                    elif "Approval Title:" in detail_row[0]:
                        approver_details["title"] = detail_row[1]
            elif i == 3 and "Invoice #" in row[0]:  # Invoice headers
                headers = row
            elif i > 3:  # Invoice data
                invoice_data["invoice#"].append(row[0])
                invoice_data["store"].append(row[1])
                invoice_data["dept"].append(row[2])
                invoice_data["account"].append(row[3])
                invoice_data["SL"].append(row[4])
                invoice_data["project"].append(row[5])
                invoice_data["activity"].append(row[6])
                invoice_data["GST"].append(row[7])
                invoice_data["invoiceTotal"].append(row[8])

    # Combine into final structured JSON
    structured_output = {
        "email_metadata": email_metadata,
        "invoiceDetails": invoice_data,
        "approverDetails": approver_details
    }

    # Convert to JSON and print
    final_json = json.dumps(structured_output, indent=4)
    # print(final_json)
    return final_json

def format_data_for_template3(parsed_data):
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
        "amount": []  # Changed to 'amount' as the column name is "Amount"
    }
    approver_details = {}

    # Process the table to differentiate cases
    for table in tables:
        for i, row in enumerate(table):
            # Extract invoice number (from first row in new format)
            if "Invoice #" in row[0]:
                # invoice_number = row[1]  # Extract invoice number
                # invoice_data["invoice#"] = invoice_number
                invoice_data["invoice#"] = row[1] if len(row) > 1 else ""  # Default to empty if index doesn't exist

            # Extract GST and Grand Total
            elif "GST:" in row[0]:
                # invoice_data["GST"] = row[1]
                invoice_data["GST"] = row[1] if len(row) > 1 else ""  # Default to empty if index doesn't exist

            elif "Grand Total:" in row[0]:
                # invoice_data["grandTotal"] = row[1]
                invoice_data["invoiceTotal"] = row[1] if len(row) > 1 else ""  # Default to empty if index doesn't exist


            # Extract approver details
            elif "Approver Name:" in row[0]:
                # approver_details["approverName"] = row[1]
                approver_details["approverName"] = row[1] if len(row) > 1 else ""

            elif "Approver TM ID:" in row[0]:
                # approver_details["TMID"] = row[1]
                approver_details["TMID"] = row[1] if len(row) > 1 else ""

            elif "Approval Title:" in row[0]:
                # approver_details["title"] = row[1]
                approver_details["title"] = row[1] if len(row) > 1 else ""

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


def clean_parsed_data(parsed_data):
    """
    Recursively clean all occurrences of '$\xa0' from the parsed_data.
    """
    if isinstance(parsed_data, dict):
        # If the data is a dictionary, recursively clean each value
        return {key: clean_parsed_data(value) for key, value in parsed_data.items()}
    elif isinstance(parsed_data, list):
        # If the data is a list, recursively clean each element
        return [clean_parsed_data(item) for item in parsed_data]
    elif isinstance(parsed_data, str):
        # If the data is a string, replace '$\xa0' and any extra whitespace
        return parsed_data.replace('$\xa0', '').strip()
    else:
        # If it's neither dict, list, nor string, return it as is
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
                cid = part.get("Content-ID").strip("<>")
                data_url = f"data:image/{image_type};base64,{image_base64}"
                html_content = html_content.replace(f"cid:{cid}", data_url)

    return html_content

def html_to_base64_image(html_content, config_path):
    
    try:
        # Set up the config for wkhtmltoimage
        config = imgkit.config(wkhtmltoimage=config_path)
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
        print(f"An error occurred: {e}")


def dynamic_split_and_convert_to_pdf(encoded_image, eml_file_path, container_name):
    """
    Dynamically splits an image based on its height and converts it to a PDF.
    The PDF is directly uploaded to Azure Blob Storage in the same directory as the input .eml file.

    :param encoded_image: Base64-encoded string of the input PNG image.
    :param eml_file_path: Path to the original .eml file in the blob container.
    :param container_name: Name of the Azure Blob Storage container.
    """
    try:
        # Extract directory and base name from .eml file path
        eml_directory = os.path.dirname(eml_file_path)  # Directory path in the blob container
        eml_base_name = os.path.splitext(os.path.basename(eml_file_path))[0]  # File name without extension
        blob_name = f"{eml_directory}/{eml_base_name}_output.pdf"  # PDF will be saved in the same directory

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
                container_name=container_name,
                blob_path=blob_name,
                data=pdf_bytes_io.getvalue(),
                content_type="application/pdf"
            )

    except Exception as e:
        print(f"An error occurred: {e}")



# CRUD function to process the invoice voucher and send it to peoplesoft
def processCorpInvoiceVoucher(request_payload):
    try:
        # # Fetch the invoice details from the voucherdata table
        # voucherdata = (
        #     db.query(model.VoucherData)
        #     .filter(model.VoucherData.documentID == doc_id)
        #     .scalar()
        # )
        # if not voucherdata:
        #     return {"message": "Voucherdata not found for document ID: {doc_id}"}

        # # Call the function to get the base64 file and content type
        # try:
        #     file_data = read_invoice_file_voucher(doc_id, db)
        #     if file_data and "result" in file_data:
        #         base64file = file_data["result"]["filepath"]

        #         # If filepath is a bytes object, decode it
        #         if isinstance(base64file, bytes):
        #             base64file = base64file.decode("utf-8")
        #     else:
        #         base64file = "Error retrieving file: No result found in file data."
        # except Exception as e:
        #     # Catch any error from the read_invoice_file
        #     # function and use the error message
        #     base64file = f"Error retrieving file: {str(e)}"

        # Continue processing the file
        # print(f"Filepath (Base64 Encoded or Error): {base64file}")

        # request_payload = {
        #     "RequestBody": [
        #         {
        #             "OF_VCHR_IMPORT_STG": [
        #                 {
        #                     "VCHR_HDR_STG": [
        #                         {
        #                             "BUSINESS_UNIT": "MERCH",
        #                             "VOUCHER_STYLE": "REG",
        #                             "INVOICE_ID": "",
        #                             "INVOICE_DT": "",
        #                             "VENDOR_SETID": "",
        #                             "VENDOR_ID": "",
        #                             "ORIGIN": "IDP",
        #                             "ACCOUNTING_DT": "",
        #                             "VOUCHER_ID_RELATED": "",
        #                             "GROSS_AMT": 0,
        #                             "SALETX_AMT": 0,
        #                             "FREIGHT_AMT": 0,
        #                             "MISC_AMT": 0,
        #                             "PYMNT_TERMS_CD": "",
        #                             "TXN_CURRENCY_CD": "",
        #                             "VAT_ENTRD_AMT": 0,
        #                             "VCHR_LINE_STG": [
        #                                 {
        #                                     "BUSINESS_UNIT": "MERCH",
        #                                     "VOUCHER_LINE_NUM": 1,
        #                                     "DESCR": "",
        #                                     "MERCHANDISE_AMT": 0,
        #                                     "QTY_VCHR": 1,
        #                                     "UNIT_OF_MEASURE": "",
        #                                     "UNIT_PRICE": 0,
        #                                     "VAT_APPLICABILITY": "",
        #                                     "BUSINESS_UNIT_RECV": "",
        #                                     "RECEIVER_ID": "",
        #                                     "RECV_LN_NBR": 0,
        #                                     "SHIPTO_ID": "",
        #                                     "VCHR_DIST_STG": [
        #                                         {
        #                                             "BUSINESS_UNIT": "MERCH",
        #                                             "VOUCHER_LINE_NUM": 1,
        #                                             "DISTRIB_LINE_NUM": 1,
        #                                             "BUSINESS_UNIT_GL": "OFG01",
        #                                             "ACCOUNT": "",
        #                                             "DEPTID": "",
        #                                             "OPERATING_UNIT": "",
        #                                             "MERCHANDISE_AMT": 0,
        #                                             "BUSINESS_UNIT_PC": " ",
        #                                             "PROJECT_ID": " ",
        #                                             "ACTIVITY_ID": " ",
        #                                         }
        #                                     ],
        #                                 }
        #                             ],
        #                         }
        #                     ],
        #                     "INV_METADATA_STG": [
        #                         {
        #                             "BUSINESS_UNIT": "MERCH",
        #                             "INVOICE_ID": "",
        #                             "INVOICE_DT": "",
        #                             "VENDOR_SETID": "",
        #                             "VENDOR_ID": "",
        #                             "IMAGE_NBR": 1,
        #                             "FILE_NAME": "",
        #                             "base64file": "",
        #                         }
        #                     ],
        #                 }
        #             ]
        #         }
        #     ]
        # }
        # logger.info(f"request_payload for doc_id: {doc_id}: {request_payload}")
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
                json=request_payload,
                headers=headers,
                auth=(username, password),
                timeout=60,  # Set a timeout of 60 seconds
            )
            response.raise_for_status()
            # Raises an HTTPError if the response was unsuccessful
            # Log full response details
            logger.info(f"Response Status : {response.status_code}")
            logger.info(f"Response Headers : {response.headers}")
            # print("Response Content: ", response.content.decode())  # Full content

            # Check for success
            if response.status_code == 200:
                try:
                    response_data = response.json()
                    if not response_data:
                        logger.info("Response JSON is empty.")
                        responsedata = {
                            "message": "Success, but response JSON is empty."
                        }
                    else:
                        responsedata = {"message": "Success", "data": response_data}
                except ValueError:
                    # Handle case where JSON decoding fails
                    logger.info("Response returned, but not in JSON format.")
                    responsedata = {
                        "message": "Success, but response is not JSON.",
                        "data": response.text,
                    }

        except requests.exceptions.HTTPError as e:
            logger.info(f"HTTP error occurred: {traceback.format_exc()}")
            logger.info(f"Response content: {response.content.decode()}")
            responsedata = {"message": str(e), "data": response.json()}

    except Exception:
        responsedata = {
            "message": "InternalError",
            "data": {"Http Response": "500", "Status": "Fail"},
        }
        logger.error(
            f"Error while processing invoice voucher: {traceback.format_exc()}")
        # raise HTTPException(
        #     status_code=500,
        #     detail=f"Error processing invoice voucher: {str(traceback.format_exc())}",
        # )

    return responsedata


# CRUD function to add a new record
def create_corp_metadata(u_id, v_id, db, metadata):
    vendor = db.query(model.Vendor).filter(model.Vendor.idVendor == v_id).first()
    if not vendor:
        return (f" Vendor with id {v_id} does not exist", 404)
    
    new_metadata = model.corp_metadata(
        vendorcode=vendor.VendorCode,
        vendorid = v_id,
        synonyms_name=metadata.synonyms_name,
        synonyms_address=metadata.synonyms_address,
        dateformat=metadata.dateformat,
        status="Onboarded" if metadata.dateformat != "Not Onboarded" else "Not Onboarded",
        created_on=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        updated_on=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    )
    db.add(new_metadata)
    db.commit()
    db.refresh(new_metadata)
    return new_metadata
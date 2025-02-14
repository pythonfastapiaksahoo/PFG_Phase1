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
from pfg_app.core.utils import get_credential, upload_blob_securely
from pfg_app.crud.ERPIntegrationCrud import read_invoice_file_voucher
from pfg_app.logger_module import logger
from sqlalchemy import and_, case, func, or_, desc, text
from sqlalchemy.orm import Load, load_only
from datetime import datetime, timedelta
from fastapi import Response
from azure.storage.blob import BlobServiceClient
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
                    invoice_data["invoicetotal"] = row[1] if len(row) > 1 else "" 

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

    except Exception:
        logger.info(f"Error while extracting coding details for template 1:{traceback.format_exc()}")
        # Combine into final structured JSON
        structured_output = {
            "email_metadata": {"Error"},
            "invoiceDetails": {"Error"},
            "approverDetails": {"Error"}
        }
        # Convert to JSON and print
        final_json = json.dumps(structured_output, indent=4)
        return final_json
    
    
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
                    invoice_data["invoicetotal"].append(row[8])

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

    except Exception:
        logger.info(f"Error while extracting coding details for template 2:{traceback.format_exc()}")
        # Combine into final structured JSON
        structured_output = {
            "email_metadata": {"Error"},
            "invoiceDetails": {"Error"},
            "approverDetails": {"Error"}
        }
        # Convert to JSON and print
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
                    invoice_data["invoicetotal"] = row[1] if len(row) > 1 else ""  # Default to empty if index doesn't exist


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
    
    except Exception:
        logger.info(f"Error while extracting coding details for template 3: {traceback.format_exc()}")
        # Combine into final structured JSON
        structured_output = {
            "email_metadata": {"Error"},
            "invoiceDetails": {"Error"},
            "approverDetails": {"Error"}
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
            return parsed_data.replace('$\xa0', '').strip()
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
                    cid = part.get("Content-ID").strip("<>")
                    data_url = f"data:image/{image_type};base64,{image_base64}"
                    html_content = html_content.replace(f"cid:{cid}", data_url)

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
        blob_name = f"{eml_directory}/{eml_base_name}.pdf"  # PDF will be saved in the same directory

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
                        func.to_char(model.CorpQueueTask.created_at, "YYYY-MM-DD").ilike(pattern)
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
            }

            # Get related attachments
            related_attachments = (
                db.query(model.corp_trigger_tab)
                .filter(model.corp_trigger_tab.mail_row_key == row.mail_row_key)
                .all()
            )

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
            else:
                data_to_insert["email_path"] = None
                data_to_insert["sender"] = None
                data_to_insert["subject"] = None

            # Count total attachments
            data_to_insert["total_attachment_count"] = len(data_to_insert["associated_invoice_files"])

            # Determine Overallstatus
            statuses = {attachment["status"] for attachment in data_to_insert["associated_invoice_files"]}

            if not data_to_insert["associated_invoice_files"]:
                data_to_insert["Overallstatus"] = "Queued"
            elif statuses == {"Processed"}:
                data_to_insert["Overallstatus"] = "Completed"
            elif "Processed" in statuses:
                data_to_insert["Overallstatus"] = "Partially Completed"
            elif statuses:
                data_to_insert["Overallstatus"] = "Error"
            else:
                data_to_insert["Overallstatus"] = "Unknown"

            data.append(data_to_insert)

        return {"data": data, "total_items": total_items}

    except Exception as e:
        return {"error": str(e), "total_items": 0}
    
    
async def read_corp_invoice_file(u_id, inv_id, db):
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
                container = settings.container_name
                # if invdat.vendor_id is None:
                blob_client = blob_service_client.get_blob_client(
                    container=container, blob=invdat.invo_filepath
                )
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
                invdat.invo_filepath = ""

        return {"result": {"filepath": invdat.invo_filepath, "content_type": content_type}}

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
                    "sender_name"
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
            raise ValueError("No record found for the given corp_doc_id")

        consolidated_updates = []
        
        corp_doc_tab = db.query(model.corp_document_tab).filter_by(corp_doc_id=corp_doc_id).first()
        if not corp_doc_tab:
            raise ValueError("No record found in corp_document_tab for the given corp_doc_id")
        
        # Iterate through the list of updates
        for update in updates:
            field = update.field
            old_value = update.OldValue
            new_value = update.NewValue

            # Ensure the field exists in the model
            if hasattr(corp_doc, field):
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
                if field in ["invoice_id", "invoicetotal", "invoice_date"]:
                    setattr(corp_doc_tab, field, new_value)
                    consolidated_updates.append(f"{field} (corp_document_tab): {old_value} -> {new_value}")
        # Updating the consolidated history log for updated fields
        if consolidated_updates:
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
                print(f"Error updating document history: {traceback.format_exc()}")

        # Commit changes
        db.commit()
    except Exception as e:
        print(f"Error updating corp_docdata: {traceback.format_exc()}")
        db.rollback()

async def upsert_coding_line_data(user_id, corp_doc_id, updates, db):
    try:
        # Fetch document status
        docStatus_id, docSubStatus_id = db.query(
            model.corp_document_tab.documentstatus, model.corp_document_tab.documentsubstatus
        ).filter(model.corp_document_tab.corp_doc_id == corp_doc_id).first() or (None, None)

        # Fetch or create corp_coding record
        corp_coding = db.query(model.corp_coding_tab).filter_by(corp_doc_id=corp_doc_id).first()

        if not corp_coding:
            # If no record exists, create a new one
            corp_coding = model.corp_coding_tab(corp_doc_id=corp_doc_id)
            db.add(corp_coding)
            is_new_record = True
        else:
            is_new_record = False

        consolidated_updates = []
        
        corp_doc_tab = db.query(model.corp_document_tab).filter_by(corp_doc_id=corp_doc_id).first()
        if not corp_doc_tab:
            raise ValueError("No record found in corp_document_tab for the given corp_doc_id")
        
        # Process each update
        for update in updates:
            field = update.field
            old_value = update.OldValue
            new_value = update.NewValue
            field_type = type(getattr(corp_coding, field))
            # Ensure the field exists in the model
            if field_type == dict:
                # Compare JSON objects
                if old_value != new_value:
                    setattr(corp_coding, field, new_value)  # Store as JSON string
                    # Convert old and new values to JSON string before storing
                    old_value_str = json.dumps(old_value) if old_value is not None else None
                    new_value_str = json.dumps(new_value) if new_value is not None else None
                    
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
                        "old_value": old_value_str,  # Keep as original type
                        "new_value": new_value_str,  # Keep as original type
                        "created_on": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        "user_id": user_id,
                        "is_active": 1
                    }
                    
                    update_log = model.CorpDocumentUpdates(**data)
                    db.add(update_log)
                    db.flush()
                    consolidated_updates.append(f"{field}: JSON Updated")

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
                if field in ["invoice_id", "invoicetotal", "invoice_date"]:
                    setattr(corp_doc_tab, field, new_value)
                    consolidated_updates.append(f"{field} (corp_document_tab): {old_value} -> {new_value}")
        # If it's a new record, insert it
        if is_new_record:
            db.add(corp_coding)

        # Updating the consolidated history log
        if consolidated_updates:
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
                print(f"Error updating document history: {str(e)}")
            return {"result": "updated", "updated_data": data}
        # Commit the transaction
        db.commit()
    
    except Exception as e:
        print(f"Error in upsert_coding_line_data: {str(e)}")
        db.rollback()


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
    
    
    
async def read_corp_invoice_eml_file(inv_id, db):
    """Function to read the invoice and email files, returning base64 encoded content
    along with their content types.

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
        A dictionary containing base64 encoded invoice and email file contents along 
        with their respective content types.
    """
    try:
        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=get_credential())
        container = settings.container_name

        # Fetch invoice document record
        invdat = (
            db.query(model.corp_document_tab)
            .options(load_only("invo_filepath", "email_filepath","vendor_id"))
            .filter_by(corp_doc_id=inv_id)
            .one()
        )

        def fetch_and_encode(filepath):
            """Helper function to fetch and encode a file from Azure Blob Storage."""
            if not filepath:
                return None, None
            try:
                blob_client = blob_service_client.get_blob_client(container=container, blob=filepath)
                # If the file size is within the limit, proceed to read and encode
                filetype = os.path.splitext(invdat.docPath)[1].lower()
                if filetype == ".png":
                    content_type = "image/png"
                elif filetype == ".jpg" or filetype == ".jpeg":
                    content_type = "image/jpg"
                else:
                    content_type = "application/pdf"
                file_data = blob_client.download_blob().readall()
                encoded_data = base64.b64encode(file_data)  # Convert bytes to base64 string

                return encoded_data, content_type
            except Exception:
                logger.error(f"Error processing file {filepath}: {traceback.format_exc()}")
                return None, None

        # Fetch and encode invoice file
        if invdat.vendor_id:
            try:
                if invdat.invo_filepath:
                    inv_base64, inv_content_type = fetch_and_encode(invdat.invo_filepath)
            except Exception:
                inv_base64 = ""
            try:
                if invdat.email_filepath:
                    # Fetch and encode email file
                    email_base64, email_content_type = fetch_and_encode(invdat.email_filepath)
            except Exception:
                email_base64 = ""
        return {
            "result": {
                "invoice": {"filepath": inv_base64, "content_type": inv_content_type},
                "email": {"filepath": email_base64, "content_type": email_content_type}
            }
        }

    except Exception:
        logger.error(traceback.format_exc())
        return Response(status_code=500, headers={"codeError": "Server Error"})
    finally:
        db.close()


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
        
        # Initialize variables to avoid UnboundLocalError
        # base64invoicefile = "Error retrieving invoice file."
        # base64emailfile = "Error retrieving email file."
        # Call the function to get the base64 file and content type
        try:
            file_data = read_corp_invoice_eml_file(doc_id, db)
            if file_data and "result" in file_data:
                base64invoicefile = file_data["result"]["invoice"]["filepath"]
                base64emailfile = file_data["result"]["email"]["filepath"]
                
                # If filepath is a bytes object, decode it
                if isinstance(base64invoicefile, bytes):
                    base64invoicefile = base64invoicefile.decode("utf-8")
                # If filepath is a bytes object, decode it
                if isinstance(base64emailfile, bytes):  
                    base64emailfile = base64emailfile.decode("utf-8")
            else:
                base64invoicefile = "Error retrieving file: No result found in file data."
                base64emailfile = "Error retrieving file: No result found in file data."
        except Exception as e:
            # Catch any error from the read_invoice_file
            # function and use the error message
            base64invoicefile = f"Error retrieving invoice file: {traceback.format_exc()}"
            base64emailfile = f"Error retrieving email file: {traceback.format_exc()}"

        # Continue processing the file
        # print(f"Filepath (Base64 Encoded or Error): {base64file}")
        
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
                "PROJECT_ID": dist.get("project", ""),
                "ACTIVITY_ID": dist.get("activity", ""),
                "MERCHANDISE_AMT": dist.get("amount", 0),
                "BUSINESS_UNIT_PC": ""
            }
            for key, dist in vchr_dist_stg.items()
        ]

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
                                    ]
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
                                    "FILE_NAME": corpvoucherdata.INVOICE_FILE_PATH,
                                    "base64file": base64invoicefile
                                },
                                {
                                    "BUSINESS_UNIT": "NONPO",
                                    "INVOICE_ID": corpvoucherdata.INVOICE_ID or "",
                                    "INVOICE_DT": corpvoucherdata.INVOICE_DT or "",
                                    "VENDOR_SETID": "GLOBL",
                                    "VENDOR_ID": corpvoucherdata.VENDOR_ID or "",
                                    "IMAGE_NBR": 2,
                                    "FILE_NAME": corpvoucherdata.EMAIL_PATH,
                                    "base64file": base64emailfile
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        request_payload = json.dumps(voucher_payload, indent=4)
        logger.info(f"request_payload for doc_id: {doc_id}: {request_payload}")
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

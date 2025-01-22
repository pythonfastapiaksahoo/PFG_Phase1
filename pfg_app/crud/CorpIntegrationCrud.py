import json
import base64
import os
import imgkit
import re
from PIL import Image
from io import BytesIO
from bs4 import BeautifulSoup

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
                invoice_number = row[1]  # Extract invoice number
                invoice_data["invoice#"] = invoice_number

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
                invoice_data["GST"] = row[1]
            elif "Grand Total:" in row[0]:
                invoice_data["grandTotal"] = row[1]

            # Extract approver details
            elif "Approver Name:" in row[0]:
                approver_details["approverName"] = row[1]
            elif "Approver TM ID:" in row[0]:
                approver_details["TMID"] = row[1]
            elif "Approval Title:" in row[0]:
                approver_details["title"] = row[1]

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
                invoice_number = row[1]  # Extract invoice number
                invoice_data["invoice#"] = invoice_number

            # Extract GST and Grand Total
            elif "GST:" in row[0]:
                invoice_data["GST"] = row[1]
            elif "Grand Total:" in row[0]:
                invoice_data["grandTotal"] = row[1]

            # Extract approver details
            elif "Approver Name:" in row[0]:
                approver_details["approverName"] = row[1]
            elif "Approver TM ID:" in row[0]:
                approver_details["TMID"] = row[1]
            elif "Approval Title:" in row[0]:
                approver_details["title"] = row[1]

            # Check if header matches expected columns
            elif row == ["Store", "Dept", "Account", "SL", "Project", "Activity", "Amount"]:
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
                    invoice_data["amount"].append(data_row[6])  # Ensure 'Amount' remains

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

# # Example usage with parsed_data
# template_type = identify_template(parsed_data)
# print(f"The identified template is: {template_type}")


def replace_cid_links(html, attachments):
    def cid_to_data_uri(match):
        cid = match.group(1).split("@")[0]  # Extract the base filename before '@'
        for filename, data in attachments.items():
            if filename == cid:
                mime_type = "image/png" if filename.endswith(".png") else "image/jpeg"
                encoded_data = base64.b64encode(data).decode("utf-8")
                # print(f"Replacing CID {cid} with data URI for {filename}")
                return f"data:{mime_type};base64,{encoded_data}"
        # print(f"CID {cid} not found in attachments")
        return match.group(0)  # Leave the original cid: link if no match is found

    return re.sub(r'cid:([^"\'\s]+)', cid_to_data_uri, html)

def convert_eml_to_encoded_image(email_msg):
    
    config = imgkit.config(wkhtmltoimage=r"C:\Program Files\wkhtmltopdf\bin\wkhtmltoimage.exe")
    # Step 1: Extract and process HTML content
    html_content = email_msg.get_body(preferencelist=('html', 'plain')).get_content()

    # # Debug: Extract all cid: links in the HTML
    # cid_links = re.findall(r'cid:([^"\'\s]+)', html_body)
    # print("CID references in HTML:", cid_links)
    # Step 2: Extract attachments
    attachments = {}
    for attachment in email_msg.iter_attachments():
        filename = attachment.get_filename()
        if filename:
            attachments[filename] = attachment.get_payload(decode=True)

    # Step 3: Replace CID links in the HTML body with base64-encoded data URIs
    html_body = replace_cid_links(html_content, attachments)
    # Step 1: Remove all `cid:` references
    def remove_cid_references(html):
        cleaned_html = re.sub(r'cid:[^"\'\s]+', '', html)
        return cleaned_html

    cleaned_html = remove_cid_references(html_body)
    # # Step 5: Optional - Save the updated HTML to a file for verification
    # with open(input_html_path, "w", encoding="utf-8") as file:
    #     file.write(cleaned_html)

    # Step 6: Convert HTML to PNG and get base64 string
    options = {"format": "png", "quality": "90"}
    image_data = imgkit.from_string(cleaned_html, False, config=config, options=options)

    # Encode the binary image data in base64
    encoded_image = base64.b64encode(image_data).decode("ascii")
    return encoded_image


def dynamic_split_and_convert_to_pdf(encoded_image, eml_file_path, output_dir):
    """
    Dynamically splits an image based on its height and converts it to a PDF.
    The output file name is based on the .eml file's base name.

    :param encoded_image: Base64-encoded string of the input PNG image.
    :param eml_file_path: Path to the original .eml file.
    :param output_dir: Directory where the output PDF will be saved.
    """
    try:
        # Extract base name from .eml file (without extension)
        eml_base_name = os.path.splitext(os.path.basename(eml_file_path))[0]
        output_pdf = os.path.join(output_dir, f"{eml_base_name}_output.pdf")
        # output_prefix = os.path.join(output_dir, eml_base_name)

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

        print(f"Image height: {height}, Splitting into {n_splits} pages.")

        # Calculate the height of each split
        split_height = height // n_splits

        # List to hold images for the PDF
        images_for_pdf = []

        # Split the image and save each part
        for i in range(n_splits):
            upper = i * split_height
            lower = (i + 1) * split_height if i < n_splits - 1 else height
            cropped_img = img.crop((0, upper, width, lower))
            
            # # Save each split as a PNG
            # output_file = f"{output_prefix}_part_{i+1}.png"
            # cropped_img.save(output_file)
            # print(f"Saved: {output_file}")
            
            # Append to PDF list (convert to RGB if needed)
            images_for_pdf.append(cropped_img.convert("RGB"))

        # Save images as a single PDF
        if images_for_pdf:
            images_for_pdf[0].save(output_pdf, save_all=True, append_images=images_for_pdf[1:])
            print(f"PDF saved: {output_pdf}")

    except Exception as e:
        print(f"An error occurred: {e}")
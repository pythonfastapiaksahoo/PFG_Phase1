import json
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
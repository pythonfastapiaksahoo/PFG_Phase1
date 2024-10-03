import concurrent.futures
import time
from io import BytesIO

from azure.storage.blob import BlobServiceClient
from pypdf import PdfWriter

from pfg_app import settings
from pfg_app.core.azure_fr import call_form_recognizer
from pfg_app.core.stampData import stampDataFn
from pfg_app.core.utils import get_credential
from pfg_app.logger_module import logger


def group_pages(data):
    grouped_pages = []
    current_group = []

    for page_num, page_data in data.items():
        # Extract confidence levels
        invoice_id_confidence = page_data.get("InvoiceId", ["", 0])[1]
        invoice_date_confidence = page_data.get("InvoiceDate", ["", 0])[1]
        subtotal_confidence = page_data.get("SubTotal", ["", 0])[1]

        # Determine if a new group should start
        if invoice_id_confidence > 90 and invoice_date_confidence > 90:
            # If there's an ongoing group, add it to the grouped_pages
            if current_group:
                grouped_pages.append(current_group)
            # Start a new group with the current page
            current_group = [page_num]
        elif subtotal_confidence > 90:
            # If SubTotal has high confidence, add page to the current group
            current_group.append(page_num)
        else:
            # If confidence is low, continue adding the page to the current
            # group
            current_group.append(page_num)

    if current_group:
        grouped_pages.append(current_group)

    return grouped_pages


def split_pdf_and_upload(
    pdf,
    ranges,
    destination_container_name,
    subfolder_name,
    prompt,
    # deployment_name,
    # OpenAI_api_base,
    # OpenAI_api_key,
    # openAI_api_version,
):
    """Splits a PDF file into multiple parts based on the provided ranges and
    uploads each part to Azure Blob Storage."""
    splitfileNames = []
    stampdata = []

    reader = pdf
    # Iterate over the ranges and create separate PDF files for each range
    for i, (start, end) in enumerate(ranges):
        # writer = pypdf.PdfWriter()
        writer = PdfWriter()
        for page_num in range(start, end + 1):
            writer.add_page(reader.pages[page_num - 1])

        # Save to a BytesIO object instead of a file
        output_pdf_stream = BytesIO()
        writer.write(output_pdf_stream)
        output_pdf_stream.seek(0)
        # Get the current time in seconds since the epoch
        current_timestamp = str(time.time()).replace(".", "_")

        # Define the blob name (including the subfolder path)
        output_blob_name = f"{subfolder_name}/{current_timestamp}_split_part{i + 1}.pdf"
        account_name = settings.storage_account_name
        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=get_credential()
        )
        destination_blob_client = blob_service_client.get_blob_client(
            container=destination_container_name, blob=output_blob_name
        )
        splitfileNames.append(output_blob_name)

        # Upload the modified PDF to the destination container
        destination_blob_client.upload_blob(output_pdf_stream, overwrite=True)
        logger.info(
            f"Uploaded PDF to {output_blob_name} in container \
                {destination_container_name}"
        )
        time.sleep(1)

        stp_blb_data = output_pdf_stream.getvalue()
        # TODO below uses the stampData function from core.stampData
        pgstampdata = stampDataFn(
            stp_blb_data,
            prompt,
            # deployment_name,
            # OpenAI_api_base,
            # OpenAI_api_key,
            # openAI_api_version,
        )

        stampdata.append(pgstampdata)

    return splitfileNames, stampdata


def extract_pdf_pages(pdf_document):
    """Extracts pages from a PDF file as binary streams."""

    pdf_pages = []
    for page_no in range(len(pdf_document.pages)):
        # Create a new PdfWriter for each page if you need separate
        # files/streams per page
        writer = PdfWriter()

        # Add the current page to the writer
        writer.add_page(pdf_document.pages[page_no])

        # Write the content to a BytesIO object
        tmp = BytesIO()
        writer.write(tmp)

        # Get the byte data of the page
        data = tmp.getvalue()

        # Append the byte data to the list
        pdf_pages.append(data)

    return pdf_pages


def splitDoc(
    pdf,
    subfolder_name,
    destination_container_name,
    prompt,
    fr_endpoint,
    fr_api_version,
):

    logger.info(f"num_pages: {len(pdf.pages)}")

    pdf_pages = extract_pdf_pages(pdf)

    # Use ThreadPoolExecutor to process the pages concurrently
    output_data = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(call_form_recognizer, page, fr_endpoint, fr_api_version)
            for page in pdf_pages
        ]

        # Collect the results as they complete
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                output_data.append(result)
            except Exception as e:
                print(f"Error processing a page: {e}")

    # # Get the list of Invoice IDs from the Form Recognizer results
    # # TODO continue this refactor later
    # invoice_ids = [result["analyzeResult"]["documents"][
    #             0]["fields"]["InvoiceId"]["content"] for result in output_data]

    pageInvoDate = {}

    pageInvoDate = {}
    try:
        for i in range(len(output_data)):
            pageInvoDate[i + 1] = output_data[i]["documents"][0]["fields"]["InvoiceId"][
                "content"
            ]
    except Exception as er:
        logger.info(f"Exception splitDoc line 261: {er}")
    grouped_dict = {}
    sndChk = 0
    for key, value in pageInvoDate.items():
        if value not in grouped_dict:
            grouped_dict[value] = []
        grouped_dict[value].append(key)

    input_list = list(grouped_dict.values())
    output_list = [
        (item[0], item[1]) if len(item) == 2 else (item[0], item[0])
        for item in input_list
    ]
    logger.info(f"input_list:  {output_list}")

    pgVal = []
    for pgvl in grouped_dict:
        pgVal.append(grouped_dict[pgvl][0])
        # print(grouped_dict[pgvl])
    if "" in grouped_dict:
        # print(grouped_dict[''])
        pgVal = list(set(pgVal + grouped_dict[""]))
    if len(pgVal) == len(output_data):
        sndChk = 1
    # if sndChk == 1:
    prbtHeaders = {}
    rwOcrData = []
    # print(len(output_data))

    for docPg in range(len(output_data)):
        if docPg == 10:
            print(output_data[docPg])
        try:
            rwOcrData.append(output_data[docPg]["content"])
        except Exception as er:
            logger.error(f"Exception splitdoc line 291: {er}")

        preDt = output_data[docPg]["documents"][0]["fields"]
        prbtHeaderspg = {}
        for pb in preDt:
            # logger.info(f"line 297: {preDt[pb]}")
            if "type" in preDt[pb] and preDt[pb]["type"] != "array":
                prbtHeaderspg[pb] = [
                    preDt[pb]["content"],
                    round(float(preDt[pb]["confidence"]) * 100, 2),
                ]
        prbtHeaders[docPg] = prbtHeaderspg
        # print(
        #     "/n-----------------------------------------------------------------------------------------------------------------------------------------")
    # logger.info(f"headerData: {prbtHeaderspg}")
    # print(
    #     "-----------------------------------------------------------------------------------------------------------------------------------------\n\n\n")
    if sndChk == 1:
        grouped_pages = group_pages(prbtHeaders)

        splitpgsDt = [
            (x[0] + 1, x[0] + 1) if len(x) == 1 else tuple(y + 1 for y in x)
            for x in grouped_pages
        ]

        grp_pages = splitpgsDt
        splitfileNames, stampData = split_pdf_and_upload(
            pdf,
            splitpgsDt,
            destination_container_name,
            subfolder_name,
            prompt,
            # deployment_name,
            # OpenAI_api_base,
            # OpenAI_api_key,
            # openAI_api_version,
        )

    else:

        grp_pages = output_list
        splitfileNames, stampData = split_pdf_and_upload(
            pdf,
            output_list,
            destination_container_name,
            subfolder_name,
            prompt,
            # deployment_name,
            # OpenAI_api_base,
            # OpenAI_api_key,
            # openAI_api_version,
        )

    return (
        prbtHeaders,
        grp_pages,
        splitfileNames,
        len(pdf.pages),
        stampData,
        output_data,
        1,  # Means success (fr_model_status)
        "success",  # fr_model_msg
    )

import concurrent.futures

# import json
import time
import traceback
from datetime import date, datetime
from io import BytesIO

from azure.storage.blob import BlobServiceClient
from pypdf import PdfWriter
from rapidfuzz import fuzz

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
    fileSize={},
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
        # get PDF size
        try:
            # Get the size of the PDF in bytes
            pdf_size_bytes = output_pdf_stream.getbuffer().nbytes
            pdf_size_kb = round(pdf_size_bytes / 1024, 2)
            pdf_size_mb = round(pdf_size_kb / 1024, 2)  # Convert to MB
            fileSize[output_blob_name] = pdf_size_mb

        except Exception as e:
            logger.error(f"Exception in fileSize: {str(e)}")
            logger.error(f"{traceback.format_exc()}")

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

    return splitfileNames, stampdata, fileSize


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


def serialize_dates(item):
    if isinstance(item, dict):
        return {k: serialize_dates(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [serialize_dates(i) for i in item]
    elif isinstance(item, (datetime, date)):
        return item.isoformat()
    else:
        return item


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
    output_data_dt = {}
    output_data = []
    pg_cnt = 1
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(call_form_recognizer, page, fr_endpoint, fr_api_version)
            for page in pdf_pages
        ]

        # Collect the results in the order they were scheduled
        for future in futures:
            try:
                result = future.result()
                output_data_dt[pg_cnt] = result
                output_data.append(result)
                pg_cnt = pg_cnt + 1

            except Exception as e:
                logger.error(f"Error processing a page: {e}")
                logger.error(f"{traceback.format_exc()}")

    pageInvoData = {}
    # data_serialized = serialize_dates(output_data)
    # with open("data.json", "w") as f:
    #     json.dump(data_serialized, f, indent=4)

    #
    try:
        pageInvoVendorData = {}
        for i in output_data_dt.keys():
            if "InvoiceId" in output_data_dt[i]["documents"][0]["fields"]:
                if (
                    "content"
                    in output_data_dt[i]["documents"][0]["fields"]["InvoiceId"]
                ):
                    invoId = output_data_dt[i]["documents"][0]["fields"]["InvoiceId"][
                        "content"
                    ]
                    if (
                        "confidence"
                        in output_data_dt[i]["documents"][0]["fields"]["InvoiceId"]
                    ):
                        try:
                            invoConf = output_data_dt[i]["documents"][0]["fields"][
                                "InvoiceId"
                            ]["confidence"]
                        except Exception:
                            invoConf = 0.0
                    else:
                        invoConf = 0.0
                else:
                    invoId = ""
                    invoConf = 0.0
            else:
                invoId = ""
                invoConf = 0.0

            if "VendorName" in output_data_dt[i]["documents"][0]["fields"]:
                if (
                    "content"
                    in output_data_dt[i]["documents"][0]["fields"]["VendorName"]
                ):
                    vndrName = output_data_dt[i]["documents"][0]["fields"][
                        "VendorName"
                    ]["content"]
                    if (
                        "confidence"
                        in output_data_dt[i]["documents"][0]["fields"]["VendorName"]
                    ):
                        try:
                            VndrNmConf = output_data_dt[i]["documents"][0]["fields"][
                                "VendorName"
                            ]["confidence"]
                        except Exception:
                            VndrNmConf = 0.0
                    else:
                        VndrNmConf = 0.0
                else:
                    VndrNmConf = 0.0
                    vndrName = ""
            else:
                VndrNmConf = 0.0
                vndrName = ""

            pageInvoVendorData[i] = {
                "InvoiceId": [invoId, invoConf],
                "VendorName": [vndrName, VndrNmConf],
            }
    except Exception as er:
        logger.error(f"{traceback.format_exc()}")
        logger.info(f"Exception splitDoc line 261: {er}")
    vendor_names = []
    for vrdNm in pageInvoVendorData:
        vendor_names.append(pageInvoVendorData[vrdNm]["VendorName"][0])
    reference_name = vendor_names[0]
    threshold = 80
    same_vendor = all(
        fuzz.token_set_ratio(reference_name, name) > threshold for name in vendor_names
    )

    if same_vendor:

        try:
            for i in range(len(output_data)):
                pageInvoData[i + 1] = output_data[i]["documents"][0]["fields"][
                    "InvoiceId"
                ]["content"]
        except Exception as er:
            logger.info(f"Exception splitDoc line 261: {er}")
        grouped_dict = {}
        sndChk = 0
        for key, value in pageInvoData.items():
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

            try:
                rwOcrData.append(output_data[docPg]["content"])
            except Exception as er:
                logger.error(f"Exception splitdoc line 291: {er}")

            preDt = output_data[docPg]["documents"][0]["fields"]
            prbtHeaderspg = {}
            for pb in preDt:
                # logger.info(f"line 297: {preDt[pb]}")
                if (
                    "value_type" in preDt[pb]
                    and preDt[pb]["value_type"] != "array"
                    and pb != "Items"
                ):
                    try:
                        prbtHeaderspg[pb] = [
                            preDt[pb]["content"],
                            round(float(preDt[pb]["confidence"]) * 100, 2),
                        ]
                    except Exception:
                        prbtHeaderspg[pb] = [preDt[pb]["content"], 0.0]

            prbtHeaders[docPg] = prbtHeaderspg
        if len(output_data) == 1:
            split_list = [(1, 1)]

        elif sndChk == 1:
            try:
                split_list =  [(x[0], x[1]) if len(x) == 2 else (x[0], x[0]) for x in output_list]    # noqa: E501
            except Exception:
                logger.error(f"{traceback.format_exc()}")
                grouped_pages = group_pages(prbtHeaders)

                splitpgsDt = [
                    (x[0] + 1, x[0] + 1) if len(x) == 1 else tuple(y + 1 for y in x)
                    for x in grouped_pages
                ]

                if len(splitpgsDt) == 1 and isinstance(splitpgsDt[0], tuple):
                    # Unpack the tuple and create a list of tuples (n, n)
                    grp_pages = [(i, i) for i in splitpgsDt[0]]
                elif all(isinstance(i, tuple) for i in splitpgsDt):
                    # If the input is already a list of tuples, return it as-is
                    grp_pages = splitpgsDt
                else:
                    grp_pages = splitpgsDt
                split_list = grp_pages
        else:
            split_list = output_list
    else:

        prbtHeaders = {}
        rwOcrData = []
        # print(len(output_data))

        for docPg in range(len(output_data)):

            try:
                rwOcrData.append(output_data[docPg]["content"])
            except Exception:
                logger.error(f"{traceback.format_exc()}")

            preDt = output_data[docPg]["documents"][0]["fields"]
            prbtHeaderspg = {}
            for pb in preDt:
                if "content" in preDt[pb]:
                    try:
                        # logger.info(f"line 297: {preDt[pb]}")
                        if (
                            "value_type" in preDt[pb]
                            and preDt[pb]["value_type"] != "array"
                            and pb != "Items"
                            and pb != "list"
                            and preDt[pb]["value_type"] == "string"
                        ):
                            if "confidence" in preDt[pb]:
                                try:
                                    prbtHeaderspg[pb] = [
                                        preDt[pb]["content"],
                                        round(float(preDt[pb]["confidence"]) * 100, 2),
                                    ]
                                except Exception:
                                    prbtHeaderspg[pb] = [
                                        preDt[pb]["content"],
                                        0.0,
                                    ]
                    except Exception:
                        logger.info(f"line 300: {[pb]}")
                        logger.error(f"{traceback.format_exc()}")
            prbtHeaders[docPg] = prbtHeaderspg

        #
        if len(output_data) == 1:
            split_list = [(1, 1)]
        else:
            spltLtmain = []
            nwPg = 0

            groupInvo = {}
            cnt_dt = 0
            tmpLt = []
            for inv, data in pageInvoVendorData.items():
                if cnt_dt ==0:
                    nwPg = 1
                    cnt_dt = 1
                    tmpLt.append(inv)
                    continue
                    
                if cnt_dt !=0:
                    crtInv = pageInvoVendorData[inv]['InvoiceId']
                    
                    prvInv = pageInvoVendorData[inv-1]['InvoiceId']
                    

                    if prvInv[0]==crtInv[0]:
                        #same invoice
                        nwPg = 0
                        tmpLt.append(inv)
                    else:
                        if crtInv[1] >= 0.90:
                            #new page
                            spltLtmain.append(tmpLt)
                            
                            tmpLt= []
                            tmpLt.append(inv)
                            nwPg = 1
                        # else crtInv[1] <0.90 and (crtVdr[0]=='' or crtVdr[1]<70):
                        else: 
                            #same page

                            tmpLt.append(inv)
                groupInvo[inv] = nwPg
                    
            spltLtmain.append(tmpLt)


            # Transform each sublist to the desired tuple format
            split_list = [(item[0], item[-1]) for item in spltLtmain]

        # grouped_invoices = {}
        # previous_invoice = ""
        # for k, v in pageInvoVendorData.items():
        #     current_invoice = v["InvoiceId"][0]
        #     if v["InvoiceId"][1] > 0.89 and v["VendorName"][1] > 0.80:
        #         if current_invoice not in grouped_invoices:
        #             grouped_invoices[current_invoice] = [k]
        #         else:
        #             grouped_invoices[current_invoice].append(k)
        #         previous_invoice = current_invoice
        #     else:
        #         if previous_invoice not in grouped_invoices:
        #             continue
        #         grouped_invoices[previous_invoice].append(k)
        # split_list = []
        # for pg0, pg1 in grouped_invoices.items():
        #     split_list.append((min(pg1), max(pg1)))

    logger.info(f"split_list: {split_list}")
    splitfileNames, stampData, fileSize = split_pdf_and_upload(
        pdf,
        split_list,
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
        split_list,
        splitfileNames,
        len(pdf.pages),
        stampData,
        output_data,
        1,
        "success",
        fileSize,
    )

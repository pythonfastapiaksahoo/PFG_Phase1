import json
from io import BytesIO

from azure.storage.blob import BlobServiceClient
from PIL import Image
from pypdf import PdfReader, PdfWriter

from pfg_app import settings
from pfg_app.core.utils import get_credential
from pfg_app.logger_module import logger

accepted_inch = 10 * 72
accepted_pixel_max = 8000
accepted_pixel_min = 50
accepted_filesize_max = 50


def get_binary_data(file_type, spltFileName, container):
    global accepted_inch, accepted_pixel_max, accepted_pixel_min, accepted_filesize_max
    try:
        # resp = requests.get(file_path)
        logger.info(f"spltFileName prepro 282: {spltFileName}")
        logger.info(f"container: {container}")
        spltFileName = spltFileName.replace("//", "/")
        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(
            account_url=account_url,
            credential=get_credential(),
        )
        blob_client = blob_service_client.get_blob_client(
            container=container, blob=spltFileName
        )
        download_stream = blob_client.download_blob()
        blob_data = download_stream.readall()
        pdf = PdfReader(BytesIO(blob_data))

        first_page = pdf.pages[0]
        dimension = first_page.mediabox
        num_pages = len(pdf.pages)

        inputdata_list = []
        if file_type == "pdf":
            # pdf = PdfFileReader(input_data, strict=False)
            # dimention = pdf.getPage(0).mediaBox
            dimension = pdf.pages[0].mediabox
            # num_pages = pdf.getNumPages()
            num_pages = len(pdf.pages)
            for page_no in range(num_pages):
                writer = PdfWriter()
                # page = pdf.get_page(page_no)
                page = pdf.pages[page_no]
                if max(dimension.width, dimension.height) > accepted_inch:
                    page.scale_by(
                        accepted_inch / max(int(dimension.width), int(dimension.height))
                    )
                writer.add_page(page)
                tmp = BytesIO()
                writer.write(tmp)
                data = tmp.getvalue()
                inputdata_list.append(data)
        else:
            img = Image.open(blob_data)
            w, h = img.size
            if w <= accepted_pixel_min or h <= accepted_pixel_min:
                # Discard this due to low quality
                return False, f"File is below {accepted_pixel_min}"
            elif w >= accepted_pixel_max or h >= accepted_pixel_max:
                """# resize image"""
                factor = accepted_pixel_max / max(img.size[0], img.size[1])
                img.thumbnail(
                    (int(img.size[0] * factor), int(img.size[1] * factor)),
                    Image.ANTIALIAS,
                )

            byte_io = BytesIO()
            format = "PNG" if file_type == "png" else "JPEG"
            img.save(byte_io, format)
            data = byte_io.getvalue()
            inputdata_list.append(data)

        get_binary_status = 1
        get_binary_msg = " Document pg cnt : " + str(len(inputdata_list))
        if len(inputdata_list) > 0:
            logger.info(
                f"type(inputdata_list[0]) frm preprocessing: {type(inputdata_list[0])}"
            )

    except Exception as e:
        get_binary_status = 0
        get_binary_msg = str(e)
        logger.info(f"in binary exception: {str(e)}")
        inputdata_list = []
    return inputdata_list, get_binary_status, get_binary_msg


def fr_preprocessing(
    vendorAccountID,
    entityID,
    file_path,
    accepted_file_type,
    file_size_accepted,
    filename,
    spltFileName,
    container,
    db,
):
    fr_preprocessing_status_msg = ""
    fr_preprocessing_data = []
    fr_preprocessing_status = 0
    try:
        file_exists_status = 1
        file_exists_msg = "skip"
        if file_exists_status == 1:
            check_filetype_status = 1
            check_filetype_msg = "skip"
            if check_filetype_status == 1:
                fl_sts_msg = "skip"
                fl_status = 1
                logger.info(f"filename: {filename.lower().split('.')[-1]}")
                if fl_status == 1:
                    input_data, get_binary_status, get_binary_msg = get_binary_data(
                        filename.lower().split(".")[-1], spltFileName, container
                    )
                    logger.info(f"input_data: {len(input_data)}")

                    if get_binary_status == 1:
                        if len(input_data) > 0:
                            fr_preprocessing_data = input_data
                            fr_preprocessing_status = 1
                            fr_preprocessing_msg = "Loaded Binary data"
                            fr_preprocessing_status_msg = json.dumps(
                                {"percentage": 25, "status": "Pre-Processing ⏳"}
                            )
                        else:
                            fr_preprocessing_msg = "Binary data issue"
                            fr_preprocessing_status = 0
                    else:
                        fr_preprocessing_msg = get_binary_msg
                        fr_preprocessing_status = get_binary_status

                else:
                    fr_preprocessing_status = fl_status
                    fr_preprocessing_msg = fl_sts_msg
            else:
                fr_preprocessing_status = check_filetype_status
                fr_preprocessing_msg = check_filetype_msg
        else:
            fr_preprocessing_status = file_exists_status
            fr_preprocessing_msg = file_exists_msg
    except Exception as r:
        fr_preprocessing_status = 0
        fr_preprocessing_msg = "Error in preprocessing: " + str(r)
    return (
        fr_preprocessing_status,
        fr_preprocessing_msg,
        fr_preprocessing_data,
        fr_preprocessing_status_msg,
    )

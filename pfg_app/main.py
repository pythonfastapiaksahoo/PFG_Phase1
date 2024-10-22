import os
import tempfile
import traceback

from azure.data.tables import TableServiceClient
from azure.storage.blob import BlobServiceClient
from core.stampData import stampDataFn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pypdf import PdfReader, PdfWriter
from sqlalchemy import create_engine

from pfg_app import settings
from pfg_app.core.utils import get_credential, get_secret_from_vault
from pfg_app.logger_module import logger
from pfg_app.routers import (  # maillistener,
    FR,
    OCR,
    ERPIntegrationapi,
    batchexception,
    common,
    invoice,
    modelonboarding,
    vendor,
)

# from opencensus.ext.azure.trace_exporter import AzureExporter
# from opencensus.trace import execution_context
# from opencensus.trace.propagation.trace_context_http_header_format import (
#     TraceContextPropagator,
# )


# dependencies=[Depends(get_query_token)])
app = FastAPI(
    title="IDP",
    version="1.0",
    swagger_ui_oauth2_redirect_url="/oauth2-redirect",
    swagger_ui_init_oauth={
        "usePkceWithAuthorizationCodeGrant": True,
        "clientId": settings.swagger_ui_client_id,
        "scopes": [f"api://{settings.api_client_id}/access_as_user"],
    },
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# # Set up tracing
# exporter = AzureExporter(
#     connection_string=settings.application_insights_connection_string
# )
# propagator = TraceContextPropagator()


# @app.middleware("http")
# async def add_tracing(request: Request, call_next):
#     context = propagator.from_headers(request.headers)

#     # Set the trace context for the current request
#     tracer = execution_context.get_opencensus_tracer()
#     trace_id = context.trace_id or "00000000000000000000000000000000"
#     tracer.span_context.trace_id = trace_id
#     tracer.span_context.span_id = context.span_id
#     tracer.exporter = exporter

#     # Process the request and get the response
#     with tracer.span(name=request.url.path):
#         response = await call_next(request)

#     # Add the trace_id to the response headers
#     response.headers["X-Trace-Id"] = trace_id

#     return response


# Define routers in the main
app.include_router(vendor.router)
app.include_router(invoice.router)
app.include_router(FR.router)
app.include_router(OCR.router)
app.include_router(modelonboarding.router)
app.include_router(ERPIntegrationapi.router)
app.include_router(batchexception.router)
app.include_router(common.router)


@app.on_event("startup")
async def app_startup():
    logger.warning(
        "App Startup is called",
    )
    # # Load the files as BytesIO from TestData
    # import os, tempfile
    # from io import BytesIO
    # from pypdf import PdfReader, PdfWriter

    # for pdf in os.listdir("TestData"):
    #     data_list = []
    #     # if the pdf startes with 12 set the model_type as custom else prebuilt
    #     if pdf.startswith("12"):
    #         model_type = "custom"
    #     else:
    #         model_type = "prebuilt"
    #     reader = PdfReader("TestData/"+pdf)
    #     number_of_pages = len(reader.pages)
    #     for i in range(number_of_pages):
    #         page = reader.pages[i]
    #         page_writer = PdfWriter()
    #         page_writer.add_page(page)
    #         # append the page as BytesIO object
    #         with tempfile.NamedTemporaryFile() as temp_file:
    #             page_writer.write(temp_file)
    #             temp_file.seek(0)

    #             data_list.append(temp_file.read())
    #         # call azure fr
    #     from core.azure_fr import get_fr_data
    #     from pfg_app import settings
    #     result = get_fr_data(
    #         inputdata_list=data_list,
    #         API_version=settings.api_version,
    #         endpoint=settings.form_recognizer_endpoint,
    #         model_type=model_type,
    #         inv_model_id="Chuckleberry_Community_Farm_Temp_2"
    #     )
    #     # logger.info(result)
    #     logger.info('completed')

    #     # call stampData only if the model_type is custom with the
    #     # first page as blob_data
    #     if model_type == "custom" and len(data_list) > 0:
    #         from core.stampData import stampDataFn
    #         prompt = '''This is an invoice document. It may contain a receiver's
    # stamp and might have inventory or supplies marked or circled with a pen,
    # circled is selected. It contains store number as "STR #"
    #     InvoiceDocument: Yes/No InvoiceID: [InvoiceID]. StampPresent: Yes/No.
    # If a stamp is present, identify any markings on the document related to
    #     Inventory or Supplies, specifically if they are marked or circled with a pen.
    # If a stamp is present, extract the following handwritten details
    #     from the stamp: ConfirmationNumber (the confirmation number labeled as
    # 'Confirmation' on the stamp), ReceivingDate
    # (the date when the goods were received),
    #     Receiver (the name of the person or department who received the goods),
    # and Department (the department name or code, which may be either 'Inventory'
    # or 'Supplies',
    #     or another specified department). Provide all information in the following
    # JSON format: {'StampFound': 'Yes/No', 'MarkedDept': 'Inventory/Supplies'
    # (which ever is circled more/marked only),
    #     'Confirmation': 'Extracted data', 'ReceivingDate': 'Extracted data',
    # 'Receiver': 'Extracted data', 'Department': 'Dept code','Store Number':,
    # 'VendorName':}.Output should be just json'''
    #         result = stampDataFn(data_list[0], prompt)
    #         logger.info(result)


@app.get("/")
async def root(request: Request):
    # get the domain name
    domain = request.url.hostname
    logger.info(f"Root endpoint was accessed - {domain}")

    try:
        # connect to the database

        # Replace these variables with your actual database credentials
        username = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        database = os.getenv("DB_NAME")

        # Create the connection string
        connection_string = (
            f"postgresql://{username}:{password}@{host}:{port}/{database}"
        )

        # Create the engine
        engine = create_engine(connection_string)

        # Test the connection
        with engine.connect() as connection:
            result = connection.execute("SELECT 1")
            logger.info(f"Result of DB Connection {result.fetchone()}")
    except Exception:
        logger.error(f"Main.py-ROOT error: {traceback.format_exc()}")
        return {"message": "Error connecting to the database"}

    # check if the key vault is accessible
    try:
        if settings.build_type not in ["debug"]:
            credential = get_credential()
            api_client_id = get_secret_from_vault(
                credential, "APPORTAL-API-CLIENT-ID", "api_client_id"
            )
            logger.info(f"API Client ID: {api_client_id}")
    except Exception:
        logger.error(f"Main.py-ROOT error: {traceback.format_exc()}")
        return {"message": "Error connecting to the key vault"}

    # check if blob is accessible
    try:
        # try to create a container client and delete it
        # Get the credential
        credential = get_credential()

        account_url = f"https://{settings.storage_account_name}.blob.core.windows.net"
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=credential
        )

        # create a container client
        container_name = "test-container"
        # create the container
        blob_service_client.create_container(container_name)

        container_client = blob_service_client.get_container_client(container_name)

        # create a blob client
        blob_path = "test-blob.txt"
        blob_client = container_client.get_blob_client(blob_path)

        # upload a blob
        blob_client.upload_blob("test", overwrite=True)

        # delete the blob
        blob_client.delete_blob()

        # delete the container
        container_client.delete_container()

    except Exception:
        logger.error(f"Main.py-ROOT error: {traceback.format_exc()}")
        return {"message": "Error accessing the blob storage"}

    # check if table storage is accessible
    try:
        # try to create a table client and delete it
        # Get the credential
        credential = get_credential()

        account_url = f"https://{settings.storage_account_name}.table.core.windows.net"
        # Create a BlobServiceClient
        table_service_client = TableServiceClient(
            account_url=account_url, credential=credential
        )

        # create a table client
        table_name = "test-table"
        table_client = table_service_client.get_table_client(table_name)

        # create a table
        table_client.create_table()

        # delete the table
        table_client.delete_table()

    except Exception:
        logger.error(f"Main.py-ROOT error: {traceback.format_exc()}")
        return {"message": "Error accessing the table storage"}

    # check if the Azure Document intelligence is accessible
    try:

        # Load the files as BytesIO from TestData
        reader = PdfReader("TestData/Chuckleberry Community Farm.pdf")

        page_writer = PdfWriter()
        page_writer.add_page(reader.pages[0])
        # append the page as BytesIO object
        with tempfile.NamedTemporaryFile() as temp_file:
            page_writer.write(temp_file)
            temp_file.seek(0)

        from core.azure_fr import call_form_recognizer

        # call the call_form_recognizer function
        result = call_form_recognizer(
            input_file=temp_file.read(),
            endpoint=settings.form_recognizer_endpoint,
            api_version=settings.api_version,
        )

        logger.info(result)
    except Exception:
        logger.error(f"Main.py-ROOT error: {traceback.format_exc()}")
        return {"message": "Error accessing the Azure Document Intelligence service"}

    # call the Azure OpenAI service
    try:
        # Load the files as BytesIO from TestData
        reader = PdfReader("TestData/Chuckleberry Community Farm.pdf")
        page_writer = PdfWriter()
        page_writer.add_page(reader.pages[0])
        # append the page as BytesIO object
        with tempfile.NamedTemporaryFile() as temp_file:
            page_writer.write(temp_file)
            temp_file.seek(0)

        prompt = """This is an invoice document. It may contain a receiver's stamp and
        might have inventory or supplies marked or circled with a pen, circled
        is selected. It contains store number as "STR #".

        InvoiceDocument: Yes/No
        InvoiceID: [InvoiceID].
        StampPresent: Yes/No.

        If a stamp is present, identify any markings on the document related to
        Inventory or Supplies, specifically if they are marked or circled with a pen.
        If a stamp is present, extract the following handwritten details from the
        stamp: ConfirmationNumber (the confirmation number labeled
        as 'Confirmation' on the stamp), ReceivingDate
        (the date when the goods were received), Receiver
        (the name of the person or department who received the goods),
        and Department (the handwritten department name or code,
        or another specified department name),
        MarkedDept (which may be either 'Inventory' or 'Supplies',
        based on pen marking).
        Extract the Invoice Number.
        Extract the Currency from the invoice document by identifying the currency
        symbol before the total amount. The currency can be CAD or USD.
        If the invoice address is in Canada, set the currency to CAD,
        otherwise set it to USD.

        Provide all information in the following JSON format:
        {
            'StampFound': 'Yes/No',
            'MarkedDept': 'Inventory/Supplies' (whichever is circled more/marked only),
            'Confirmation': 'Extracted data',
            'ReceivingDate': 'Extracted data',
            'Receiver': 'Extracted data',
            'Department': 'Dept code',
            'Store Number': 'Extracted data',
            'VendorName': 'Extracted data',
            'InvoiceID' : 'Extracted data'
            'Currency': 'Extracted data'
        }.

        Output should always be in above defined JSON format only."""

        # call the call_openai function
        result = stampDataFn(blob_data=temp_file.read(), prompt=prompt)

        logger.info(result)
    except Exception:
        logger.error(f"Main.py-ROOT error: {traceback.format_exc()}")
        return {"message": "Error accessing the Azure OpenAI service"}

    return {"message": "Hello! This is IDP"}

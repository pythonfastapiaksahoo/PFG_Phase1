# from model import Vendor
# some_file.py

from azuread import config
from core.config import settings
from fastapi import FastAPI, Request

# from dependency.dependencies import get_query_token, get_token_header
from fastapi.middleware.cors import CORSMiddleware
from logger_module import logger
from opencensus.ext.azure.trace_exporter import AzureExporter
from opencensus.trace import execution_context
from opencensus.trace.propagation.trace_context_http_header_format import (
    TraceContextPropagator,
)
from routers import (  # maillistener,
    FR,
    OCR,
    ERPIntegrationapi,
    VendorPortal,
    batchexception,
    invoice,
    modelonboarding,
    summary,
    vendor,
)

# dependencies=[Depends(get_query_token)])
app = FastAPI(
    title="IDP",
    version="1.0",
    swagger_ui_oauth2_redirect_url="/oauth2-redirect",
    swagger_ui_init_oauth={
        "usePkceWithAuthorizationCodeGrant": True,
        "clientId": config.SWAGGER_UI_CLIENT_ID,
        "scopes": [f"api://{config.API_CLIENT_ID}/access_as_user"],
    },
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Set up tracing
exporter = AzureExporter(
    connection_string=settings.application_insights_instrumentation_key
)
propagator = TraceContextPropagator()


@app.middleware("http")
async def add_tracing(request: Request, call_next):
    context = propagator.from_headers(request.headers)

    # Set the trace context for the current request
    tracer = execution_context.get_opencensus_tracer()
    trace_id = context.trace_id or "00000000000000000000000000000000"
    tracer.span_context.trace_id = trace_id
    tracer.span_context.span_id = context.span_id
    tracer.exporter = exporter

    # Process the request and get the response
    with tracer.span(name=request.url.path):
        response = await call_next(request)

    # Add the trace_id to the response headers
    response.headers["X-Trace-Id"] = trace_id

    return response


# Define routers in the main
app.include_router(vendor.router)
app.include_router(invoice.router)
app.include_router(FR.router)
app.include_router(OCR.router)
app.include_router(modelonboarding.router)
app.include_router(VendorPortal.router)
app.include_router(summary.router)
app.include_router(ERPIntegrationapi.router)
app.include_router(batchexception.router)


@app.on_event("startup")
async def app_startup():
    logger.warning(
        "App Startup is called",
    )
    # # Load the files as BytesIO from TestData
    # import os, tempfile
    # from io import BytesIO
    # from PyPDF2 import PdfReader, PdfWriter

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
    #     from core.config import settings
    #     result = get_fr_data(
    #         inputdata_list=data_list,
    #         API_version=settings.api_version,
    #         endpoint=settings.form_recognizer_endpoint,
    #         model_type=model_type,
    #         inv_model_id="Chuckleberry_Community_Farm_Temp_2"
    #     )
    #     # logger.info(result)
    #     logger.info('completed')

    #     # call stampData only if the model_type is custom with the first page as blob_data
    #     if model_type == "custom" and len(data_list) > 0:
    #         from core.stampData import stampDataFn
    #         prompt = '''This is an invoice document. It may contain a receiver's stamp and might have inventory or supplies marked or circled with a pen, circled is selected. It contains store number as "STR #"
    #     InvoiceDocument: Yes/No InvoiceID: [InvoiceID]. StampPresent: Yes/No. If a stamp is present, identify any markings on the document related to
    #     Inventory or Supplies, specifically if they are marked or circled with a pen. If a stamp is present, extract the following handwritten details
    #     from the stamp: ConfirmationNumber (the confirmation number labeled as 'Confirmation' on the stamp), ReceivingDate (the date when the goods were received),
    #     Receiver (the name of the person or department who received the goods), and Department (the department name or code, which may be either 'Inventory' or 'Supplies',
    #     or another specified department). Provide all information in the following JSON format: {'StampFound': 'Yes/No', 'MarkedDept': 'Inventory/Supplies'(which ever is circled more/marked only),
    #     'Confirmation': 'Extracted data', 'ReceivingDate': 'Extracted data', 'Receiver': 'Extracted data', 'Department': 'Dept code','Store Number':,'VendorName':}.Output should be just json'''
    #         result = stampDataFn(data_list[0], prompt)
    #         logger.info(result)


@app.get("/")
async def root():

    logger.info("Root endpoint was accessed -new change")
    return {"message": "Hello! This is IDP"}

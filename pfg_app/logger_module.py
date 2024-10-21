import logging

from opencensus.ext.azure.log_exporter import AzureLogHandler

from pfg_app import settings

# import os


# from opencensus.trace import execution_context


# class TraceIdFilter(logging.Filter):
#     def filter(self, record):
#         # Get the current tracer and retrieve the trace_id
#         tracer = execution_context.get_opencensus_tracer()
#         trace_id = (
#             tracer.span_context.trace_id
#             if tracer
#             else "00000000000000000000000000000000"
#         )

#         # Add the trace_id to the log record
#         record.trace_id = trace_id
#         record.environment = os.getenv("ENVIRONMENT", "unknown")
#         record.worker_pid = os.getpid()
#         return True


# # Configure logging
# trace_id_filter = TraceIdFilter()
# formatter = logging.Formatter(
#     "%(asctime)s - %(environment)s - \
#         Worker PID: %(worker_pid)s - %(trace_id)s - %(message)s"
# )
print(settings.application_insights_connection_string)
handler = AzureLogHandler(
    connection_string=settings.application_insights_connection_string
)
# handler.setFormatter(formatter)
# handler.addFilter(trace_id_filter)


# Console (Terminal) Log Handler
console_handler = logging.StreamHandler()
# console_handler.setFormatter(formatter)
# console_handler.addFilter(trace_id_filter)

logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

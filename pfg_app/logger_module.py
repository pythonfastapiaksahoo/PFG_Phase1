import os
from contextvars import ContextVar
from logging import DEBUG, INFO, Filter, StreamHandler, getLogger

from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace

from pfg_app import settings

logger = getLogger(__name__)
# Create OpenTelemetry tracer
tracer = trace.get_tracer(__name__)


# Custom logging filter to inject Operation ID automatically
class OperationIdFilter(Filter):
    def filter(self, record):
        # Retrieve the Operation ID
        # from the context variable or OpenTelemetry trace context
        operation_id = (
            operation_id_var.get()
            or trace.get_current_span().get_span_context().trace_id
        )
        # Convert trace_id to hexadecimal string if necessary
        if isinstance(operation_id, int):
            operation_id = format(operation_id, "032x")
        record.operation_id = operation_id
        record.worker_id = os.getpid()  # Use process ID to identify the worker
        record.msg = f"[Worker PID: {record.worker_id}] [Operation ID: {operation_id}] {record.msg}"  # noqa: E501
        return True


# Helper function to manually set Operation ID if needed (optional)
def set_operation_id(operation_id):
    operation_id_var.set(operation_id)

def get_operation_id():
    return operation_id_var.get()

if settings.build_type != "debug":
    os.environ["APPLICATIONINSIGHTS_CONNECTION_STRING"] = (
        settings.appinsights_connection_string
    )
    configure_azure_monitor(
        instrumentation=["fastapi", "requests", "sqlalchemy"]   
    )
    # Create OpenTelemetry tracer
    tracer = trace.get_tracer(__name__)

    # Context variable to store Operation ID
    operation_id_var = ContextVar("operation_id", default=None)

    # Initialize logger
    logger = getLogger(__name__)
    logger.setLevel(DEBUG)

    # Add the filter to the logger
    logger.addFilter(OperationIdFilter())

else:
    # Console (Terminal) Log Handler
    console_handler = StreamHandler()
    # Context variable to store Operation ID
    operation_id_var = ContextVar("operation_id", default=None)

    logger.addHandler(console_handler)
    logger.setLevel(INFO)
    # Add the filter to the logger
    logger.addFilter(OperationIdFilter())

__all__ = ["logger", "tracer", "set_operation_id"]
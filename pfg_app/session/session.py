import os
import traceback

from azure.identity import CredentialUnavailableError, DefaultAzureCredential
from fastapi import HTTPException
from sqlalchemy import create_engine, event
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from tenacity import retry, stop_after_attempt, wait_exponential
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry import trace
from sqlalchemy.pool import QueuePool
from pfg_app import settings
from pfg_app.core.utils import build_rfc1738_url
from pfg_app.logger_module import logger
# import urllib.parse
engine = None
Session = None
Base = None
DB = None
SQLALCHEMY_DATABASE_URL = None
SCHEMA = "pfg_schema"

SCOPE = "https://ossrdbms-aad.database.windows.net/.default"

# Initialize credential globally
try:
    credential = DefaultAzureCredential()
except Exception as e:
    logger.error(f"Failed to initialize Azure credential: {e}")
    # raise

def create_sqlalchemy_engine(url: str):
    try:
        eng = create_engine(
            url,
            poolclass=QueuePool,
            pool_size=200,
            max_overflow=10,
            pool_recycle=3600,
            pool_pre_ping = True,
        )

        @event.listens_for(eng, "do_connect")
        def inject_token(dialect, conn_rec, cargs, cparams):
            logger.info("Injecting AAD token into connection parameters")
            try:
                token = credential.get_token(SCOPE).token
                cparams["password"] = token
            except Exception as e:
                logger.error(f"Failed to retrieve AAD token: {e}")
                # raise

        return eng
    except Exception as e:
        logger.critical(f"Failed to create SQLAlchemy engine: {e}")
        # raise


try:
    if settings.build_type in ["prod", "dev"]:
        try:
            conn_string = os.getenv("AZURE_POSTGRESQL_CONNECTIONSTRING")
            if not conn_string:
                logger.error(f"AZURE_POSTGRESQL_CONNECTIONSTRING not found in environment")

            db_url, status = build_rfc1738_url(conn_string, "")  # Password will be injected
            SQLALCHEMY_DATABASE_URL = db_url + "&options=-csearch_path=pfg_schema"

            engine = create_sqlalchemy_engine(SQLALCHEMY_DATABASE_URL)
            logger.info(f"Database URL (AAD): {SQLALCHEMY_DATABASE_URL}")
        except Exception as e:
            logger.critical(f"Error configuring AAD-based connection: {e}")
            # raise

    else:
        try:
            USR = settings.db_user
            HOST = settings.db_host
            PORT = settings.db_port
            DB = settings.db_name
            SCHEMA = settings.db_schema

            SQLALCHEMY_DATABASE_URL = (
                f"postgresql://{USR}@{HOST}:{PORT}/{DB}?options=-csearch_path={SCHEMA}"
            )

            engine = create_sqlalchemy_engine(SQLALCHEMY_DATABASE_URL)

            SQLAlchemyInstrumentor().instrument(
                engine=engine,
                tracer_provider=trace.get_tracer_provider(),
            )
            logger.info("SQLAlchemy instrumented")
            logger.info(f"Database URL: {SQLALCHEMY_DATABASE_URL}")
        except Exception as e:
            logger.critical(f"Error configuring local database connection: {e}")


    # Finalize session and base after successful engine creation
    Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
    Base = declarative_base()

except Exception as final_error:
    logger.critical(f"Database initialization failed: {final_error}")


def get_db():
    """
    FastAPI dependency: yields a session and closes it when done.
    """
    global Session
    try:
        db = Session()
        # logger.info(f"Opening DB session")
        yield db
        # logger.info(f"Committing DB session")
    except Exception as e:
        logger.error(f"General error in get_db: {e} => {traceback.format_exc()}")
    finally:
        # logger.info("Closing DB session")
        db.close()

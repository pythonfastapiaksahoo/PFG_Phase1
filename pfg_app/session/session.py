import os
import traceback

from azure.identity import CredentialUnavailableError, DefaultAzureCredential
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from tenacity import retry, stop_after_attempt, wait_exponential

from pfg_app import settings
from pfg_app.core.utils import build_rfc1738_url
from pfg_app.logger_module import logger

engine = None
Session = None
Base = None
DB = None
SQLALCHEMY_DATABASE_URL = None
SCHEMA = "pfg_schema"

if settings.build_type in ["prod"]:

    # Retrieve the access token using DefaultAzureCredential
    credential = DefaultAzureCredential()
    access_token = credential.get_token(
        "https://ossrdbms-aad.database.windows.net/.default"
    ).token

    # Get the connection string from the environment variable
    conn_string = os.getenv("AZURE_POSTGRESQL_CONNECTIONSTRING")

    # Convert connection string to RFC1738 format
    db_url, status = build_rfc1738_url(conn_string, access_token)

    # include the schema name int he connection string
    SQLALCHEMY_DATABASE_URL = db_url + "&options=-csearch_path=pfg_schema"

    # Create the SQLAlchemy engine
    engine = create_engine(
        SQLALCHEMY_DATABASE_URL,
        pool_recycle=1800,
        pool_size=20,
        max_overflow=2,
        pool_timeout=30,
    )
    Session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    Base = declarative_base()
    logger.info(f"Database URL: {SQLALCHEMY_DATABASE_URL}")


else:
    # main db config
    PWD = settings.db_password
    USR = settings.db_user
    HOST = settings.db_host
    PORT = settings.db_port
    DB = settings.db_name
    SCHEMA = settings.db_schema

    SQLALCHEMY_DATABASE_URL = (
        f"postgresql://{USR}:{PWD}@{HOST}:{PORT}/{DB}?options=-csearch_path={SCHEMA}"
    )

    engine = create_engine(
        SQLALCHEMY_DATABASE_URL,
        pool_recycle=1800,
        pool_size=25,
        max_overflow=5,
        pool_timeout=30,
    )

    Session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    Base = declarative_base()
    logger.info(f"Database URL: {SQLALCHEMY_DATABASE_URL}")


# Retry logic for transient issues
@retry(
    stop=stop_after_attempt(5),  # Retry up to 5 times
    wait=wait_exponential(multiplier=1, min=1, max=10),  # Exponential backoff
    reraise=True,  # Reraise exceptions after retries are exhausted
)
def refresh_access_token_and_get_session():
    """Refreshes the access token if expired and provides a new session."""
    global engine, Session
    try:
        # If using DefaultAzureCredential, refresh token
        credential = DefaultAzureCredential()
        access_token = credential.get_token(
            "https://ossrdbms-aad.database.windows.net/.default"
        ).token

        # Rebuild the connection string with the new token
        conn_string = os.getenv("AZURE_POSTGRESQL_CONNECTIONSTRING")
        db_url, status = build_rfc1738_url(conn_string, access_token)

        SQLALCHEMY_DATABASE_URL = db_url + "&options=-csearch_path=pfg_schema"

        # Recreate the engine and session
        engine.dispose()  # Dispose of the old engine
        engine = create_engine(
            SQLALCHEMY_DATABASE_URL,
            pool_recycle=1800,
            pool_size=20,
            max_overflow=2,
            pool_timeout=30,
        )
        Session = scoped_session(
            sessionmaker(autocommit=False, autoflush=False, bind=engine)
        )
    except CredentialUnavailableError as e:
        logger.error(
            "Credential is unavailable. Ensure the environment is configured correctly."
        )
        raise e
    except Exception as e:
        logger.error(f"Error refreshing access token: {e} => {traceback.format_exc()}")
        raise e


def get_db():
    global Session
    attempt = 0
    max_retries = 2  # Number of retries (1 original + 1 retry)
    while attempt < max_retries:
        try:
            if settings.build_type in ["prod"]:
                # Refresh the token and recreate the session if necessary
                if attempt > 0:  # Retry logic
                    refresh_access_token_and_get_session()
            db = Session()
            current_schema = db.execute("SELECT current_schema();").scalar()
            # logger.info(f"Current schema before setting: {current_schema}")
            db.execute("SET search_path TO pfg_schema;")
            current_schema = db.execute("SELECT current_schema();").scalar()
            # logger.info(f"Current schema after setting: {current_schema}")
            yield db  # Provide the session to the endpoint
            return  # Exit after successful execution
        except OperationalError as e:
            logger.error(f"Operational error in get_db: {e}. Attempt: {attempt + 1}")
            attempt += 1
            if attempt >= max_retries:
                raise HTTPException(
                    status_code=500,
                    detail="Database connection failed after retrying. Please retry.",
                )
        except Exception as e:
            logger.error(f"General error in get_db: {e} => {traceback.format_exc()}")
            raise e
        finally:
            db.close()  # Always close the session

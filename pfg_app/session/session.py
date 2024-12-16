import os
import traceback

from azure.identity import DefaultAzureCredential
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from pfg_app import settings
from pfg_app.core.utils import build_rfc1738_url
from pfg_app.logger_module import logger

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


def get_db():
    try:
        db = Session()
        current_schema = db.execute("SELECT current_schema();").scalar()
        logger.info(f"Current schema before setting: {current_schema}")
        db.execute("SET search_path TO pfg_schema;")
        current_schema = db.execute("SELECT current_schema();").scalar()
        logger.info(f"Current schema after setting: {current_schema}")
        yield db
    except Exception as e:
        logger.error(f"Error in get_db: {e} =>" + traceback.format_exc())
    finally:
        db.close()

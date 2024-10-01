from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from pfg_app import settings

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
    pool_size=20,
    max_overflow=2,
    pool_timeout=30,
)


Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base()


def get_db():
    try:
        db = Session()
        yield db
    finally:
        db.close()

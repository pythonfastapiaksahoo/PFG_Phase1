import os

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

# # main db config
# PWD=os.getenv('DATABASE_PASS',default='Connect123')
# USR=os.getenv('DATABASE_USER',default='serina')
# HOST = os.getenv('DATABASE_HOST',default="serina-qa-server1.mysql.database.azure.com")
# PORT = os.getenv('DATABASE_PORT',default='3306')
# DB = os.getenv('DATABASE_DB',default='rove_hotels')


# SQLALCHEMY_DATABASE_URL = f'mysql://{USR}:{PWD}@{HOST}:{PORT}/{DB}'


# main db config
PWD = os.getenv("DATABASE_PASS", default="Connect1234")
USR = os.getenv("DATABASE_USER", default="pfg_user")
HOST = os.getenv("DATABASE_HOST", default="ap-postgres-dev.postgres.database.azure.com")
PORT = os.getenv("DATABASE_PORT", default="5432")
DB = os.getenv("DATABASE_DB", default="pfg_db")
SCHEMA = os.getenv("DATABASE_SCHEMA", default="pfg_schema")


SQLALCHEMY_DATABASE_URL = (
    f"postgresql://{USR}:{PWD}@{HOST}:{PORT}/{DB}?options=-csearch_path={SCHEMA}"
)
# SQLALCHEMY_DATABASE_URL = f'mysql+pymysql://{SQL_USER}:{SQL_PASS}@{localhost}:{SQL_PORT}/{SQL_DB}'

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

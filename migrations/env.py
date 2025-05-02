from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context
import urllib.parse
from azure.identity import DefaultAzureCredential

from sqlalchemy.engine.url import URL
from pfg_app import settings

# main db config
PWD = settings.db_password
USR = settings.db_user
HOST = settings.db_host
PORT = settings.db_port
DB = settings.db_name
SCHEMA = settings.db_schema

# Get AAD token (no base64, just plain)
credential = DefaultAzureCredential()
token = credential.get_token("https://ossrdbms-aad.database.windows.net")
aad_token = token.token

# Escape special characters in token
escaped_token = urllib.parse.quote_plus(aad_token)
SSL_MODE = "require"
# Construct SQLAlchemy URL
SQLALCHEMY_DATABASE_URL = (
    f"postgresql+psycopg2://{USR}:{escaped_token}@{HOST}/{DB}?sslmode={SSL_MODE}&options=-csearch_path=pfg_schema"
)

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Override URL in config
config.set_main_option("sqlalchemy.url", SQLALCHEMY_DATABASE_URL)

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata

from pfg_app.model import Base
target_metadata = Base.metadata
# target_metadata = None

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

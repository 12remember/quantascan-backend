import os
import psycopg2
import environ
import logging
from psycopg2 import pool
from .settings import DJANGO_ENV, USE_PROD_DB
import datetime
from contextlib import contextmanager

env = environ.Env()
# reading .env file
environ.Env.read_env()

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DOCUMENT_DIR = os.path.join(PROJECT_ROOT, 'Documenten')

# ✅ Initialize Database Connection Pool
db_pool = None

@contextmanager
def db_cursor():
    connection, cursor = get_db_connection()
    try:
        yield connection, cursor
    except Exception as e:
        connection.rollback()
        raise
    finally:
        release_connection(connection)



def init_db_pool():
    """Initialize the database connection pool."""
    global db_pool
    try:
        if DJANGO_ENV == "production" or USE_PROD_DB:
            db_pool = pool.SimpleConnectionPool(
                minconn=1, maxconn=10, dsn=env("DATABASE_URL")
            )
        else:
            db_settings = {
                "host": env('DEV_DB_HOST', default='127.0.0.1'),
                "user": env('DEV_DB_USER', default='dev_user'),
                "password": env('DEV_DB_PASSWORD', default='dev_password'),
                "dbname": env('DEV_DB_NAME', default='qrl_dev'),
                "port": env('DEV_DB_PORT', default='5432')
            }
            db_pool = pool.SimpleConnectionPool(1, 10, **db_settings)

        if db_pool:
            logging.info("✅ Database connection pool initialized successfully.")

    except psycopg2.Error as e:
        logging.error(f"❌ Failed to initialize database pool: {e}")
        raise  # Stop execution if the pool fails

# ✅ Call this function once at startup
init_db_pool()

def get_connection():
    """Get a connection from the pool."""
    try:
        if db_pool:
            return db_pool.getconn()
        else:
            logging.error("❌ Database pool is not initialized.")
            raise psycopg2.OperationalError("Database pool not available.")
    except psycopg2.Error as e:
        logging.error(f"❌ Error getting DB connection: {e}")
        raise

def release_connection(conn):
    """Release a connection back to the pool."""
    try:
        if db_pool and conn:
            db_pool.putconn(conn)
        else:
            logging.error("❌ Tried to release a None connection or uninitialized pool.")
    except psycopg2.Error as e:
        logging.error(f"❌ Error releasing DB connection: {e}")

# ✅ Modify existing function to use the pool
def get_db_connection():
    """Returns a pooled database connection and cursor."""
    try:
        connection = get_connection()
        connection.autocommit = True
        cursor = connection.cursor()
        return connection, cursor
    except psycopg2.Error as e:
        logging.error(f"❌ Database connection error: {e}")
        raise  # Ensure Scrapy stops if DB fails

# ✅ Set scrap_url based on environment
scrap_url = "https://explorer.theqrl.org" if DJANGO_ENV == "production" or USE_PROD_DB else "http://127.0.0.1:3000"

def bytes_to_hex(byte_list):
    return bytes(byte_list).hex()

def bytes_to_string(byte_list):
    return bytes(byte_list).decode('utf-8', 'ignore')

def convert_timestamp(ts):
    return datetime.fromtimestamp(int(ts))


def list_integer_to_hex(list_of_ints):
    array = bytearray(list_of_ints)
    return bytearray.hex(array)


import os
import psycopg2
import environ
import logging
from .settings import DJANGO_ENV, USE_PROD_DB

env = environ.Env()
# reading .env file
environ.Env.read_env()

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DOCUMENT_DIR = os.path.join(PROJECT_ROOT, 'Documenten')



def get_db_connection():
    """Returns a new database connection and cursor based on environment settings."""
    try:
        if DJANGO_ENV == "production" or USE_PROD_DB:
            # ✅ Production Environment (uses DATABASE_URL)
            connection = psycopg2.connect(env("DATABASE_URL"))
        else:
            # ✅ Local Development Environment
            db_settings = {
                "host": env('DEV_DB_HOST', default='127.0.0.1'),
                "user": env('DEV_DB_USER', default='dev_user'),
                "password": env('DEV_DB_PASSWORD', default='dev_password'),
                "dbname": env('DEV_DB_NAME', default='qrl_dev'),
                "port": env('DEV_DB_PORT', default='5432')
            }
            connection = psycopg2.connect(**db_settings)
        connection.autocommit = True 
        cursor = connection.cursor()  # Use dict cursor for better readability

        return connection, cursor

    except psycopg2.Error as e:
        logging.error(f"Database connection error: {e}")
        raise  # ❌ Ensure Scrapy stops if the database fails

# ✅ Set scrap_url based on environment
scrap_url = "https://explorer.theqrl.org" if DJANGO_ENV == "production" or USE_PROD_DB else "http://127.0.0.1:3000"

    
def bytes_to_hex(byte_list):
    return bytes(byte_list).hex()

def bytes_to_string(byte_list):
    return bytes(byte_list).decode('utf-8', 'ignore')

def convert_timestamp(ts):
    return datetime.fromtimestamp(int(ts))
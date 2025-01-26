import os
import psycopg2
import environ
import dj_database_url

# Load environment variables from the backend directory (root)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Adjusted for `qrl_analytics_scripts`
env = environ.Env()
environ.Env.read_env(os.path.join(BASE_DIR, '.env'))

# Determine environment
DJANGO_ENV = env("DJANGO_ENV", default="development")
USE_PROD_DB= env.bool("USE_PROD_DB", default="False")


# Database configuration
if DJANGO_ENV == "production" or USE_PROD_DB:
    # Use DATABASE_URL for production or production DB in development
    database_url = env("DATABASE_URL")
    connection = psycopg2.connect(database_url)
    database='live'
    hostname='live'
    port='live'

else:
    hostname = env('DEV_DB_HOST', default='127.0.0.1')
    username = env('DEV_DB_USER', default='dev_user')
    password = env('DEV_DB_PASSWORD', default='dev_password')
    database = env('DEV_DB_NAME', default='qrl_dev')
    port = env('DEV_DB_PORT', default='5432')
    connection = psycopg2.connect(
        host=hostname,
        user=username,
        password=password,
        dbname=database,
        port=port
    )

# Connect to the database
try:
    cur = connection.cursor()
    print(f"Connected to database: {database} on {hostname}:{port}")
except Exception as e:
    print(f"Failed to connect to database: {e}")
    raise

# Debugging output
print(f"Analytics environment: {DJANGO_ENV}")
print(f"Using production database: {USE_PROD_DB}")

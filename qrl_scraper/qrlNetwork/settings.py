# -*- coding: utf-8 -*-

# Scrapy settings for qrlNetwork project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import os
import psycopg2
import environ


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # Adjusted for `qrlNetwork`
env = environ.Env()
environ.Env.read_env(os.path.join(BASE_DIR, '.env'))

BOT_NAME = 'qrlNetwork'

SPIDER_MODULES = ['qrlNetwork.spiders']
NEWSPIDER_MODULE = 'qrlNetwork.spiders'

# Determine environment
DJANGO_ENV = env("DJANGO_ENV", default="development")
USE_PROD_DB = env.bool("USE_PROD_DB", default="False")
LOG_LEVEL = 'INFO'

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

# Define the scraping URL
scrap_url = env('SCRAP_URL', default='https://explorer.theqrl.org')

# Debugging output
print(f"Scrapy environment: {DJANGO_ENV}")
print(f"Using production database: {USE_PROD_DB}")

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'QuantaScan.io (https://quantascan.io)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 16

# Configure a delay for requests for the same website (default: 0)
DOWNLOAD_DELAY = 0.05  # 0.07
CONCURRENT_REQUESTS_PER_DOMAIN = 16

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

# Configure item pipelines
ITEM_PIPELINES = {
    'qrlNetwork.pipelines.QrlnetworkPipeline_block': 300,
    'qrlNetwork.pipelines.QrlnetworkPipeline_transaction': 300,
    'qrlNetwork.pipelines.QrlnetworkPipeline_address': 300,
    'qrlNetwork.pipelines.QrlnetworkPipeline_missed_items': 300,
}

# Enable logging for debugging (optional)
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
        },
    },
    'root': {
        'handlers': ['console'],
        'level': 'DEBUG',
    },
}

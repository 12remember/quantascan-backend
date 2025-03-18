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
import logging

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # Adjusted for `qrlNetwork`
env = environ.Env()
environ.Env.read_env(os.path.join(BASE_DIR, '.env'))

BOT_NAME = 'qrlNetwork'

SPIDER_MODULES = ['qrlNetwork.spiders']
NEWSPIDER_MODULE = 'qrlNetwork.spiders'

# Determine environment
DJANGO_ENV = env("DJANGO_ENV", default="development")
USE_PROD_DB = env.bool("USE_PROD_DB", default="False")
LOG_LEVEL = 'ERROR'



logging.getLogger('scrapy.core.scraper').setLevel(logging.INFO)
logging.getLogger('scrapy.middleware').setLevel(logging.WARNING)



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
CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
#DOWNLOAD_DELAY = 0.0  # 0.07
CONCURRENT_REQUESTS_PER_DOMAIN = 32
REACTOR_THREADPOOL_MAXSIZE = 20
# Disable cookies (enabled by default)
COOKIES_ENABLED = False

# Configure item pipelines
ITEM_PIPELINES = {
    'qrlNetwork.pipelines.QrlnetworkPipeline_Emission': 200,
    'qrlNetwork.pipelines.QrlnetworkPipeline_block': 300,
    'qrlNetwork.pipelines.QrlnetworkPipeline_transaction': 300,
    'qrlNetwork.pipelines.QrlnetworkPipeline_address': 300,
    'qrlNetwork.pipelines.QrlnetworkPipeline_missed_items': 300,
}

# Enable logging for debugging (optional)
SPIDER_MIDDLEWARES = {
    'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': 50,
    'scrapy.spidermiddlewares.referer.RefererMiddleware': 100,
}

DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.downloadtimeout.DownloadTimeoutMiddleware': 50,
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': 100,
}



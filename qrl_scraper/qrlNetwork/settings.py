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

env = environ.Env()
# reading .env file
environ.Env.read_env()

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DOCUMENT_DIR = os.path.join(PROJECT_ROOT, 'Documenten')

BOT_NAME = 'qrlNetwork'

SPIDER_MODULES = ['qrlNetwork.spiders']
NEWSPIDER_MODULE = 'qrlNetwork.spiders'


ON_LIVE_SERVER = False # if set on True, changes etc will be mad on Live Server !!!!
if "QRL" in DOCUMENT_DIR and ON_LIVE_SERVER == False:
    hostname = 'localhost'
    username = 'postgres'
    password = 'postgres' # your password
    database = 'qrl'
    port = '5432'
    connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database, port=port)
    cur = connection.cursor()

    scrap_url = 'http://127.0.0.1:3000'
    #scrap_url = 'https://explorer.theqrl.org'


else:
    hostname = env('DATABASE_HOST')
    username = env('DATABASE_USER')
    password = env('DATABASE_PASSWORD')
    database = env('DATABASE_NAME')
    port = env('DATABASE_PORT')
    connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database, port=port)
    cur = connection.cursor()
    
    scrap_url = 'https://explorer.theqrl.org'
    
    #scrap_url = 'http://127.0.0.1:3000'







# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'QuantaScan.io (https://quantascan.io)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 64
#CONCURRENT_ITEMS = 32

#DEPTH_PRIORITY = 1
#SCHEDULER_DISK_QUEUE = 'scrapy.squeues.PickleFifoDiskQueue'
#SCHEDULER_MEMORY_QUEUE = 'scrapy.squeues.FifoMemoryQueue'
#CONCURRENT_REQUESTS = 1

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 0 #0.07
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 64
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'qrlNetwork.middlewares.QrlnetworkSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'qrlNetwork.middlewares.QrlnetworkDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'qrlNetwork.pipelines.QrlnetworkPipeline_block':300,
    'qrlNetwork.pipelines.QrlnetworkPipeline_transaction':300,    
    'qrlNetwork.pipelines.QrlnetworkPipeline_address':300,
    'qrlNetwork.pipelines.QrlnetworkPipeline_missed_items':300
    #'qrlNetwork.pipelines.QrlnetworkPipeline': 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

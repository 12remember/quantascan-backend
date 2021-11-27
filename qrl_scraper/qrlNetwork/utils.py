
import os
import psycopg2
import environ

env = environ.Env()
# reading .env file
environ.Env.read_env()

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DOCUMENT_DIR = os.path.join(PROJECT_ROOT, 'Documenten')


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

    

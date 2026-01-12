from .base import *

DEBUG = True
INSTALLED_APPS += [
    'debug_toolbar',
]

MIDDLEWARE += [
    'debug_toolbar.middleware.DebugToolbarMiddleware',
]

INTERNAL_IPS = [
    '127.0.0.1',
]

ALLOWED_HOSTS = [
    'localhost',
    '127.0.0.1',
    'www.quantascan.io',
    'analytics.quantascan.io',
    '.quantascan.io',
    
    '.herokuapp.com',        # <--- allow all subdomains of herokuapp.com
]

# Only allow localhost in development when running locally
# This prevents localhost from accessing the live production API
CORS_ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:8080",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:8080",
    # Add your local dev frontend URL here if different
]


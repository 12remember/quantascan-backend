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

CORS_ALLOW_ALL_ORIGINS = True


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

CORS_ALLOWED_ORIGINS += [
    'http://localhost:8000',
]





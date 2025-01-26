from .base import *

DEBUG = False

SECURE_SSL_REDIRECT = True
SESSION_COOKIE_SECURE = True
CSRF_COOKIE_SECURE = True
SECURE_HSTS_SECONDS = 31536000
SECURE_HSTS_INCLUDE_SUBDOMAINS = True
SECURE_HSTS_PRELOAD = True
SECURE_CONTENT_TYPE_NOSNIFF = True
SECURE_BROWSER_XSS_FILTER = True


CORS_ALLOWED_ORIGINS = [
    "https://www.quantascan.io",
    "https://analytics.quantascan.io",
]
CSRF_TRUSTED_ORIGINS = [
    "https://www.quantascan.io",
    "https://analytics.quantascan.io",
]
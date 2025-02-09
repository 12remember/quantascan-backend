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

ALLOWED_HOSTS = [
    'localhost',
    '127.0.0.1',
    'www.quantascan.io',
    'analytics.quantascan.io',
    '.quantascan.io',
    '.herokuapp.com',        # <--- allow all subdomains of herokuapp.com
]

# django-cors-headers
CORS_ALLOWED_ORIGINS = [
    "https://www.quantascan.io",
    "https://quantascan.io",
    "https://analytics.quantascan.io",
]
CORS_ALLOWED_ORIGIN_REGEXES = [
    r"^https://.*\.herokuapp\.com$"  # <--- match https://ANYTHING.herokuapp.com
]

CSRF_TRUSTED_ORIGINS = [
    'https://www.quantascan.io',
    "https://quantascan.io",
    'https://analytics.quantascan.io',
    '.herokuapp.com',  # <--- allow all herokuapp subdomains
]
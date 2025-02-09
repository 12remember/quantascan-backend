"""
WSGI config for quantascan project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/2.0/howto/deployment/wsgi/
"""


import os
import environ
from django.core.wsgi import get_wsgi_application

# Load environment variables
env = environ.Env()
environ.Env.read_env()

# Choose settings file based on DJANGO_ENV
environment = env("DJANGO_ENV", default="production")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", f"quantascan.settings.{environment}")

application = get_wsgi_application()



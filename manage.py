import os
import sys
import environ

if __name__ == "__main__":
    # Load environment variables
    env = environ.Env()
    environ.Env.read_env()
    
    # Choose settings file based on DJANGO_ENV
    environment = env("DJANGO_ENV", default="development")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", f"quantascan.settings.{environment}")

    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)

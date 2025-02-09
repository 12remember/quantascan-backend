
from django.contrib import admin
from .models import LogEntry

@admin.register(LogEntry)
class LogEntryAdmin(admin.ModelAdmin):
    list_display = ('timestamp', 'level', 'module', 'message')
    list_filter = ('level', 'timestamp')
    search_fields = ('message', 'module')
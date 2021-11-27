# Generated by Django 3.1.1 on 2020-10-21 11:20

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('analytics', '0007_auto_20201021_1021'),
    ]

    operations = [
        migrations.RenameField(
            model_name='qrlblockchainmisseditems',
            old_name='file_name',
            new_name='trace_back',
        ),
        migrations.RemoveField(
            model_name='qrlblockchainmisseditems',
            name='item_error',
        ),
        migrations.RemoveField(
            model_name='qrlblockchainmisseditems',
            name='item_error_type',
        ),
        migrations.RemoveField(
            model_name='qrlblockchainmisseditems',
            name='item_json_load',
        ),
        migrations.RemoveField(
            model_name='qrlblockchainmisseditems',
            name='missed_in',
        ),
        migrations.RemoveField(
            model_name='qrlblockchainmisseditems',
            name='missed_item_type',
        ),
        migrations.AddField(
            model_name='qrlblockchainmisseditems',
            name='error',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name='qrlblockchainmisseditems',
            name='error_type',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name='qrlblockchainmisseditems',
            name='location_script_file',
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
        migrations.AddField(
            model_name='qrlblockchainmisseditems',
            name='location_script_function',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchainmisseditems',
            name='error_timestamp',
            field=models.DateTimeField(auto_now_add=True, default=django.utils.timezone.now),
            preserve_default=False,
        ),
    ]

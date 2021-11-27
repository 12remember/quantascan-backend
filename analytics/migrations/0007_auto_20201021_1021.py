# Generated by Django 3.1.1 on 2020-10-21 10:21

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('analytics', '0006_auto_20201019_1554'),
    ]

    operations = [
        migrations.RenameField(
            model_name='qrlblockchainblocks',
            old_name='block_timestamp_seconds',
            new_name='block_found_datetime',
        ),
        migrations.RemoveField(
            model_name='qrlwalletaddress',
            name='address_first_found',
        ),
        migrations.RemoveField(
            model_name='qrlwalletaddress',
            name='address_first_found_block_num',
        ),
        migrations.RemoveField(
            model_name='qrlwalletaddress',
            name='address_last_active_receiving',
        ),
        migrations.RemoveField(
            model_name='qrlwalletaddress',
            name='address_last_active_receiving_block_num',
        ),
        migrations.RemoveField(
            model_name='qrlwalletaddress',
            name='address_last_active_sending',
        ),
        migrations.RemoveField(
            model_name='qrlwalletaddress',
            name='address_last_active_sending_block_num',
        ),
        migrations.RemoveField(
            model_name='qrlwalletaddress',
            name='address_last_updated',
        ),
        migrations.AddField(
            model_name='qrlblockchainblocks',
            name='block_added_timestamp',
            field=models.DateTimeField(auto_now_add=True, default=django.utils.timezone.now),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='qrlblockchainblocks',
            name='block_found_timestamp_seconds',
            field=models.IntegerField(default=1),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='qrlblockchainblocks',
            name='block_updated_timestamp',
            field=models.DateTimeField(auto_now=True),
        ),
        migrations.AddField(
            model_name='qrlblockchaintransactions',
            name='transaction_added_timestamp',
            field=models.DateTimeField(auto_now_add=True, default=django.utils.timezone.now),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='qrlblockchaintransactions',
            name='transaction_updated_timestamp',
            field=models.DateTimeField(auto_now=True),
        ),
        migrations.AddField(
            model_name='qrlwalletaddress',
            name='address_added_date',
            field=models.DateTimeField(auto_now_add=True, default=django.utils.timezone.now),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='qrlwalletaddress',
            name='address_updated_date',
            field=models.DateTimeField(auto_now=True),
        ),
    ]

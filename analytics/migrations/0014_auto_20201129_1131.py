# Generated by Django 3.1.1 on 2020-11-29 11:31

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('analytics', '0013_auto_20201025_1500'),
    ]

    operations = [
        migrations.AddField(
            model_name='qrlwalletaddress',
            name='address_first_found',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='qrlwalletaddress',
            name='address_first_found_block_num',
            field=models.BigIntegerField(blank=True, null=True),
        ),
    ]

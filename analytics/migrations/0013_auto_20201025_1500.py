# Generated by Django 3.1.1 on 2020-10-25 15:00

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('analytics', '0012_remove_qrlblockchaintransactions_transaction_slave_pk'),
    ]

    operations = [
        migrations.AlterField(
            model_name='qrlblockchainmisseditems',
            name='error',
            field=models.CharField(blank=True, db_column='error_name', max_length=255, null=True),
        ),
    ]

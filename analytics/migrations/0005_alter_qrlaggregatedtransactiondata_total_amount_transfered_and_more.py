# Generated by Django 5.1.5 on 2025-02-09 20:49

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('analytics', '0004_alter_qrlblockchaintransactions_id_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='qrlaggregatedtransactiondata',
            name='total_amount_transfered',
            field=models.DecimalField(blank=True, decimal_places=8, max_digits=30, null=True),
        ),
        migrations.AlterField(
            model_name='qrlaggregatedtransactiondata',
            name='transaction_fee_total',
            field=models.DecimalField(blank=True, decimal_places=8, max_digits=30, null=True),
        ),
    ]

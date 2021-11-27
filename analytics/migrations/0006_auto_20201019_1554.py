# Generated by Django 3.1.1 on 2020-10-19 15:54

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('analytics', '0005_auto_20201019_1549'),
    ]

    operations = [
        migrations.RenameField(
            model_name='qrlaggregatedtransactiondata',
            old_name='type_of_transaction',
            new_name='transaction_type',
        ),
        migrations.AlterUniqueTogether(
            name='qrlaggregatedtransactiondata',
            unique_together={('date', 'transaction_type')},
        ),
    ]

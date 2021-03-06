# Generated by Django 3.1.1 on 2020-10-19 15:49

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('analytics', '0004_remove_qrlaggregatedblockdata_block_found_time_average'),
    ]

    operations = [
        migrations.RenameField(
            model_name='qrlaggregatedtransactiondata',
            old_name='total_amount_transfered_coinbase',
            new_name='total_amount_transfered',
        ),
        migrations.RenameField(
            model_name='qrlaggregatedtransactiondata',
            old_name='total_number_of_transactions_all',
            new_name='total_number_of_transactions',
        ),
        migrations.AddField(
            model_name='qrlaggregatedtransactiondata',
            name='type_of_transaction',
            field=models.CharField(default=1, max_length=100),
            preserve_default=False,
        ),
        migrations.AlterUniqueTogether(
            name='qrlaggregatedtransactiondata',
            unique_together={('date', 'type_of_transaction')},
        ),
        migrations.RemoveField(
            model_name='qrlaggregatedtransactiondata',
            name='total_amount_transfered_all',
        ),
        migrations.RemoveField(
            model_name='qrlaggregatedtransactiondata',
            name='total_amount_transfered_slave',
        ),
        migrations.RemoveField(
            model_name='qrlaggregatedtransactiondata',
            name='total_amount_transfered_transfers',
        ),
        migrations.RemoveField(
            model_name='qrlaggregatedtransactiondata',
            name='total_number_of_transactions_coinbase',
        ),
        migrations.RemoveField(
            model_name='qrlaggregatedtransactiondata',
            name='total_number_of_transactions_slave',
        ),
        migrations.RemoveField(
            model_name='qrlaggregatedtransactiondata',
            name='total_number_of_transactions_transfers',
        ),
    ]

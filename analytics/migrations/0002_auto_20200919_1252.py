# Generated by Django 3.0.7 on 2020-09-19 12:52

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('analytics', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='qrlblockchainblocks',
            name='block_hash_header_data',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchainblocks',
            name='block_hash_header_data_prev',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchainblocks',
            name='block_hash_header_type',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchainblocks',
            name='block_hash_header_type_prev',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchainblocks',
            name='block_merkle_root_data',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchainblocks',
            name='block_merkle_root_type',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchainmisseditems',
            name='file_name',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchainmisseditems',
            name='item_error',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchainmisseditems',
            name='item_error_type',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchainmisseditems',
            name='item_url',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchainmisseditems',
            name='missed_in',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchainmisseditems',
            name='missed_item_type',
            field=models.CharField(blank=True, max_length=5000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchaintransactions',
            name='master_addr_data',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchaintransactions',
            name='master_addr_fee',
            field=models.BigIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchaintransactions',
            name='master_addr_type',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchaintransactions',
            name='public_key_data',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchaintransactions',
            name='public_key_type',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchaintransactions',
            name='signature_data',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchaintransactions',
            name='signature_type',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchaintransactions',
            name='transaction_addrs_to_type',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
        migrations.AlterField(
            model_name='qrlblockchaintransactions',
            name='transaction_slave_pk',
            field=models.CharField(blank=True, max_length=50000, null=True),
        ),
    ]

# Generated by Django 5.1.5 on 2025-03-19 00:13

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('analytics', '0006_qrlblockchainmisseditems_failed_data'),
    ]

    operations = [
        migrations.CreateModel(
            name='EmissionData',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('emission', models.BigIntegerField(default=0)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
        ),
    ]

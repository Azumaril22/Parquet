# Generated by Django 5.1.6 on 2025-03-11 15:19

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        (
            "parquetapp",
            "0004_rename_file_2_lienentrefichiersparquet_parquet_file_left_and_more",
        ),
    ]

    operations = [
        migrations.AlterField(
            model_name="lienentrefichiersparquet",
            name="parquet_file_left",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="parquet_file_left",
                to="parquetapp.parquetfile",
            ),
        ),
        migrations.AlterField(
            model_name="lienentrefichiersparquet",
            name="parquet_file_right",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="parquet_file_right",
                to="parquetapp.parquetfile",
            ),
        ),
    ]

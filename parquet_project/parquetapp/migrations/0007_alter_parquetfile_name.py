# Generated by Django 5.1.6 on 2025-03-14 08:57

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("parquetapp", "0006_parquetfile_original_vcf_file_path"),
    ]

    operations = [
        migrations.AlterField(
            model_name="parquetfile",
            name="name",
            field=models.CharField(max_length=255),
        ),
    ]

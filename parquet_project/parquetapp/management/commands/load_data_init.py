import os
from django.core.management.base import BaseCommand

from parquetapp.models import LienEntreFichiersParquet, ParquetFile


def create_parquet_files_in_path(path):
    for item in os.listdir(path):
        item_path = os.path.join(path, item)
        if item.endswith('.parquet'):
            print(item)
            ParquetFile.objects.create(name=path.replace('../db', '').replace('/', '_') + "_" +item, file_path=f'{path}/{item}')

        elif os.path.isdir(item_path):
            create_parquet_files_in_path(item_path)


class Command(BaseCommand):
    def handle(self, *args, **options):
        ParquetFile.objects.all().delete()

        create_parquet_files_in_path('../db')

        for file in ParquetFile.objects.all():
            self.stdout.write(self.style.SUCCESS(f'Loading {file.name}'))

        LienEntreFichiersParquet.objects.all().delete()
        LienEntreFichiersParquet.objects.create(
            file_1=ParquetFile.objects.get(name='_VCF_annovar_entete_variant.parquet'),
            file_2=ParquetFile.objects.get(name='_VCF_annovar_info_variant.parquet'),
            field='HASH'
        )
        LienEntreFichiersParquet.objects.create(
            file_1=ParquetFile.objects.get(name='_VCF_annovar_sample_variant.parquet'),
            file_2=ParquetFile.objects.get(name='_VCF_annovar_info_variant.parquet'),
            field='HASH'
        )
        LienEntreFichiersParquet.objects.create(
            file_1=ParquetFile.objects.get(name='_VCF_annovar_entete_variant.parquet'),
            file_2=ParquetFile.objects.get(name='_VCF_annovar_sample_variant.parquet'),
            field='HASH'
        )

        LienEntreFichiersParquet.objects.create(
            file_1=ParquetFile.objects.get(name='_VCF_lite_entete_variant.parquet'),
            file_2=ParquetFile.objects.get(name='_VCF_lite_info_variant.parquet'),
            field='HASH'
        )
        LienEntreFichiersParquet.objects.create(
            file_1=ParquetFile.objects.get(name='_VCF_lite_sample_variant.parquet'),
            file_2=ParquetFile.objects.get(name='_VCF_lite_info_variant.parquet'),
            field='HASH'
        )
        LienEntreFichiersParquet.objects.create(
            file_1=ParquetFile.objects.get(name='_VCF_lite_entete_variant.parquet'),
            file_2=ParquetFile.objects.get(name='_VCF_lite_sample_variant.parquet'),
            field='HASH'
        )

        self.stdout.write(self.style.SUCCESS('Data loaded successfully'))
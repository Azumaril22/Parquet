import datetime
import os

from django.core.management.base import BaseCommand
from parquetapp.models import ParquetFileColumn
from parquetapp.utils.entetesvcf2parquet import VCFEnteteToPython
from parquetapp.utils.vcf2parquet import VCF2ParquetExporter


class Command(BaseCommand):
    def load_file(self, filepath):
        start = datetime.datetime.now()

        exporteur = VCF2ParquetExporter(filepath)
        # Découper le fichier en 3 parquets
        exporteur.run()
        if exporteur.info_variant_django_file_object:
            self.stdout.write(
                "FileName : " + str(exporteur.info_variant_django_file_object.name),
                style_func=self.style.SUCCESS,
            )
            # Lire l'entete du fichier VCF pour déduire les colonnes et leurs types
            entete_parser = VCFEnteteToPython(
                filepath.replace(".gz", "").replace(".vcf", "_entete.txt")
            )
            entetes = entete_parser.run()
            for entete in entetes:
                if "ID" in entete and "Type" in entete and "Description" in entete:
                    _, created = ParquetFileColumn.objects.update_or_create(
                        parquet_file=exporteur.info_variant_django_file_object,
                        name=entete["ID"],
                        defaults={
                            "data_type_from_vcf": entete["Type"],
                            "description": entete["Description"],
                        },
                    )
                    if created:
                        self.stdout.write(
                            str(entete), style_func=self.style.SUCCESS
                        )
                    # else:
                    #     self.stdout.write("Already exists : " + str(entete), style_func=self.style.WARNING)

        end = datetime.datetime.now()

        self.stdout.write(
            f"Export {filepath} terminé", style_func=self.style.SUCCESS
        )
        self.stdout.write(
            f"Temps d'execution : {end - start}", style_func=self.style.SUCCESS
        )

    def handle(self, *args, **options):

        for file in os.listdir("../Datasets"):
            if file.endswith(".gz"):
                self.load_file(f"../Datasets/{file}")

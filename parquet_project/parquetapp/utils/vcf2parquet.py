import glob
import hashlib
import os
import duckdb
import polars as pl
import re

from collections import OrderedDict

from dev_tools.utils import timeit
from parquetapp.services import ParquetManager
from parquetapp.models import LienEntreFichiersParquet, ParquetFile


@timeit
def auto_cast_columns_lazy(lf: pl.LazyFrame) -> pl.LazyFrame:
    # lf.sink_csv("../db/temp.csv")
    # lf = pl.scan_csv("../db/temp.csv", infer_schema_length=1000000)
    return lf

class VCF2ParquetExporter:
    def __init__(self, filepath):
        self.filepath = filepath
        self.filename = self.filepath.split("/")[-1]
        self.extention = self.filename.split(".")[-1]

        # Liste des colonnes à mettre dans entete_variant
        self.ENTETE_COLUMNS = [
            "#CHROM",
            "POS",
            "ID",
            "REF",
            "ALT",
            "QUAL",
            "FILTER",
            "INFO",
        ]

        if self.extention == "gz":
            self.unzip()

        if self.extention != "vcf":
            raise ValueError("Le fichier doit avoir l'extension .vcf")

        self.export_path = self.get_export_path()
        self.lf = self.get_lf()

        self.entete_variant_django_file_object = None
        self.sample_variant_django_file_object = None
        self.info_variant_django_file_object = None

    def unzip(self):
        import gzip

        with gzip.open(self.filepath, "rb") as f_in:
            with open(self.filepath[:-3], "wb") as f_out:
                f_out.write(f_in.read())

        self.filepath = self.filepath[:-3]
        self.filename = self.filepath.split("/")[-1]
        self.extention = self.filename.split(".")[-1]

    def get_export_path(self):
        export_path = f"../db/{self.filename.split('.')[0]}/"
        if not os.path.exists(export_path):
            os.makedirs(export_path)
        return export_path

    def run(self):
        self.export_entete_variant()
        self.export_sample_variant()
        self.export_info_variant()
        self.extract_entete_vcf()

        self.create_linkend_files()

        self.compile_parquet()

    def get_lf(self):

        # Chargement du fichier VCF
        lf = pl.scan_csv(
            self.filepath,
            skip_rows=self.get_start_line(),
            separator="\t",
            schema_overrides={"#CHROM": pl.Utf8},
        )

        # === AJOUT DU HASH ===
        lf = lf.with_columns(
            pl.concat_str(["#CHROM", "POS", "REF", "ALT"], separator="_")
            .map_elements(
                lambda x: hashlib.sha256(x.encode()).hexdigest(), return_dtype=pl.Utf8
            )
            .alias("HASH")
        )

        return lf

    def get_start_line(self):
        with open(self.filepath, "r") as f:
            for i, line in enumerate(f):
                if line.startswith("#CHROM"):
                    return i

    def get_filename(self, export_path):
        return export_path.split(".parquet")[0].replace("../db/", "").replace("/", "_")

    def extract_entete_vcf(self):
        with open(self.filepath, "r") as f_in:
            with open(self.filepath.split(".vcf")[0] + "_entete.txt", "w") as f_out:
                for line in f_in:
                    if line.startswith("#CHROM"):
                        break
                    f_out.write(line)

    def _extract_info_keys_preserve_order(self, sample_df: pl.DataFrame):
        """Extrait les clés de INFO en conservant l'ordre d'apparition"""
        keys = OrderedDict()
        for info in sample_df["INFO"]:
            if isinstance(info, str):
                for pair in info.split(";"):
                    if "=" in pair:
                        key = pair.split("=", 1)[0]
                        if key not in keys:
                            keys[key] = True
        return list(keys.keys())

    def export_entete_variant(self):
        # === EXPORT ENTETE_VARIANT ===
        entete_columns = ["HASH"] + self.ENTETE_COLUMNS

        lf_entete = self.lf.select(
            [pl.col("#CHROM").alias("CHROM")] + [col for col in entete_columns if col in self.lf.collect_schema().names()]
        )

        lf_entete = lf_entete.drop("#CHROM")

        export_path = self.export_path + "entete_variant.parquet"
        lf_entete.sink_parquet(export_path)
        filename = self.get_filename(export_path)
        self.entete_variant_django_file_object, _ = ParquetFile.objects.update_or_create(
            name=filename, defaults={"file_path": export_path}
        )

    def export_sample_variant(self):
        # === EXPORT SAMPLE_VARIANT ===
        sample_columns = [
            col for col in self.lf.collect_schema().names() if col not in self.ENTETE_COLUMNS
        ]

        lf_sample = self.lf.select(sample_columns)

        # === UNPIVOT ===
        # Unpivot ["HASH", "SAMPLE1" "SAMPLE2"] ->
        #   ["HASH", "SAMPLE", "GENOTYPE"]
        lf_sample = lf_sample.unpivot(
            index=["HASH", "FORMAT"],
        )

        # Rename "variable" ==> "SAMPLE" et "value" ==> "GENOTYPE"
        lf_sample = lf_sample.with_columns(
            pl.col("variable").alias("SAMPLE"),
            pl.col("value").alias("GENOTYPE"),
        )

        # Suppression des colonnes "variable" et "value"
        lf_sample = lf_sample.drop(["variable", "value"])

        export_path = self.export_path + "sample_variant.parquet"
        filename = self.get_filename(export_path)
        lf_sample.sink_parquet(export_path)
        self.sample_variant_django_file_object, _ = ParquetFile.objects.update_or_create(
            name=filename, defaults={"file_path": export_path}
        )

    def export_info_variant(self):
        # === PARSING INFO ===

        # Petit échantillon pour détecter les clés présentes
        lf_info = self.lf.select(["HASH", "INFO"])

        df_info = lf_info.limit(1000).collect()

        info_keys = self._extract_info_keys_preserve_order(df_info)

        # Ajouter les colonnes parsées de INFO
        for key in info_keys:
            escaped_key = re.escape(
                key
            )  # Échapper les caractères spéciaux dans le nom de la clé
            lf_info = lf_info.with_columns(
                pl.col("INFO")
                .str.extract(rf"{escaped_key}=([^;]*)")
                .alias(key.replace(".", "_"))
            )

        # Ajouter les colonnes parsées de INFO avec gestion de "."
        # for key in info_keys:
        #     escaped_key = re.escape(key)  # Échapper les caractères spéciaux

        #     lf_info = lf_info.with_columns(
        #         pl.when(
        #             pl.col("INFO").str.extract(rf"{escaped_key}=([^;]*)") == "."
        #         )
        #         .then(None)
        #         .otherwise(pl.col("INFO").str.extract(rf"{escaped_key}=([^;]*)"))
        #         .alias(key.replace(".", "_"))
        #     )

        # Essaie de convertir dynamiquement la colonne en Int, Float ou laisse en Utf8 si conversion impossible
        lf_info = auto_cast_columns_lazy(lf_info)

        export_path = self.export_path + "info_variant.parquet"
        lf_info.sink_parquet(export_path)
        filename = self.get_filename(export_path)
        self.info_variant_django_file_object, _ = ParquetFile.objects.update_or_create(
            name=filename, defaults={"file_path": export_path}
        )

    @timeit
    def compile_parquet(self):
        export_path = self.export_path + "compile_variant"
        query = ParquetManager(self.entete_variant_django_file_object.file_path).get_query(
            limit=None,
            offset=None,
            order_by=None,
            lier_fichiers=True
        )

        query_count = ParquetManager(self.entete_variant_django_file_object.file_path).get_query(
            limit=None,
            offset=None,
            order_by=None,
            lier_fichiers=True,
            count_only=True
        )
        result = duckdb.sql(query_count).fetchone()
        print(result)

        duckdb.sql(
            f"COPY ({query}) TO '{export_path}' (FORMAT PARQUET, OVERWRITE, PARTITION_BY (SAMPLE))"
        )

        # Modifier la structure d'export des fichiers
        # Trouver tous les fichiers exportés
        for folder in os.listdir(export_path):
            folder_path = os.path.join(export_path, folder)
            
            if os.path.isdir(folder_path) and folder.startswith("SAMPLE="):  # Vérifie les partitions
                sample_value = folder.split("=")[1]  # Extrait ID_SAMPLE
                parquet_files = glob.glob(os.path.join(folder_path, "*.parquet"))
                
                for old_file in parquet_files:
                    new_file = os.path.join(export_path, f'{sample_value}.parquet')
                    os.rename(old_file, new_file)
                    
                    filename = self.get_filename(new_file)
                    ParquetFile.objects.update_or_create(
                        name=filename, defaults={"file_path": new_file}
                    )

                # Supprimer le dossier vide après déplacement
                os.rmdir(folder_path)

        print("Fichiers renommés avec succès !")

        # filename = self.get_filename(export_path)
        # ParquetFile.objects.update_or_create(
        #     name=filename, defaults={"file_path": export_path}
        # )

    def get_schema_polars_lazy(self, lf):
        # Obtenir le schéma du LazyFrame
        schema = lf.schema

        # Créer un dictionnaire pour stocker les colonnes et leurs types
        schema_dict = {name: str(dtype) for name, dtype in schema.items()}

        return schema_dict

    def create_linkend_files(self):
        if (self.entete_variant_django_file_object and
                self.sample_variant_django_file_object and
                self.info_variant_django_file_object):

            LienEntreFichiersParquet.objects.get_or_create(
                parquet_file_right=self.entete_variant_django_file_object,
                parquet_file_left=self.sample_variant_django_file_object,
                field="HASH",
            )
            LienEntreFichiersParquet.objects.get_or_create(
                parquet_file_right=self.entete_variant_django_file_object,
                parquet_file_left=self.info_variant_django_file_object,
                field="HASH",
            )
            LienEntreFichiersParquet.objects.get_or_create(
                parquet_file_right=self.sample_variant_django_file_object,
                parquet_file_left=self.info_variant_django_file_object,
                field="HASH",
            )

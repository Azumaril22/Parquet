import hashlib
import os
import polars as pl
import re

from collections import OrderedDict

from parquetapp.models import LienEntreFichiersParquet, ParquetFile


def auto_cast_columns_lazy(lf: pl.LazyFrame) -> pl.LazyFrame:
    import datetime
    start = datetime.datetime.now()
    lf.sink_csv("../db/temp.csv")
    lf = pl.scan_csv("../db/temp.csv", infer_schema_length=1000000)
    print(f"Temps d'execution trsf CSV: {datetime.datetime.now() - start}")
    return lf

class VCF2ParquetExporter:
    def __init__(self, filepath):
        self.filepath = filepath
        self.filename = self.filepath.split("/")[-1]
        self.extention = self.filename.split(".")[-1]

        # Liste des colonnes à mettre dans entete_variant
        self.ENTETE_COLUMNS = [
            "CHROM",
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

    def get_lf(self):

        # Chargement du fichier VCF
        lf = pl.scan_csv(
            self.filepath,
            skip_rows=self.get_start_line(),
            separator="\t",
            schema_overrides={"#CHROM": pl.Utf8},
        )

        # Renommer #CHROM en CHROM
        lf = lf.with_columns(pl.col("#CHROM").alias("CHROM"))

        # === AJOUT DU HASH ===
        lf = lf.with_columns(
            pl.concat_str(["CHROM", "POS", "REF", "ALT"], separator="_")
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
            [col for col in entete_columns if col in self.lf.columns]
        )

        export_path = self.export_path + "entete_variant.parquet"
        lf_entete.sink_parquet(export_path)
        self.entete_variant_django_file_object, _ = ParquetFile.objects.update_or_create(
            name=export_path.split(".")[0], defaults={"file_path": export_path}
        )

    def export_sample_variant(self):
        # === EXPORT SAMPLE_VARIANT ===
        sample_columns = [
            col for col in self.lf.columns if col not in self.ENTETE_COLUMNS
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
        lf_sample.sink_parquet(export_path)
        self.sample_variant_django_file_object, _ = ParquetFile.objects.update_or_create(
            name=export_path.split(".")[0], defaults={"file_path": export_path}
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

        # Essaie de convertir dynamiquement la colonne en Int, Float ou laisse en Utf8 si conversion impossible
        lf_info = auto_cast_columns_lazy(lf_info)

        export_path = self.export_path + "info_variant.parquet"
        lf_info.sink_parquet(export_path)
        ParquetFile.objects.update_or_create(
            name=export_path.split(".")[0], defaults={"file_path": export_path}
        )
        self.info_variant_django_file_object, _ = ParquetFile.objects.update_or_create(
            name=export_path.split(".")[0], defaults={"file_path": export_path}
        )

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

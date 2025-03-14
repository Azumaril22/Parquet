import glob
import hashlib
import os
import re
from collections import OrderedDict

import duckdb
import polars as pl
from dev_tools.utils import timeit
from parquetapp.models import LienEntreFichiersParquet, ParquetFile
from parquetapp.services import ParquetManager



class VCF2ParquetExporter:

    # Liste des colonnes à mettre dans entete_variant
    ENTETE_COLUMNS = [
        "#CHROM",
        "POS",
        "ID",
        "REF",
        "ALT",
        "QUAL",
        "FILTER",
        "INFO",
    ]

    def __init__(self, filepath):
        self.filepath = filepath
        self.filename = self.filepath.split("/")[-1]
        self.extention = self.filename.split(".")[-1]

        if self.extention == "gz":
            self.unzip()

        if not self.extention in ("vcf", "vep"):

            raise ValueError("Le fichier doit avoir l'extension .vcf ou .vep")

        self.export_path = self.get_export_path()
        self.lf = self.get_lf()

        self.entete_variant_django_file_object = None
        self.sample_variant_django_file_object = None
        self.info_variant_django_file_object = None

    def run(self):
        self.export_entete_variant()
        self.export_sample_variant()
        self.export_info_variant()
        self.extract_entete_vcf()

        self._create_linkend_files()

        self.compile_parquet()

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

    def get_filename(self, export_path):
        return os.path.splitext(os.path.basename(export_path))[0].replace("/", "_")

    def get_start_line(self):
        with open(self.filepath, "r") as f:
            for i, line in enumerate(f):
                if line.startswith("#CHROM"):
                    return i

    def extract_entete_vcf(self):
        with open(self.filepath, "r") as f_in:
            with open(os.path.splitext(self.filepath)[0] + "_entete.txt", "w") as f_out:
                for line in f_in:
                    if line.startswith("#CHROM"):
                        break
                    f_out.write(line)

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

    def _create_linkend_files(self):
        if (
            self.entete_variant_django_file_object
            and self.sample_variant_django_file_object
            and self.info_variant_django_file_object
        ):

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
        else:
            print("There is no file to link.")

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

    def _reference_file(self, export_path):
        django_file_object, _ = ParquetFile.objects.update_or_create(
            name=self.get_filename(export_path),
            original_vcf_file_path=self.filepath,
            defaults={
                "file_path": export_path,
            },
        )
        return django_file_object

    def export_entete_variant(self):
        # === EXPORT ENTETE_VARIANT ===
        entete_columns = ["HASH"] + self.ENTETE_COLUMNS

        lf_entete = self.lf.select(
            [pl.col("#CHROM").alias("CHROM")]
            + [col for col in entete_columns if col in self.lf.collect_schema().names()]
        )

        lf_entete = lf_entete.drop("#CHROM")

        export_path = self.export_path + "entete_variant.parquet"
        lf_entete.sink_parquet(export_path)

        self.entete_variant_django_file_object = self._reference_file(export_path)

    def export_sample_variant(self):
        # === EXPORT SAMPLE_VARIANT ===
        sample_columns = [
            col
            for col in self.lf.collect_schema().names()
            if col not in self.ENTETE_COLUMNS
        ]

        lf_sample = self.lf.select(sample_columns)

        if "FORMAT" in lf_sample.collect_schema().names():
            export_path = self.export_path + "sample_variant.parquet"

            # === UNPIVOT ===
            # Unpivot ["HASH", "SAMPLE1" "SAMPLE2"] ->
            #   ["HASH", "SAMPLE", "GENOTYPE"]
            lf_sample = lf_sample.unpivot(
                index=["HASH", "FORMAT"],
            )

            sample_columns += ["HASH", "FORMAT"]

            # Rename "variable" ==> "SAMPLE" et "value" ==> "GENOTYPE"
            lf_sample = lf_sample.with_columns(
                pl.col("variable").alias("SAMPLE"),
                pl.col("value").alias("VALEUR"),
            )

            sample_columns += ["SAMPLE", "VALEUR"]

            # Suppression des colonnes "variable" et "value"
            lf_sample = lf_sample.drop(["variable", "value"])

            # Splitter les colonnes en listes
            lf_sample = lf_sample.with_columns(
                [
                    pl.col("FORMAT").str.split(":").alias("keys"),
                    pl.col("VALEUR").str.split(":").alias("values"),
                ]
            )

            # Créer un dictionnaire clé -> valeur par ligne
            lf_sample = lf_sample.with_columns(
                pl.struct(["keys", "values"])
                .map_elements(
                    lambda row: dict(zip(row["keys"], row["values"])),
                    return_dtype=pl.Struct,
                )
                .alias("kv_dict")
            ).drop(["keys", "values"])

            # Transformer le dictionnaire en colonnes séparées
            lf_sample = lf_sample.unnest("kv_dict")

            # Collecter le résultat
            df = lf_sample.collect()

            # Renommer les colonnes créées en "SAMPLE_{col}"
            df = df.rename(
                {col: f"SAMPLE_{col}" for col in df.schema if col not in sample_columns}
            )
            # Ecrire le fichier parquet (ici en df à cause de "unnest")
            df.write_parquet(export_path)

            # Référencer le fichier dans la base de données
            self.sample_variant_django_file_object = self._reference_file(export_path)


    def export_info_variant(self):
        # === PARSING INFO ===

        # Petit échantillon pour détecter les clés présentes
        lf_info = self.lf.select(["HASH", "INFO"])

        df_info = lf_info.limit(100000).collect()

        info_keys = self._extract_info_keys_preserve_order(df_info)

        # Ajouter les colonnes parsées de INFO
        for key in info_keys:
            # Échapper les caractères spéciaux dans le nom de la clé
            escaped_key = re.escape(key)
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

        export_path = self.export_path + "info_variant.parquet"
        lf_info.sink_parquet(export_path)

        # Référencer le fichier dans la base de données
        self.info_variant_django_file_object = self._reference_file(export_path)

    def reorganise_parquet_partition_files(self, export_path):
        # Modifier la structure d'export des fichiers
        for folder in os.listdir(export_path):
            folder_path = os.path.join(export_path, folder)

            if os.path.isdir(folder_path) and folder.startswith(
                "SAMPLE="
            ):  # Vérifie les partitions
                sample_value = folder.split("=")[1]  # Extrait ID_SAMPLE
                parquet_files = glob.glob(os.path.join(folder_path, "*.parquet"))

                for old_file in parquet_files:
                    new_file = os.path.join(export_path, f"{sample_value}.parquet")
                    os.rename(old_file, new_file)

                    self._reference_file(new_file)

                # Supprimer le dossier vide après déplacement
                os.rmdir(folder_path)

        print("Fichiers renommés avec succès !")

    @timeit
    def compile_parquet(self):
        export_path = self.export_path + "compile_variant"
        query = ParquetManager(
            self.entete_variant_django_file_object.file_path
        ).get_query(limit=None, offset=None, order_by=None, lier_fichiers=True)

        query_count = ParquetManager(
            self.entete_variant_django_file_object.file_path
        ).get_query(
            limit=None, offset=None, order_by=None, lier_fichiers=True, count_only=True
        )
        result = duckdb.sql(query_count).fetchone()[0]
        print("Nb de lignes dans le fichier : ", export_path, result)

        try:
            duckdb.sql(
                f"COPY ({query}) TO '{export_path}' (FORMAT PARQUET, OVERWRITE, PARTITION_BY (SAMPLE))"
            )
            self.reorganise_parquet_partition_files(export_path)
        except duckdb.BinderException as e:
            print(e)

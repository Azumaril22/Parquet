import duckdb
import glob
import hashlib
import os
import polars as pl
import re
import shutil

from collections import OrderedDict

from dev_tools.utils import timeit
from parquetapp.utils.bdml_model import create_bdml
from parquetapp.models import LienEntreFichiersParquet, ParquetFile
from parquetapp.services import ParquetManager
from parquetapp.utils.entetesvcf2parquet import VCFEnteteToPython


def generate_select(file, all_columns, alias):
    return "SELECT " + ", ".join(
        [f"{col} AS {alias[col]}" if col in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{file}')").df()["column_name"].tolist() 
        else f"NULL AS {alias[col]}" for col in all_columns]
    ) + f" FROM read_parquet('{file}', hive_partitioning=0)"


def remove_tmp_files(path):
    """
    Supprime tous les fichiers et le dossier spécifié.
    Args:
        path (str): Chemin du dossier à supprimer.
    """
    if not os.path.exists(path):
        return

    try:
        shutil.rmtree(path)  # Supprime le dossier et tout son contenu
    except Exception as e:
        print(f"Erreur en supprimant {path}: {e}")


class VCF2ParquetExporter:

    # Liste des colonnes à mettre dans variant.parquet
    VARIANT_COLUMNS = [
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

        if self.extention not in ("vcf", "vep"):
            raise ValueError("Le fichier doit avoir l'extension .vcf ou .vep")

        self.export_path = self.get_export_path()
        self.lf = self.get_lf()

        self.vcf_has_sample_column = False
        self.info_has_csq_key = False
        self.variant_django_file_object = None
        self.sample_variant_django_file_object = None
        self.info_variant_django_file_object = None
        self.info_csq_variant_django_file_object = None

    def run(self):
        self.extract_entete_vcf()

        self.export_variant()
        self.export_sample_variant()
        self.export_info_variant()

        if self.vcf_has_sample_column:
            self.pivot_parquet_sample()

        if self.info_has_csq_key:
            self.split_csq_key()

        self._create_linkend_files()
        self.compile_parquet()

        # create_bdml(vcf_file_path=self.filepath)

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

        # AJOUT DU HASH
        lf = lf.with_columns(
            pl.concat_str(["#CHROM", "POS", "REF", "ALT"], separator="_")
            .map_elements(
                lambda x: hashlib.sha256(x.encode()).hexdigest(), return_dtype=pl.Utf8
            )
            .alias("HASH")
        )

        return lf

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

    def export_variant(self):
        # === EXPORT VARIANT ===
        VARIANT_COLUMNS = ["HASH"] + self.VARIANT_COLUMNS

        lf_entete = self.lf.select(
            [pl.col("#CHROM").alias("CHROM")]
            + [col for col in VARIANT_COLUMNS if col in self.lf.collect_schema().names()]
        )

        lf_entete = lf_entete.drop("#CHROM")

        export_path = self.export_path + "variant.parquet"
        lf_entete.sink_parquet(export_path)

        self.variant_django_file_object = self._reference_file(export_path)

    @timeit
    def export_sample_variant(self, lazy=True):
        # === EXPORT SAMPLE_VARIANT ===
        sample_columns = [
            col
            for col in self.lf.collect_schema().names()
            if col not in self.VARIANT_COLUMNS
        ]

        lf_sample = self.lf.select(sample_columns)

        if "FORMAT" in lf_sample.collect_schema().names():
            self.vcf_has_sample_column = True
            export_path = self.export_path + "sample_variant.parquet"

            # === UNPIVOT ===
            # Unpivot ["HASH", "SAMPLE1" "SAMPLE2"] ->
            #   ["HASH", "SAMPLE", "GENOTYPE"]
            lf_sample = lf_sample.unpivot(
                index=["HASH", "FORMAT"],
            )

            sample_columns += ["HASH", "FORMAT"]

            # Rename "variable" ==> "SAMPLE" et "value" ==> "VALUE"
            lf_sample = lf_sample.with_columns(
                pl.col("variable").alias("SAMPLE"),
                pl.col("value").alias("VALUES"),
            )

            sample_columns += ["SAMPLE", "VALUES"]

            # Suppression des colonnes "variable" et "value"
            lf_sample = lf_sample.drop(["variable", "value"])

            tmp_export_path = self.export_path + "tmp/sample_variant.parquet"
            if not os.path.exists(self.export_path + "tmp/"):
                os.makedirs(self.export_path + "tmp/")

            lf_sample.sink_parquet(tmp_export_path)

            # === EXTRACT KEY / VALUES ===
            # Splitter les colonnes en listes
            if lazy:
                all_columns = set()
                tmp_parquet_files = []
                tmp_file_id = 0
                for row in duckdb.sql(f"SELECT FORMAT FROM read_parquet('{tmp_export_path}') GROUP BY FORMAT").fetchall():
                    keys = row[0]
                    all_columns.update(keys.split(":"))
                    tmp_file_id += 1
                    tmp_key_export_path = f"{self.export_path}tmp/{tmp_file_id}sample_variant.parquet"

                    duckdb.sql(f"COPY (SELECT * FROM '{tmp_export_path}' WHERE FORMAT = '{keys}') TO '{tmp_key_export_path}' (FORMAT PARQUET)")
                    lf_key = pl.scan_parquet(tmp_key_export_path)

                    # Séparer les valeurs "VALUE" en plusieurs colonnes sur la base de ":"
                    lf_key = lf_key.with_columns(
                        pl.col("VALUES").str.split(":").alias("VALUES_split")  # Transformer en liste
                    )

                    # Extraction des colonnes dynamiquement en respectant keys
                    columns = [
                        pl.col("VALUES_split").list.get(i).alias(name)
                        for i, name in enumerate(keys.split(":"))
                    ]

                    # Ajouter ces nouvelles colonnes et supprimer les colonnes temporaires
                    lf_key = lf_key.with_columns(
                        columns
                    ).drop(
                        ["VALUES_split"]
                    )

                    # Exporter le résultat en Parquet
                    tmp_key_splited_export_path = f"{self.export_path}tmp/{tmp_file_id}_splited_sample_variant.parquet"
                    lf_key.sink_parquet(tmp_key_splited_export_path)
                    tmp_parquet_files.append(tmp_key_splited_export_path)

                alias = {col: f"SAMPLE_{col}" for col in all_columns}
                alias.update({col: col for col in ["HASH", "FORMAT", "SAMPLE", "VALUES"]})

                all_columns.update(["HASH", "FORMAT", "SAMPLE", "VALUES"])
                
                query = " UNION ALL ".join([
                    generate_select(file, all_columns, alias)
                    for file in tmp_parquet_files]
                )

                duckdb.sql(f"COPY ({query}) TO '{export_path}' (FORMAT PARQUET)")
                remove_tmp_files(self.export_path + "tmp/")

            else:
                lf_sample = lf_sample.with_columns(
                    [
                        pl.col("FORMAT").str.split(":").alias("keys"),
                        pl.col("VALUE").str.split(":").alias("values"),
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

        if "CSQ" in key:
            self.info_has_csq_key = True

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

    def split_csq_key(self):
        csq_format = VCFEnteteToPython(self.filepath).get_csq_columns()

        # Vérifier que csq_format est bien défini
        if not csq_format:
            raise ValueError("csq_format est vide ! Vérifie que la ligne ID=CSQ a bien été extraite.")

        if self.info_variant_django_file_object:
            lf_csq = pl.scan_parquet(
                self.info_variant_django_file_object.file_path,
            )

            # Séparer les valeurs "CSQ" en plusieurs lignes sur la base de ","
            lf_csq = lf_csq.with_columns(
                pl.col("CSQ").str.split(",").alias("CSQ_list")  # Créer une liste de valeurs séparées par ","
            ).explode("CSQ_list")  # Déplier la liste en plusieurs lignes

            # Séparer les valeurs "CSQ_list" en plusieurs colonnes sur la base de "|"
            lf_csq = lf_csq.with_columns(
                pl.col("CSQ_list").str.split("|").alias("CSQ_split")  # Transformer en liste
            )

            # Extraction des colonnes dynamiquement en respectant csq_format
            columns = [
                pl.col("CSQ_split").list.get(i).alias(name)
                for i, name in enumerate(csq_format)
            ]

            # Ajouter ces nouvelles colonnes et supprimer les colonnes temporaires
            lf_csq = lf_csq.with_columns(
                columns
            ).drop(
                ["CSQ", "CSQ_list", "CSQ_split"]
            )

            # Exporter le résultat en Parquet
            export_path = self.export_path + "info_csq_variant.parquet"
            lf_csq.sink_parquet(export_path)

            # Référencer le fichier dans la base de données
            self.info_csq_variant_django_file_object = self._reference_file(export_path)

        else:
            print("csq_format:", csq_format)
            print("self.info_variant_django_file_object", self.info_variant_django_file_object)

    @timeit
    def compile_parquet(self):
        export_path = self.export_path + "compile_variant"
        query = ParquetManager(
            self.variant_django_file_object.file_path
        ).get_query(limit=None, offset=None, order_by=None, lier_fichiers=True)

        query_count = ParquetManager(
            self.variant_django_file_object.file_path
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
            print(query)

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
    def pivot_parquet_sample(self, file=None):
        if self.sample_variant_django_file_object is None and file is None:
            print("If file is not provided, self.sample_variant_django_file_object should be created before pivot_parquet_sample.")
            raise ValueError("If file is not provided, self.sample_variant_django_file_object should be created before pivot_parquet_sample.")
        elif file is None:
            file = self.sample_variant_django_file_object.file_path

        export_path = self.export_path + "sample_variant_unpivot.parquet"

        # Obtenir les samples uniques
        samples = []
        for sample in duckdb.sql(
            f"SELECT DISTINCT SAMPLE FROM '{file}'"
        ).fetchall():
            samples.append(sample[0])

        select_clauses = ["HASH"]
        excluded_columns = ["FORMAT", "VALEUR"]
        pivot_column = ["SAMPLE",]

        columns_to_pivot = []
        for column in duckdb.sql(f"SELECT * FROM '{file}' LIMIT 1").columns:
            if column not in select_clauses + pivot_column + excluded_columns:
                columns_to_pivot.append(column)

        # Créer les parties de la requête SQL dynamiquement
        for sample in samples:
            for col in columns_to_pivot:
                col_name = f"SAMPLE_{sample}_{col.replace('SAMPLE_', '')}"
                select_clauses.append(f"MAX(CASE WHEN SAMPLE = '{sample}' THEN {col} END) AS {col_name}")

        # Construire la requête complète
        query = f"""
        SELECT
            {','.join(select_clauses)}
        FROM '{file}'
        GROUP BY HASH
        """

        duckdb.sql(
            f"COPY ({query}) TO '{export_path}' (FORMAT PARQUET)"
        )

        self._reference_file(export_path)

    def _create_linkend_files(self):
        if (
            self.variant_django_file_object
            and self.sample_variant_django_file_object
            and self.info_variant_django_file_object
        ):
            LienEntreFichiersParquet.objects.get_or_create(
                parquet_file_right=self.variant_django_file_object,
                parquet_file_left=self.sample_variant_django_file_object,
                field="HASH",
            )
            LienEntreFichiersParquet.objects.get_or_create(
                parquet_file_right=self.variant_django_file_object,
                parquet_file_left=self.info_variant_django_file_object,
                field="HASH",
            )
            LienEntreFichiersParquet.objects.get_or_create(
                parquet_file_right=self.sample_variant_django_file_object,
                parquet_file_left=self.info_variant_django_file_object,
                field="HASH",
            )
            if self.info_csq_variant_django_file_object:
                LienEntreFichiersParquet.objects.get_or_create(
                    parquet_file_right=self.variant_django_file_object,
                    parquet_file_left=self.info_csq_variant_django_file_object,
                    field="HASH",
                )
                LienEntreFichiersParquet.objects.get_or_create(
                    parquet_file_right=self.sample_variant_django_file_object,
                    parquet_file_left=self.info_csq_variant_django_file_object,
                    field="HASH",
                )
                LienEntreFichiersParquet.objects.get_or_create(
                    parquet_file_right=self.info_variant_django_file_object,
                    parquet_file_left=self.info_csq_variant_django_file_object,
                    field="HASH",
                )
        else:
            print("There is no file to link.")

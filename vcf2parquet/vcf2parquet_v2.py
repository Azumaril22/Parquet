from collections import OrderedDict
import datetime
import hashlib
import os
import polars as pl
import re

class VCF2ParquetExporter:
    def __init__(self, filepath):
        self.filepath = filepath
        self.filename = self.filepath.split("/")[-1]
        self.extention = self.filename.split(".")[-1]

        # Liste des colonnes à mettre dans entete_variant
        self.ENTETE_COLUMNS = [
            '#CHROM',
            'POS',
            'ID',
            'REF',
            'ALT',
            'QUAL',
            'FILTER',
            'INFO',
            # 'FORMAT'
        ]

        if self.extention == "gz":
            self.unzip()

        if self.extention != "vcf":
            raise ValueError("Le fichier doit avoir l'extension .vcf")

        self.export_path = self.get_export_path()
        self.lf = self.get_lf()

    def unzip(self):
        print("Unzipping...")
        import gzip
        with gzip.open(self.filepath, "rb") as f_in:
            with open(self.filepath[:-3], "wb") as f_out:
                f_out.write(f_in.read())

        self.filepath = self.filepath[:-3]
        self.filename = self.filepath.split("/")[-1]
        self.extention = self.filename.split(".")[-1]
        print("Unzipping done")

    def get_export_path(self):
        export_path = f"db/{self.filename.split('.')[0]}/"
        if not os.path.exists(export_path):
            os.makedirs(export_path)
        return export_path

    def run(self):
        self.export_entete_variant()
        self.export_sample_variant()
        self.export_info_variant()
        self.extract_entete_vcf()

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
                lambda x: hashlib.sha256(x.encode()).hexdigest(),
                return_dtype=pl.Utf8
            ).alias("HASH")
        )

        return lf

    def get_start_line(self):
        with open(self.filepath, "r") as f:
            for i, line in enumerate(f):
                if line.startswith("#CHROM"):
                    return i

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

        lf_entete.sink_parquet(self.export_path + "entete_variant.parquet")

    def export_sample_variant(self):
        # === EXPORT SAMPLE_VARIANT ===
        sample_columns = [
            col for col in self.lf.columns if col not in self.ENTETE_COLUMNS
        ]

        lf_sample = self.lf.select(sample_columns)

        # === UNPIVOT ===
        # Unpivot ["HASH", "SAMPLE1" "SAMPLE2"] -> ["HASH", "SAMPLE", "GENOTYPE"]
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

        lf_sample.sink_parquet(self.export_path + "sample_variant.parquet")

    def export_info_variant(self):
        # === PARSING INFO ===

        # Petit échantillon pour détecter les clés présentes
        lf_info = self.lf.select(["HASH", "INFO"])

        df_info = lf_info.limit(1000).collect()

        info_keys = self._extract_info_keys_preserve_order(df_info)

        # Ajouter les colonnes parsées de INFO
        for key in info_keys:
            escaped_key = re.escape(key)  # Échapper les caractères spéciaux dans le nom de la clé
            lf_info = lf_info.with_columns(
                pl.col("INFO").str.extract(rf"{escaped_key}=([^;]*)").alias(key.replace(".", "_"))
            )

        lf_info.sink_parquet(self.export_path + "info_variant.parquet")

    def extract_entete_vcf(self):
        with open(self.filepath, "r") as f_in:
            with open(self.filepath.split(".")[0] + "_entete.txt", "w") as f_out:
                for line in f_in:
                    if line.startswith("#CHROM"):
                        break
                    f_out.write(line)

    def get_schema_polars_lazy(self, lf):
        # Obtenir le schéma du LazyFrame
        schema = lf.schema

        # Créer un dictionnaire pour stocker les colonnes et leurs types
        schema_dict = {name: str(dtype) for name, dtype in schema.items()}

        return schema_dict


class VCFEnteteToPython:
    def __init__(self, filepath):
        self.filepath = filepath

    def parse_vcf_info_headers(self):
        info_dicts = []

        # Expression régulière pour extraire le contenu entre les chevrons
        info_pattern = re.compile(r'##INFO=<(.+?)>')

        # Expressions régulières pour extraire les attributs spécifiques
        id_pattern = re.compile(r'ID=([^,]+)')
        number_pattern = re.compile(r'Number=([^,]+)')
        type_pattern = re.compile(r'Type=([^,]+)')
        description_pattern = re.compile(r'Description=([^,]+)')

        with open(self.filepath, 'r') as vcf_file:
            for line in vcf_file:
                # Ne traiter que les lignes qui commencent par ##INFO=
                if line.startswith('##INFO='):
                    # Extraire le contenu entre les chevrons
                    match = info_pattern.search(line)
                    if match:
                        content = match.group(1)

                        # Initialiser un dictionnaire pour stocker les attributs
                        info_dict = {}

                        # Extraire ID
                        id_match = id_pattern.search(content)
                        if id_match:
                            info_dict["ID"] = id_match.group(1).replace('.', '_')

                        # Extraire Number
                        number_match = number_pattern.search(content)
                        if number_match:
                            info_dict["Number"] = number_match.group(1)

                        # Extraire Type
                        type_match = type_pattern.search(content)
                        if type_match:
                            info_dict["Type"] = type_match.group(1)

                        # Extraire Description
                        description_match = description_pattern.search(content)
                        if description_match:
                            info_dict["Description"] = description_match.group(1).replace('"', '')

                        # Ajouter le dictionnaire à la liste
                        info_dicts.append(info_dict)

                # Arrêter la lecture une fois que les en-têtes sont terminés
                elif not line.startswith('#'):
                    break

        return info_dicts


if __name__ == "__main__":
    start = datetime.datetime.now()

    exporteur = VCF2ParquetExporter("Datasets/VCF_annovar.vcf")
    exporteur.run()

    entetes = VCFEnteteToPython(
        "Datasets/VCF_annovar_entete.txt"
    ).parse_vcf_info_headers()
    print(entetes)

    end = datetime.datetime.now()

    print("Export terminé")
    print(f"Temps d'execution : {end - start}")

    start = datetime.datetime.now()

    exporteur = VCF2ParquetExporter("Datasets/VCF_lite.vcf.gz")
    exporteur.run()

    entetes = VCFEnteteToPython(
        "Datasets/VCF_lite_entete.txt"
    ).parse_vcf_info_headers()
    print(entetes)

    end = datetime.datetime.now()

    print("Export terminé")
    print(f"Temps d'execution : {end - start}")

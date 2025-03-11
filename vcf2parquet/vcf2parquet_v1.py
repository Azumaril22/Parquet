import datetime
import hashlib
import os
from collections import OrderedDict

import polars as pl


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
            "FORMAT",
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

        lf_sample.sink_parquet(self.export_path + "sample_variant.parquet")

    def export_info_variant(self):
        # === PARSING INFO ===

        # Petit échantillon pour détecter les clés présentes
        lf_info = self.lf.select(["HASH", "INFO"])

        df_info = lf_info.limit(1000).collect()

        info_keys = self._extract_info_keys_preserve_order(df_info)

        # Ajouter les colonnes parsées de INFO
        for key in info_keys:
            lf_info = lf_info.with_columns(
                pl.col("INFO").str.extract(rf"{key}=([^;]*)").alias(key)
            )

        # === EXPORT INFO_VARIANT ===
        lf_info.sink_parquet(self.export_path + "info_variant.parquet")


if __name__ == "__main__":
    start = datetime.datetime.now()

    # exporteur = VCF2ParquetExporter("Datasets/VCF_annovar.vcf")
    # exporteur.run()

    # end = datetime.datetime.now()

    # print("Export terminé")
    # print(f"Temps d'execution : {end - start}")

    start = datetime.datetime.now()

    exporteur = VCF2ParquetExporter("Datasets/VCF_lite.vcf.gz")
    exporteur.run()

    end = datetime.datetime.now()

    print("Export terminé")
    print(f"Temps d'execution : {end - start}")

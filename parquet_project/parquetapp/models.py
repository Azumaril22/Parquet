import pandas as pd
import polars as pl
import duckdb

from django.db import models

class ParquetUser:
    parquet_file = "parquetapp/data/users.parquet"

    def __init__(self, id, username, email):
        self.id = id
        self.username = username
        self.email = email

    @classmethod
    def all(cls):
        try:
            df = pd.read_parquet(cls.parquet_file)
            return [cls(**row.to_dict()) for _, row in df.iterrows()]
        except FileNotFoundError:
            return []

    @classmethod
    def filter(cls, **conditions):
        df = pd.read_parquet(cls.parquet_file)
        for key, value in conditions.items():
            df = df[df[key] == value]
        return [cls(**row.to_dict()) for _, row in df.iterrows()]

    @classmethod
    def save(cls, user_obj):
        try:
            df = pd.read_parquet(cls.parquet_file)
        except FileNotFoundError:
            df = pd.DataFrame(columns=['id', 'username', 'email'])

        df = df[df['id'] != user_obj.id]
        new_row = pd.DataFrame([user_obj.__dict__])
        df = pd.concat([df, new_row], ignore_index=True)
        df.to_parquet(cls.parquet_file)

    def to_dict(self):
        return {
            "id": self.id,
            "username": self.username,
            "email": self.email,
        }

class ParquetModel(models.Model):
    class Meta:
        abstract = True  # Empêche la création en base SQL

    @classmethod
    def all(cls):
        return pd.read_parquet(cls.get_file_path())

    @classmethod
    def save_polars_df_to_parquet(cls, df, table_name=None):
        if table_name is None:
            table_name = cls.get_table_name()
        df.sink_parquet(f"../db/{table_name}")

    @classmethod
    def get_file_path(cls):
        return cls.get_table_name().lower()


class ParquetVariantRecord:

    def get_table_name(self):
        return "variants.parquet"

    # id = models.AutoField(primary_key=True)
    # chrom = models.CharField(max_length=10)
    # pos = models.IntegerField()
    # ref = models.CharField(max_length=255)
    # alt = models.CharField(max_length=255)
    # qual = models.FloatField()
    # filter = models.CharField(max_length=255)
    # info = models.CharField(max_length=255)
    # format = models.CharField(max_length=255)
    # sample = models.CharField(max_length=255)

    def get_start_line(self, vcf_file):
        with open(vcf_file, "r") as f:
            for i, line in enumerate(f):
                if line.startswith("# CHROM"):
                    return i

    def load_from_vcf(self, vcf_file):
        df = pl.scan_csv(
            vcf_file,
            skip_rows=self.get_start_line(vcf_file),
            separator="\t",
            schema_overrides={"#CHROM": pl.Utf8},
        )

        self.save_polars_df_to_parquet(df, table_name=self.get_table_name())

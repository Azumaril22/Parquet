import duckdb
from django.db import models


class ParquetFile(models.Model):
    name = models.CharField(max_length=255, unique=True)
    file_path = models.CharField(max_length=512)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    alias = models.CharField(
        max_length=255,
        blank=True,
        null=True
    )

    def __str__(self):
        return self.name

    def get_alias(self):
        if self.alias:
            alias = self.alias
        else:
            alias = self.name.replace(".parquet", "")
            self.alias = alias
            self.save()
        return alias

    def get_columns(self, exclude=None):
        if exclude is None:
            exclude = []

        columns = []
        for col in duckdb.sql(
            f"DESCRIBE SELECT * FROM read_parquet('{self.file_path}')"
        ).fetchall():
            if col[0] not in exclude:
                columns.append(col[0])

        return columns

    def set_colums(self):
        for col in duckdb.sql(
            f"DESCRIBE SELECT * FROM read_parquet('{self.file_path}')"
        ).fetchall():
            if col[1] != "VARCHAR":
                print(col)
            ParquetFileColumn.objects.update_or_create(
                parquet_file=self,
                name=col[0],
                defaults={
                    "data_type_from_duckdb": col[1],
                }
            )


class LienEntreFichiersParquet(models.Model):
    parquet_file_right = models.ForeignKey(
        ParquetFile,
        on_delete=models.CASCADE,
        related_name="parquet_file_right"
    )
    parquet_file_left = models.ForeignKey(
        ParquetFile,
        on_delete=models.CASCADE,
        related_name="parquet_file_left"
    )
    field = models.CharField(max_length=255)


class ParquetFileColumn(models.Model):
    parquet_file = models.ForeignKey(
        ParquetFile, on_delete=models.CASCADE, related_name="columns"
    )
    name = models.CharField(max_length=255)
    data_type_from_vcf = models.CharField(max_length=255)
    data_type_from_duckdb = models.CharField(max_length=255)
    description = models.TextField()

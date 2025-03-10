from django.db import models
import duckdb


class ParquetFile(models.Model):
    name = models.CharField(max_length=255, unique=True)
    file_path = models.CharField(max_length=512)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    alias = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
            return self.name

    def get_alias(self):
        if self.alias:
            alias = self.alias
        else:
            alias = self.name.replace('.parquet', '')
            self.alias = alias
            self.save()
        return alias

    def get_columns(self, exclude=None):
        if exclude is None:
            exclude = []

        columns = []
        for col in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{self.file_path}')").fetchall():
            if col[0] not in exclude: 
                columns.append(col[0])

        return columns

class LienEntreFichiersParquet(models.Model):
    file_1 = models.ForeignKey(ParquetFile, on_delete=models.CASCADE, related_name='lien_1')
    file_2 = models.ForeignKey(ParquetFile, on_delete=models.CASCADE, related_name='lien_2')
    field = models.CharField(max_length=255)

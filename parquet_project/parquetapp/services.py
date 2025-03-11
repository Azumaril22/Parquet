import duckdb
import polars as pl
from parquetapp.models import LienEntreFichiersParquet, ParquetFile


class ParquetManager:
    def __init__(self, file_path):
        self.file_path = file_path
        self.alias = ParquetFile.objects.get(file_path=file_path).get_alias()

    def get_columns(self, lier_fichiers=False):
        columns = ParquetFile.objects.get(file_path=self.file_path).get_columns(
            ["HASH", "INFO"]
        )

        if lier_fichiers:
            # Ajouter les colonnes des fichiers joints en excluant les colonnes HASH et INFO qui sont dupliquées
            for file in LienEntreFichiersParquet.objects.filter(
                parquet_file_right__file_path=self.file_path
            ):
                columns += ParquetFile.objects.get(
                    file_path=file.parquet_file_left.file_path
                ).get_columns(exclude=["HASH", "INFO"])

        return columns

    def get_query(
        self,
        limit=100,
        offset=None,
        order_by=None,
        lier_fichiers=False,
        count_only=False,
    ):
        if count_only:
            query = "SELECT count(*) "
        else:
            query = f'SELECT {self.alias}.HASH, {self.alias}.INFO "'
            columns = self.get_columns(lier_fichiers)
            query += '", "'.join(columns)
            query += '"'

        query += f" FROM read_parquet('{self.file_path}') AS '{self.alias}'"

        if lier_fichiers:
            for file in LienEntreFichiersParquet.objects.filter(
                parquet_file_right__file_path=self.file_path
            ):
                query += f" LEFT JOIN read_parquet('{file.parquet_file_left.file_path}')  AS '{file.parquet_file_left.get_alias()}' ON {file.parquet_file_right.get_alias()}.{file.field} = {file.parquet_file_left.get_alias()}.{file.field}"

        if count_only:
            return query

        if order_by is not None:
            query += f" ORDER BY {order_by}"
        else:
            query += f" ORDER BY {self.alias}.HASH"

        if limit is not None:
            query += f" LIMIT {limit}"

        if offset is not None:
            query += f" OFFSET {offset}"

        return query

    def read(self, limit=100, offset=None, order_by=None, lier_fichiers=False):
        query = self.get_query(limit, offset, order_by, lier_fichiers)

        # Exécuter la requête avec DuckDB et récupérer le résultat au format Arrow
        result = duckdb.sql(query).arrow()

        # Convertir en Polars DataFrame
        return pl.from_arrow(result)

    def count(self, lier_fichiers=False):
        query = self.get_query(lier_fichiers, count_only=True)

        result = duckdb.sql(query).fetchone()[0]
        return result

    def create(self, data):
        df = pl.DataFrame(data)
        df.write_parquet(self.file_path)

    def add_data(self, data):
        existing_df = pl.read_parquet(self.file_path)
        new_df = pl.DataFrame(data)
        updated_df = pl.concat([existing_df, new_df])
        updated_df.write_parquet(self.file_path)

    def delete_data_by_hashes(self, hashes: list):
        existing_df = pl.read_parquet(self.file_path)
        filtered_df = existing_df.filter(~pl.col("HASH").is_in(hashes))
        filtered_df.write_parquet(self.file_path)

    def update_data_by_hashes(self, data):
        existing_df = pl.read_parquet(self.file_path)
        hashes = []
        for item in data:
            hashes.append(item["HASH"])

        filtered_df = existing_df.filter(~pl.col("HASH").is_in(hashes))
        new_df = pl.DataFrame(data)
        updated_df = pl.concat([filtered_df, new_df])
        updated_df.write_parquet(self.file_path)

    def delete(self):
        import os

        if os.path.exists(self.file_path):
            os.remove(self.file_path)

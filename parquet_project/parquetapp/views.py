import duckdb
from rest_framework import viewsets
from rest_framework.response import Response

from .models import ParquetModel


# VueSet pour DRF
class ParquetViewSet(viewsets.ViewSet):

    def list(self, request, file_path=None):
        file_path = "../db/VCF_annovar/entete_variant.parquet"
        result = duckdb.sql("""

            SELECT * 
            FROM '../db/VCF_annovar/entete_variant.parquet' AS ENTETE 
            --LEFT JOIN '../db/VCF_annovar/sample_variant.parquet' AS SAMPLE ON ENTETE.HASH = SAMPLE.HASH
            LEFT JOIN '../db/VCF_annovar/info_variant.parquet' AS INFO ON ENTETE.HASH = INFO.HASH
            LEFT JOIN '../db/VCF_annovar/info_variant.parquet' AS INFO2 ON ENTETE.HASH = INFO2.HASH
            ORDER BY ENTETE.HASH
            LIMIT 10
            OFFSET 10
        """)

        # Fetch all rows
        rows = result.fetchall()

        # Get column names
        columns = result.columns

        # Convert to a list of dictionaries
        data = [dict(zip(columns, row)) for row in rows]
        return Response(data)

    def create(self, request, file_path=None):
        new_entry = ParquetModel(file_path).create(**request.data)
        return Response(new_entry.to_dict(orient='records'))

    def update(self, request, file_path=None, pk=None):
        filter_kwargs = {"hash": pk}  # Suppose que l'identifiant est "id"
        update_kwargs = request.data
        updated_df = ParquetModel(
            file_path
        ).update(filter_kwargs, update_kwargs)
        return Response(updated_df.to_dict(orient='records'))

    def destroy(self, request, file_path=None, pk=None):
        ParquetModel(file_path).delete(hash=pk)
        return Response({"message": "Deleted successfully"})

from rest_framework import serializers

from .models import LienEntreFichiersParquet, ParquetFile


class ParquetFileSerializer(serializers.ModelSerializer):
    class Meta:
        model = ParquetFile
        fields = "__all__"


class LienEntreFichiersParquetSerializer(serializers.ModelSerializer):
    class Meta:
        model = LienEntreFichiersParquet
        fields = "__all__"

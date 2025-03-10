from rest_framework import serializers
from .models import ParquetFile


class ParquetFileSerializer(serializers.ModelSerializer):
    class Meta:
        model = ParquetFile
        fields = '__all__'
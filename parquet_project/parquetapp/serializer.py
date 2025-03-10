from rest_framework import serializers


# Serializer pour DRF
class ParquetSerializer(serializers.Serializer):
    data = serializers.JSONField()

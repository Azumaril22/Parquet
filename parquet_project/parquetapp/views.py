from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.pagination import PageNumberPagination
from .models import ParquetFile
from .serializers import ParquetFileSerializer
from .services import ParquetManager


class ParquetFileViewSet(viewsets.ModelViewSet):
    queryset = ParquetFile.objects.all()
    serializer_class = ParquetFileSerializer
    pagination_class = PageNumberPagination

    # def retrieve(self, request, *args, **kwargs):
    #     instance = self.get_object()
    #     manager = ParquetManager(instance.file_path)
    #     data = manager.read()
    #     return Response(data.to_dicts())  # Convertir Polars DataFrame en liste de dictionnaires

    def retrieve(self, request, *args, **kwargs):
        instance = self.get_object()
        manager = ParquetManager(instance.file_path)

        # Récupérer les paramètres de pagination
        paginator = self.pagination_class()
        page_size = int(request.query_params.get('page_size', paginator.get_page_size(request)))
        page_number = int(request.query_params.get('page', 1))
        lier_fichiers = request.query_params.get('lier_fichiers', False) in ('true', 'True', '1')

        # Calculer l'offset et la limite
        offset = (page_number - 1) * page_size
        limit = page_size

        # Lire les données du fichier Parquet
        data = manager.read(limit=limit, offset=offset, lier_fichiers=lier_fichiers)

        # Obtenir le nombre total d'enregistrements
        total_count = manager.count(lier_fichiers)

        # Convertir les données en liste de dictionnaires
        data_dict = data.to_dicts()

        # Paginer les résultats manuellement
        paginated_response = {
            "count": total_count,
            "next": self.get_next_link(request, total_count, page_number, page_size),
            "previous": self.get_previous_link(request, page_number, page_size),
            "results": data_dict,
        }
        return Response(paginated_response)

    def get_next_link(self, request, total_count, page_number, page_size):
        if (int(page_number) * page_size) >= total_count:
            return None
        # Construire l'URL absolue avec l'URL de base
        return request.build_absolute_uri(f"?page={page_number + 1}&page_size={page_size}")

    def get_previous_link(self, request, page_number, page_size):
        if int(page_number) <= 1:
            return None
        # Construire l'URL absolue avec l'URL de base
        return request.build_absolute_uri(f"?page={page_number - 1}&page_size={page_size}")

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        manager = ParquetManager(serializer.data['file_path'])
        manager.create(request.data.get('data', []))
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    def update(self, request, *args, **kwargs):
        instance = self.get_object()
        manager = ParquetManager(instance.file_path)
        manager.update(request.data.get('data', []))
        return Response(status=status.HTTP_200_OK)

    def destroy(self, request, *args, **kwargs):
        instance = self.get_object()
        manager = ParquetManager(instance.file_path)
        manager.delete()
        self.perform_destroy(instance)
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=True, methods=['post'], url_path='add-data')
    def add_data(self, request, *args, **kwargs):
        instance = self.get_object()
        manager = ParquetManager(instance.file_path)
        manager.add_data(request.data)
        return Response(status=status.HTTP_200_OK)

    @action(detail=True, methods=['post'], url_path='delete-data')
    def delete_data(self, request, *args, **kwargs):
        instance = self.get_object()
        manager = ParquetManager(instance.file_path)

        hashes = request.data.get('hashes', None)
        if hashes is None:
            return Response(status=status.HTTP_400_BAD_REQUEST)

        if not isinstance(hashes, list):
            hashes = [hashes]

        manager.delete_data_by_hashes(hashes)
        return Response(status=status.HTTP_200_OK)

    @action(detail=True, methods=['post'], url_path='update-data')
    def update_data(self, request, *args, **kwargs):
        instance = self.get_object()
        manager = ParquetManager(instance.file_path)

        manager.update_data_by_hashes(request.data)
        return Response(status=status.HTTP_200_OK)

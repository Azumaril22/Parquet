from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import ParquetViewSet

router = DefaultRouter()
router.register(r'parquet', ParquetViewSet, basename='parquet')

urlpatterns = [
    path('api/<str:file_path>/', include(router.urls)),
]

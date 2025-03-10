from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import ParquetFileViewSet

router = DefaultRouter()
router.register(r'parquet-files', ParquetFileViewSet)

urlpatterns = [
    path('api/', include(router.urls)),
]

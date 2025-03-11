from django.urls import path

from .views import ListColumsInFile, PerformanceTestView

app_name = "dev_tools"

urlpatterns = [
    path("test_perfs/", PerformanceTestView.as_view(), name="test_perfs"),
    path("columns/", ListColumsInFile.as_view(), name="list_columns"),
]

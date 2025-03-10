from django.urls import path

from .views import PerformanceTestView

app_name = "dev_tools"

urlpatterns = [
    path("test_perfs/", PerformanceTestView.as_view(), name="test_perfs"),
]

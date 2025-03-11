from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", include("parquetapp.urls")),
    path("dev_tools/", include("dev_tools.urls")),
]

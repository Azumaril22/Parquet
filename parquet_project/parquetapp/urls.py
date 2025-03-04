from django.urls import path
from . import views

urlpatterns = [
    path('users/', views.user_list, name='user-list'),
    path('users/add/', views.add_user, name='add-user'),
]

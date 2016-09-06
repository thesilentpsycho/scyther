from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^about/$', views.AboutPageView.as_view()),
    url(r'^', views.HomePageView.as_view()),
]
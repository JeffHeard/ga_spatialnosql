from django.conf.urls.defaults import patterns, include, url
from ga_spatialnosql import views, api

urlpatterns = patterns('',
    url(r'^$', api.UniverseView.as_view()),
    url(r'^(?P<connection>[^/]*)/$', api.ConnectionView.as_view()),
    url(r'^(?P<connection>[^/]*)/(?P<db>[^/]*)/$', api.DBView.as_view()),
    url(r'^(?P<connection>[^/]*)/(?P<db>[^/]*)/(?P<collection>[^/]*)/$', api.CollectionView.as_view()),
    url(r'^(?P<connection>[^/]*)/(?P<db>[^/]*)/(?P<collection>[^/]*)/properties/$', api.CollectionPropertiesView.as_view()),
    url(r'^(?P<connection>[^/]*)/(?P<db>[^/]*)/(?P<collection>[^/]*)/properties/(?P<property>[^/]*)/$', api.CollectionPropertiesView.as_view()),
    url(r'^(?P<connection>[^/]*)/(?P<db>[^/]*)/(?P<collection>[^/]*)/(?P<object>[^/]*)/$', api.ObjectView.as_view()),

)

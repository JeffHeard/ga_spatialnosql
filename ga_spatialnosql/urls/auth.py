__author__ = 'jeff'

from django.conf.urls.defaults import patterns, include, url
from ga_spatialnosql import views, api

urlpatterns = patterns('',
    url(r'^my/$', api.AuthenticatedDBView.as_view()),
    url(r'^my/(?P<collection>[^/]*)/$', api.AuthenticatedCollectionView.as_view()),
    url(r'^my/(?P<collection>[^/]*)/properties/$', api.CollectionPropertiesView.as_view()),
    url(r'^my/(?P<db>[^/]*)/(?P<collection>[^/]*)/properties/(?P<property>[^/]*)/$', api.AuthenticatedCollectionPropertiesView.as_view()),
    url(r'^my/(?P<collection>[^/]*)/(?P<object>[^/]*)/$', api.AuthenticatedObjectView.as_view()),

    url(r'^(?P<connection>[^:]*):(?P<db>[^/]*)/$', api.AuthenticatedDBView.as_view()),
    url(r'^(?P<connection>[^:]*):(?P<db>[^:]*):(?P<collection>[^/]*)/$', api.AuthenticatedCollectionView.as_view()),
    url(r'^(?P<connection>[^:]*):(?P<db>[^:]*):(?P<collection>[^/]*)/properties/$', api.AuthenticatedCollectionPropertiesView.as_view()),
    url(r'^(?P<connection>[^:]*):(?P<db>[^:]*):(?P<collection>[^/]*)/properties/(?P<property>[^/]*)/$', api.AuthenticatedCollectionPropertiesView.as_view()),
    url(r'^(?P<connection>[^:]*):(?P<db>[^:]*):(?P<collection>[^/]*)/(?P<object>[^/]*)/$', api.AuthenticatedObjectView.as_view()),
)

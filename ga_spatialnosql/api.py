from django.http import HttpResponse, HttpResponseNotFound, HttpResponseForbidden, HttpResponseBadRequest

__author__ = 'jeff'

from django.conf import settings
from django.views.generic import View
import json

def _json_response(request, d, default=None):
    if 'jsonp' in request.REQUEST:
        return HttpResponse('{callback}({js})'.format(
            callback = request.REQUEST['jsonp'],
            js = json.dumps(d, default=default)
        ), mimetype='application/json')
    elif 'callback' in request.REQUEST:
        return HttpResponse('{callback}({js})'.format(
            callback = request.REQUEST['callback'],
            js = json.dumps(d, default=default)
        ), mimetype='application/json')
    else:
        return HttpResponse(json.dumps(d, default=default), mimetype='application/json' )

class UniverseView(View):
    def get(self, request, *args, **kwargs):
        return _json_response(request, settings.TERRAHUB_CONNECTIONS.keys())

    def post(self, request, *args, **kwargs):
        return HttpResponseBadRequest()

    def put(self, request, *args, **kwargs):
        return HttpResponseBadRequest()

    def delete(self, request, *args, **kwargs):
        return HttpResponseBadRequest()

class ConnectionView(View):
    def get(self, request, *args, **kwargs):
        return _json_response(request, settings.TERRAHUB_CONNECTIONS[ kwargs['connection'] ].keys())

    def post(self, request, *args, **kwargs):
        return HttpResponseBadRequest()

    def put(self, request, *args, **kwargs):
        return HttpResponseBadRequest()

    def delete(self, request, *args, **kwargs):
        return HttpResponseBadRequest()

class DBView(View):
    def get(self, request, *args, **kwargs):
        return _json_response(request, settings.TERRAHUB_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ].keys())

    def post(self, request, *args, **kwargs):
        collection = settings.TERRAHUB_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ request.POST['collection'] ]
        properties = collection.deserialize( request.POST.get('properties', '{}'))
        for key, value in properties.items():
            collection[key] = value
        return HttpResponse()

    def put(self, request, *args, **kwargs):
        return HttpResponseBadRequest()

    def delete(self, request, *args, **kwargs):
        del settings.TERRAHUB_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ]
        return HttpRespose()

class CollectionView(View):
    def get(self, request, *args, **kwargs):
        collection = settings.TERRAHUB_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        query = request.REQUEST.get('query', None)
        geo_query = request.REQUEST.get('geo_query', None)
        if query:
            query = collection.deserialize(query)
        if geo_query:
            geo_query = collection.deserialize(geo_query)

        if query or geo_query:
            return _json_response(request, list(collection.find_features(spec=query, geo_spec=geo_query)))
        else:
            return _json_response(request, list(collection.find_features()))


    def post(self, request, *args, **kwargs):
        collection = settings.TERRAHUB_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        if len(request.POST.keys()) == 1 and 'object' in request.POST:
            object = collection.deserialize(request.POST['object'])
        else:
            object = dict(request.POST)
        collection.save(object)

    def put(self, request, *args, **kwargs):
        collection = settings.TERRAHUB_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        query = request.REQUEST.get('query', None)
        updates = request.REQUEST['updates']
        geo_query = request.REQUEST.get('geo_query', None)
        collection.update(query=query, geo_query=geo_query, updates=updates)

    def delete(self, request, *args, **kwargs):
        del settings.TERRAHUB_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]

class CollectionPropertiesView(View):
    def get(self, request, *args, **kwargs):
        collection = settings.TERRAHUB_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        if 'property' in kwargs:
            return _json_response(request, {
                kwargs['property'] : collection[ kwargs['property'] ]
            })
        else:
            return _json_response(request, list(collection.items()))

    def post(self, request, *args, **kwargs):
        collection = settings.TERRAHUB_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]

        if 'dtype' in request.POST and request.POST['dtype'] == 'int':
            collection[ kwargs['property'] ] = int( request.POST[ kwargs['property'] ] )
        elif 'dtype' in request.POST and request.POST['dtype'] == 'float':
            collection[ kwargs['property'] ] = float( request.POST[ kwargs['property'] ] )
        elif 'dtype' in request.POST and request.POST['dtype'] == 'str':
            collection[ kwargs['property'] ] = request.POST[ kwargs['property'] ]
        elif 'dtype' in request.POST and request.POST['dtype'] == 'object':
            collection[ kwargs['property'] ] = collection.deserialize( request.POST[ kwargs['property'] ] )
        elif 'dtype' not in request.POST:
            collection[ kwargs['property'] ] = request.POST[ kwargs['property'] ]
        else:
            return HttpResponseBadRequest('unknown datatype for property')

        return HttpResponse()

    def put(self, request, *args, **kwargs):
        return self.post(request, *args, **kwargs)

    def delete(self, request, *args, **kwargs):
        collection = settings.TERRAHUB_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        del collection[ kwargs['property'] ]
        return HttpResponse()


class ObjectView(View):
    def get(self, request, *args, **kwargs):
        collection = settings.TERRAHUB_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        object = collection.get( kwargs['object'] )
        object = collection.serialize(object)
        return _json_response(request, object)

    def post(self, request, *args, **kwargs):
        collection = settings.TERRAHUB_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        if len(request.POST) == 1 and 'object' in request.POST:
            object = collection.deserialize(request.POST['object'])
        else:
            object = dict(request.POST)
        collection.save(object)

    def put(self, request, *args, **kwargs):
        collection = settings.TERRAHUB_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        if len(request.POST) == 1 and 'object' in request.POST:
            object = collection.deserialize(request.POST['object'])
        else:
            object = dict(request.POST)

        original = collection.get( kwargs['object'] )
        for key, value in object:
            original[key] = value

        return _json_response(collection.serialize(original))

    def delete(self, request, *args, **kwargs):
        collection = settings.TERRAHUB_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        collection.delete_feature(kwargs['object'])

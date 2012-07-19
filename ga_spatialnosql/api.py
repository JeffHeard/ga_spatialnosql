from django.http import HttpResponse, HttpResponseBadRequest

__author__ = 'jeff'

from django.conf import settings
from django.views.generic import View
import json
from logging import getLogger

log = getLogger(__name__)

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
        log.debug('listing all connections')
        return _json_response(request, settings.GA_SPATIALNOSQL_CONNECTIONS.keys())

    def post(self, request, *args, **kwargs):
        return HttpResponseBadRequest()

    def put(self, request, *args, **kwargs):
        return HttpResponseBadRequest()

    def delete(self, request, *args, **kwargs):
        return HttpResponseBadRequest()

class ConnectionView(View):
    def get(self, request, *args, **kwargs):
        log.debug('listing all databases for {connection} : {dbs}'.format(**dict(kwargs, dbs=settings.GA_SPATIALNOSQL_CONNECTIONS[ kwargs['connection']].keys() )))
        return _json_response(request, settings.GA_SPATIALNOSQL_CONNECTIONS[ kwargs['connection'] ].keys())

    def post(self, request, *args, **kwargs):
        return HttpResponseBadRequest()

    def put(self, request, *args, **kwargs):
        return HttpResponseBadRequest()

    def delete(self, request, *args, **kwargs):
        return HttpResponseBadRequest()

class DBView(View):
    def get(self, request, *args, **kwargs):
        log.debug('listing all collections for {connection}:{db}'.format(**kwargs))
        return _json_response(request, settings.GA_SPATIALNOSQL_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ].keys())

    def post(self, request, *args, **kwargs):
        log.debug('creating a new collection for {connection}:{db}:{collection}'.format(**dict(kwargs, collection=request.POST['collection'] )))
        collection = settings.GA_SPATIALNOSQL_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ request.POST['collection'] ]
        properties = collection.deserialize( request.POST.get('properties', '{}'))
        log.debug('adding properties {props}'.format(props=properties))
        for key, value in properties.items():
            collection[key] = value

        log.debug('added properties to new collection {conn}:{db}:{coll} properties: {props} '.format(conn=kwargs['connection'], db=kwargs['db'], coll=request.POST['collection'], props=collection.keys()))
        return HttpResponse()

    def put(self, request, *args, **kwargs):
        return HttpResponseBadRequest()

    def delete(self, request, *args, **kwargs):
        log.debug('deleting {db} database entirely'.format(**kwargs))
        del settings.GA_SPATIALNOSQL_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ]
        return HttpResponse()

class CollectionView(View):
    def get(self, request, *args, **kwargs):
        log.debug('accepting a query to {connection}:{db}:{collection}'.format(**kwargs))
        log.debug('query: {query}'.format(query=request.REQUEST.get('query', None)))
        log.debug('geoquery: {geo_query}'.format(geo_query=request.REQUEST.get('geo_query', None)))

        collection = settings.GA_SPATIALNOSQL_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
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
        log.debug('appending an object to collection {connection}:{db}:{collection}'.format(**kwargs))

        collection = settings.GA_SPATIALNOSQL_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        if len(request.POST.keys()) == 1 and 'object' in request.POST:
            object = collection.deserialize(request.POST['object'])
            log.debug('appending an object from JSON with keys {keys}'.format(keys=object.keys))
        else:
            object = dict(request.POST)
            log.debug('appending an object from the POST dict with keys {keys}'.format(keys=object.keys))
        collection.save(object)

    def put(self, request, *args, **kwargs):
        log.debug('updating objects in place on {connection}:{db}:{collection} matching {query}'.format(**dict(kwargs, query=request.REQUEST.get('query', None)) ))
        log.debug('constraining object update on {connection}:{db}:{collection} to {geo_query}'.format(**dict(kwargs, geo_query=request.REQUEST.get('geo_query', None)) ))
        log.debug('updates: {updates}'.format(updates=request.REQUEST['updates']))

        collection = settings.GA_SPATIALNOSQL_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        query = request.REQUEST.get('query', None)
        updates = request.REQUEST['updates']
        geo_query = request.REQUEST.get('geo_query', None)
        collection.update(query=query, geo_query=geo_query, updates=updates)

    def delete(self, request, *args, **kwargs):
        log.debug('deleting the collection at {connection}:{db}:{collection}'.format(**kwargs))
        del settings.GA_SPATIALNOSQL_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        return HttpResponse()

class CollectionPropertiesView(View):
    def get(self, request, *args, **kwargs):
        collection = settings.GA_SPATIALNOSQL_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        if 'property' in kwargs:
            log.debug('listing the {property} property on {connection}:{db}:{collection}'.format(**kwargs))
            return _json_response(request, {
                kwargs['property'] : collection[ kwargs['property'] ]
            })
        else:
            log.debug('listing all properties on {connection}:{db}:{collection}'.format(**kwargs))
            return _json_response(request, dict(collection.items()))

    def post(self, request, *args, **kwargs):
        log.debug('adding the property {property} of type {dtype} to {connection}:{db}:{collection}'.format(**dict(kwargs, dtype = request.POST['dtype'])))

        collection = settings.GA_SPATIALNOSQL_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]

        if 'dtype' in request.POST and request.POST['dtype'] == 'int':
            collection[ kwargs['property'] ] = int( request.POST['value'] )
        elif 'dtype' in request.POST and request.POST['dtype'] == 'float':
            collection[ kwargs['property'] ] = float( request.POST['value'] )
        elif 'dtype' in request.POST and request.POST['dtype'] == 'str':
            collection[ kwargs['property'] ] = request.POST['value']
        elif 'dtype' in request.POST and request.POST['dtype'] == 'object':
            collection[ kwargs['property'] ] = collection.deserialize( request.POST['value'] )
        elif 'dtype' not in request.POST:
            collection[ kwargs['property'] ] = request.POST['value']
        else:
            return HttpResponseBadRequest('unknown datatype for property')

        return HttpResponse()

    def put(self, request, *args, **kwargs):
        return self.post(request, *args, **kwargs)

    def delete(self, request, *args, **kwargs):
        collection = settings.GA_SPATIALNOSQL_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        del collection[ kwargs['property'] ]
        return HttpResponse()


class ObjectView(View):
    def get(self, request, *args, **kwargs):
        print request.path
        collection = settings.GA_SPATIALNOSQL_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        object = collection.get( kwargs['object'] )
        object = collection.serialize(object)
        return _json_response(request, object)

    def post(self, request, *args, **kwargs):
        collection = settings.GA_SPATIALNOSQL_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        if len(request.POST) == 1 and 'object' in request.POST:
            object = collection.deserialize(request.POST['object'])
        else:
            object = dict(request.POST)
        collection.save(object)

    def put(self, request, *args, **kwargs):
        collection = settings.GA_SPATIALNOSQL_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        if len(request.POST) == 1 and 'object' in request.POST:
            object = collection.deserialize(request.POST['object'])
        else:
            object = dict(request.POST)

        original = collection.get( kwargs['object'] )
        for key, value in object:
            original[key] = value

        return _json_response(request, collection.serialize(original))

    def delete(self, request, *args, **kwargs):
        collection = settings.GA_SPATIALNOSQL_CONNECTIONS[ kwargs['connection'] ][ kwargs['db'] ][ kwargs['collection'] ]
        collection.delete_feature(kwargs['object'])
        return HttpResponse()

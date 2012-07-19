from django.contrib.gis.gdal.error import OGRException
from django.contrib.gis.gdal import SpatialReference
from ga_spatialnosql.index import GeoIndex
from ga_spatialnosql import app_settings as settings
from bson import objectid
import json
from bson import json_util
from django.contrib.gis.geos import GEOSGeometry
import UserDict

def _from_objectid(oid):
    return str(oid)

def _to_objectid(oid):
    return objectid.ObjectId(oid)

def _fix_dict_geometry(g):
    if isinstance(g, dict):
        return GEOSGeometry(json.dumps(g))

def _chunk(seq, size=1000):
    chunk = []
    N = 0
    for it in seq:
        chunk.append(it)
        N += 1
        if N == size:
            yield chunk
            N=0
            del chunk[:]
    if chunk:
        yield chunk

class ValidationError(Exception):
    pass

class GeoJSONCollection(object, UserDict.DictMixin):
    def _normalize_srs(self, fcrs):
        # untested, but finished

        crs = None
        reproject = False # assume we're not going to reproject unless proven otherwise

        try: # look for a CRS in the feature or featurecollection
            if (isinstance(fcrs, str) or isinstance(fcrs, unicode)) :  # if it's a string, we can check to see if it matches the original.
                crs = SpatialReference(str(fcrs))                                                              # if it doesn't, set a new one
                reproject = True                                                                          # and reproject
            elif isinstance(fcrs, int) and fcrs == self.srid:
                reproject = False
            elif not isinstance(fcrs, SpatialReference):                                                  # if it's not a string, make sure it's a SpatialReference object.
                raise ValidationError("feature's 'crs' cannot be interpreted as a SpatialReference by OGR") # otherwise whine.
            else:
                crs = fcrs
        except OGRException as e:
            raise ValidationError("feature's 'crs' cannot be interpreted as a SpatialReference by OGR:\n"+ str(e) + "\n" + fcrs)

        return reproject, crs

    def _get_real_geometry(self, feature):
        # untested, but finished
        # start by assuming we're just using our own CRS.

        fc = feature['type'] == 'FeatureCollection' # if we have a feature collection we have to iterate over all the features
        crs, reproject = self._normalize_srs(feature['crs']) if 'crs' in feature else (self.srid, False)

        if fc: # if we have a feature collection, collect the geometry
            geometry = (GEOSGeometry(json.dumps(fs['geometry']), srid=crs) for fs in feature['features'])
            if reproject:                                        # if we have to reproject
                for i, f in enumerate(feature['features']):      # iterate over all the features
                    geometry[i].transform(self.srid)              # transform their geometry in place
                    f['geometry'] = json.loads(geometry[i].json) # and change the geometry itself in the data
                feature['crs'] = self.srid
        else: # we have only a single feature
            geometry = GEOSGeometry(json.dumps(feature['geometry']), srid=crs)
            if reproject:
                geometry.transform(self.crs)                    # reproject the feature if necessary
                feature['geometry'] = json.loads(geometry.json) # and change the geometry itself in the data.
                feature['crs'] = self.srid

        return fc, feature, geometry                            # return a tuple of featurecollection:bool, the original dict, and the GEOS geometry.

    def __init__(self, db, collection, index_path=None, srid=None, fc=None, clear=False):
        # pull collection metadata from the database if it exists
        if clear:
            GeoIndex(index_path, collection, _from_objectid, _to_objectid, srid=srid if srid else 4326).drop()
            db[collection].drop()
            db.geojson__meta.remove(collection)

        self.collection_metadata = db.geojson__meta
        self.meta = db.geojson__meta.find_one(collection) or {
            'srid' : srid,
            'index_path' : index_path,
            '_id' : collection,
            'properties' : {}
        }
        self.collection_metadata.save(self.meta)
        if collection not in db.collection_names():
            db.create_collection(collection)


        if fc and not self.meta['srid'] and 'crs' in fc:
            self.srid = fc['crs'] # however this means crs must conform to an integer srid.  This is not preferred by the spec but necessary here.

        # instantiate GeoIndex for collection
        self.index = GeoIndex(index_path, collection, _from_objectid, _to_objectid, srid=srid if srid else 4326)
        self.coll = db[collection]

        if fc:
            self.insert_features(fc, replace=True)

    @property
    def srid(self):
        return self.meta['srid']

    @srid.setter
    def srid(self, srid):
        self.meta['srid'] = srid
        self.collection_metadata.save(self.meta)

    def __setitem__(self, key, value):
        self.meta['properties'][key] = value
        self.collection_metadata.save(self.meta)

    def __getitem__(self, key):
        return self.meta['properties'][key]

    def __delitem__(self, key):
        del self.meta['properties'][key]
        self.collection_metadata.save(self.meta)

    def keys(self):
        return self.meta['properties'].keys()

    def drop(self):
        if hasattr(self, 'index'):
            self.index.drop()

        if hasattr(self, 'coll'):
            self.coll.drop()

        if hasattr(self, 'meta'):
            self.collection_metadata.remove(self.meta['_id'])

    def insert_features(self, fc, replace=False, **kwargs):

        if(hasattr(fc, 'keys')):
            is_fc, fc, geometry = self._get_real_geometry(fc)

            if is_fc:
                features = fc['features']
                del fc['features']

                if 'crs' in fc:
                    del fc['crs']
                fc['crs'] = self.srid

                if replace:
                    self.meta['properties'] = fc
                    self.collection_metadata.save(self.meta)
                else:
                    for k, v in filter(lambda (x,y): x not in ('crs','type'), fc.items()):
                        if k not in self:
                            self[k] = v
                        elif v != self[k]:
                            raise KeyError("{k} already in feature collection and is not equal")

                fcid = self.meta['_id']
                for f in features:
                    f['_parent'] = fcid

                ids = self.coll.insert(features)

                self.index.bulk_insert(zip(ids, geometry))
            else:
                oid = self.coll.insert(fc, **kwargs)
                self.index.insert(oid, geometry)
        else:
            fs = []
            gs = []
            for f in fc:
                _, f, geometry = self._get_real_geometry(f)
                fs.append(f)
                gs.append(geometry)
                f['_parent'] = self.meta["_id"]


            oids = self.coll.insert(fs)
            self.index.bulk_insert(zip(oids, geometry))


    def delete_feature(self, oid):
        self.index.delete(oid)
        self.coll.remove(oid)

    def delete_features(self, geo_spec=None, spec=None):
        # TODO use a generator function so I can use bulk_delete()

        features = _chunk(f['_id'] for f in self.find_features(geo_spec=geo_spec, spec=spec, fields=['_id']))
        for chunk in features:
            for f in chunk:
                self.index.delete(f)
            self.coll.remove({"_id" : { "$in" : chunk}})

    def update(self, spec, document, upsert=False, manipulate=False, safe=False, multi=False, no_geometry=False, **kwargs):
        # TODO use a generator function so I can use bulk insert
        self.coll.update(spec, document, upsert, manipulate, safe, multi, **kwargs)
        features = self.coll.find(spec)

        if no_geometry is False:
            for feature in features:
                _, feature, geometry = self._get_real_geometry(feature)
                self.coll.save(feature)
                self.index.insert(feature['_id'], geometry)



    def _find_geo(self, geo_spec):
        geo_ids = None
        for operator, geometry in geo_spec.items():
            if not (isinstance(geometry, tuple) or isinstance(geometry, list)):
                geometry = [geometry]
            if geo_ids is None:
                geo_ids = set(self.index.__getattribute__(operator[1:])(*geometry))
            else:
                geo_ids = geo_ids.intersection(set(self.index.__getattribute__(operator)(*geometry)))

        if geo_ids is None:
            geo_ids = set()
        return geo_ids

    def find_as_collection(self, **kawrgs):
        ret = {
            "type" : "FeatureCollection",
            "features" : self.find_features(**kwargs)
        }
        for k,v in self.collection_metadata['properties']:
            ret[k] = v
        return ret

    def find_features(self, geo_spec=None, spec=None, fields=None, skip=0, limit=0, timeout=True, snapshot=False, tailable=False, sort=None, max_scan=None,
             as_class=None, slave_okay=False, await_data=False, partial=False, manipulate=True, **kwargs):

        if geo_spec and spec:
            geo_ids = _chunk(self._find_geo(geo_spec))
            for chunk in geo_ids:
                r = self.coll.find(
                        {"$and" : [spec, { "_id" : { "$in" : chunk }}]},
                    fields=fields,
                    skip=skip,
                    timeout=timeout,
                    snapshot=snapshot,
                    tailable=tailable,
                    sort=sort,
                    max_scan=max_scan,
                    as_class=as_class,
                    slave_okay=slave_okay,
                    await_data=await_data,
                    partial=partial,
                    manipulate=manipulate,
                    **kwargs
                )
                for i in r:
                    yield i
        elif geo_spec:
            geo_ids = _chunk(self._find_geo(geo_spec))
            for chunk in geo_ids:
                r = self.coll.find(
                    spec={ '_id' : {'$in' : chunk}},
                    fields=fields,
                    skip=skip,
                    timeout=timeout,
                    snapshot=snapshot,
                    tailable=tailable,
                    sort=sort,
                    max_scan=max_scan,
                    as_class=as_class,
                    slave_okay=slave_okay,
                    await_data=await_data,
                    partial=partial,
                    manipulate=manipulate,
                    **kwargs
                )
                for i in r:
                    yield i
        else:
            iter = self.coll.find(
                spec=spec,
                fields=fields,
                skip=skip,
                limit=limit,
                timeout=timeout,
                snapshot=snapshot,
                tailable=tailable,
                sort=sort,
                max_scan=max_scan,
                as_class=as_class,
                slave_okay=slave_okay,
                await_data=await_data,
                partial=partial,
                manipulate=manipulate,
                **kwargs
            )
            for o in iter:
                yield o

    def get(self, key):
        return self.coll.find_one(key)

    def count(self, geom=None):
        if not geom:
            return self.index.count()
        else:
            return self.index.count(geom)

    def create_index(self, *args, **kwargs):
        return self.coll.create_index(*args, **kwargs)

    def ensure_index(self, *args, **kwargs):
        return self.coll.ensure_index(*args, **kwargs)

    def drop_index(self, index_or_name):
        return self.coll.drop_index(index_or_name)

    def drop_indexes(self):
        return self.coll.drop_indexes()

    def reindex(self):
        return self.coll.reindex()

    def index_information(self):
        return self.coll.index_information()

    def options(self):
        return self.coll.options()

    def group(self, *args, **kwargs):
        return self.coll.group(*args, **kwargs)

    def rename(self, *args, **kwargs):
        return self.coll.rename(*args, **kwargs)

    def distinct(self, key):
        return self.coll.distinct(key)

    def map_reduce(self, *args, **kwargs):
        return self.coll.map_reduce(*args, **kwargs)

    def inline_map_reduce(self, *args, **kwargs):
        return self.coll.inline_map_reduce(*args, **kwargs)

    def find_and_modify(self, *args, **kwargs):
        return self.coll.find_and_modify(*args, **kwargs)

    @classmethod
    def serialize(cls, dictlike):
        json.dumps(dictlike, default=json_util.default)

    @classmethod
    def deserialize(cls, js):
        return json.loads(js, object_hook=json_util.object_hook)

class Database(object, UserDict.DictMixin):
    def __init__(self, db):
        self._db = db
        self._collections = {}

    def __getitem__(self, key):
        if key not in self._collections:
            self._collections[key] = GeoJSONCollection(self._db, key, settings.INDEX_PATH, settings.DEFAULT_SRID)
        return self._collections[key]

    def __setitem__(self, key, value):
        if value is not None:
            raise NotImplemented()
        else:
            a = self[key]

    def __delitem__(self, key):
        self[key].drop()

    def keys(self):
        return self._db.collection_names()

class Connection(object, UserDict.DictMixin):
    def __init__(self, connection):
        self._connection = connection

    def __getitem__(self, key):
        return Database(self._connection[key])

    def __setitem__(self, key, value):
        if value is not None:
            raise NotImplemented()
        else:
            a = self._connection[key]

    def __delitem__(self, key):
        self._connection.drop_database(key)

    def keys(self):
        return self._connection.database_names()

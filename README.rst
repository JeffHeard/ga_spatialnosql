ga_spatialnosql - Spatial extensions for non-spatial NoSQL databases
####################################################################

Introduction
============

This is a set of extensions for adapting NoSQL databases with fast primary key
lookup to spatial databases with all the standard SQLMM types. Right now
MongoDB is supported.  Redis will be supported in the near future.  

This is a relocatable Django app.  It requires at a minimum pymongo and
pyspatialite (which requires Spatialite).

This is alpha software and should be considered a feature preview rather than 
something you would use in an everyday environment.  Performance problems, bugs,
and outright lies are probably going to be relatively common.  Caveat
download-or.  

Usage
=============

There are two main pieces to this software.  First is ``ga_spatialnosql.Index``
which is a simple spatial index based on Spatialite that allows you to index
primary keys in another collection (an object_id) by geography.  I plan to make
this index spatiotemporal in the next release so you can index by both
geography and time.  The other piece to this is the NoSQL data store, currently
PyMongo-based.  Edit the ``connections.py`` and the ``app_settings.py`` to
point to your current MongoDB installation and an extant directory for the
ancillary indexes to be stored.  Once you've done that, you can instantiate
GeoJSONCollection objects and insert features and query like this::

    coll = GeoJSONCollection(db, collection, index_path, srid=None, fc=None, clear=False) # for already existing collections, srid, fc, and clear can be left off

    coll.insert_features(some_geojson_featurecollection) # this can be a string or a python dict as long as it conforms to the GeoJSON spec exactly.

    coll.find_features({
       "type" : "Feature",
       "geometry" : { "type" : "Point", "coordinates" : [0,0] },
       "properties" : {
           "query" : { "tags__contains" : "managers" },
           "fields" : ["name", "address", "phone_number"],
           "geographic_operator" : "overlaps",
           "limit" : 0
       }
    })

    # this can also be a JSON string or a python dict

The find_features syntax does not require a GeoJSON document, so long as there
is a "properties" containing a query spec that contains a valid query spec in
"query" and otherwise contains any or all of the keyword arguments available in
PyMongo's find() method However if it is a GeoJSON document, there are a myriad
of query options.  A geographic query must have at a minimum a "type" of
"Feature", a "geometry" property, and in the "properties" property, you must
have a "geographic_operator".  Supported values for the geographic operator are
those supported in Spatialite 3.0.1. For more information, consult the
spatialite documentation:

    * relate - Relation (must have a "relation" property in the query as well
    * bboverlaps - Bounding box overlaps
    * bbcontains - Bounding box of the query object contains the stored object
    * contains - Query object contains the stored object
    * overlaps - Query object overlaps the stored object
    * contained - Query object is contained by the stored object
    * containsproperly - Query object contains the stored object with no point touching the boundary
    * coveredby - Query object is covered completely by the stored object (see GEOS or Shapely documentation) 
    * covers - Query object covers completely the stored object
    * crosses - Query object crosses the stored object
    * disjoint - Query object never touches the stored object
    * equals - Query object is nearly equal to the stored object
    * exact - Query object is exactly equal to the stored object
    * intersects - Query object and the stored object have areas in common
    * touches - Query object and the stored object touch points but do not overlap
    * within - Stored objects are entirely within the query object.

GeometryCollections are treated specially.  If you have a geometry collection,
then your properties object must have a "geographic_operators" property with a
list of operator names selected from above exactly the same length as your
geometry collection.  Each operator corresponds to its respective Geometry.
(Note nested geometrycollections are not allowed).  These geometries and their
respective operators are ANDed in order of appearance.  

Additionally, if a "crs" attribute is in the document and corresponds to an
integer SRID (sorry, this is not to spec, but that's the best I can do for
now), then the **input** geometry will be transformed to the collection's
native SRS.  The **output** documents will still be in the collection's own
native SRS.

The return vaue of find_features is a GeoJSON compliant list of Python
dictionaries, giving you the opporunity to manipulate the collection before
using it.  

The GeoJSONCollection object also supports setting collection-wide metadata
properties through the ``dict`` interface.  This means that this::

    coll['owner'] = 'jeff'

Sets a global property on the collection which can be retrieved or deleted at
any time by the standard Python dict interface.   The full UserDict interface
is supported, and ``coll.keys()`` enumerates all the metadata properties of the
collection.  The only restriction on the metadata properties is that the values
must all be JSON serializable.  No Python objects for now, although a Pickle
interface is being considered.

Vision
===================

The next steps for this application will be to make a RESTful web API.  It's
already there if you care to look for it in ``api.py`` but it is neither final
nor documented.  When the final API is ready, I will create an "authenticated"
API that sorts things out something like this::

   /username/collection_name/1/ - refers to oid in a collection owned by the Django user username, stored in the user's own personal MongoDB database
   /username/collection_name/properties/propname
   /username/collection_name/?q=JSON_DOCUMENT (GET or POST)
   /username/collection_name/ POST to insert, PUT to update
   /username/ - list collections
   / - list users



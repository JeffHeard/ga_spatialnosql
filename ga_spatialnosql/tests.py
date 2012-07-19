"""
This file demonstrates writing tests using the unittest module. These will pass
when you run "manage.py test".

Replace this with more appropriate tests for your application.
"""

from django.test import TestCase
from unittest import skip
from ga_spatialnosql.db.pymongo import GeoJSONCollection
from ga_spatialnosql.index import GeoIndex
from django.contrib.gis.geos import Polygon
from pyspatialite.dbapi2 import IntegrityError
import pymongo
import json

poly0 = Polygon( ((1000,1000), (1000, 1001), (1001, 1001), (1001, 1000), (1000, 1000)) )
poly1 = Polygon( ((0.0, 0.0), (0.0, 50.0), (50.0, 50.0), (50.0, 0.0), (0.0, 0.0)) )
poly2 = Polygon( ((0.0, 0.0), (0.0, -50.0), (-50.0, -50.0), (-50.0, 0.0), (0.0, 0.0)) )
poly3 = Polygon( ((50.0, 50.0), (50.0, 150.0), (150.0, 150.0), (150.0, 50.0), (50.0, 50.0)) )

cover = Polygon( ((-50.0, -50.0), (-50.0, 150.0), (150.0, 150.0), (150.0, -50.0), (-50.0, -50.0)) )

@skip('skipping geoindex tests for now')
class GeoIndexTests(TestCase):
    def test_insert_one(self):
        ix = GeoIndex('./test_indices', 'test_insert_one', int, int)
        ix.insert(1L, poly1)

        x = ix.within(cover)
        x = list(x)
        self.assertEqual(len(x), 1)
        self.assertEqual(x[0], 1L, "Geom in index doesn't overlap")

        x = ix.equals(poly1)
        x = list(x)
        self.assertEqual(len(x), 1)
        self.assertEqual(x[0], 1L, "Geom in index isn't exact")

        ix.drop()

    def test_insert_multiple(self):
        ix = GeoIndex('./test_indices', 'test_insert_multiple', int, int)
        ix.bulk_insert([(1, poly1), (2, poly2), (3, poly3)])

        x = ix.equals(poly1)
        x = list(x)
        self.assertEqual(len(x), 1)
        self.assertEqual(x[0], 1, "Geom in index for poly 1 isn't exact")
        x = ix.equals(poly2)
        x = list(x)
        self.assertEqual(len(x), 1)
        self.assertEqual(x[0], 2, "Geom in index for poly 2 isn't exact")
        x = ix.equals(poly3)
        x = list(x)
        self.assertEqual(len(x), 1)
        self.assertEqual(x[0], 3, "Geom in index for poly 3 isn't exact")

        x = ix.within(cover)
        x = list(x)
        self.assertEqual(len(x), 3)
        self.assertEqual(sorted(x), [1,2,3], "Didn't pull back all ids")

        ix.drop()

    def test_delete_id(self):
        ix = GeoIndex('./test_indices', 'test_delete_id', int, int)

        ix.insert(1L, poly1)
        ix.insert(2L, poly2)
        ix.insert(3L, poly3)

        ix.delete(1)


        x = ix.within(cover)
        x = list(x)
        self.assertEqual(len(x), 2, "didn't drop one like we should have")
        self.assertEqual(sorted(x), [2,3], "Didn't pull back all ids")

        ix.bulk_delete([2,3])
        x = ix.within(cover)
        x = list(x)
        self.assertEqual(len(x) ,0)

        ix.drop()

    def test_multiple_indexes(self):
        ix1 = GeoIndex('./test_indices', 'test_multiple_indexes', int, int)
        ix2 = GeoIndex('./test_indices', 'test_multiple_indexes', int, int)

        ix1.insert(1L, poly1)
        ix2.insert(2L, poly2)
        ix1.insert(3L, poly3)

        x = ix1.intersects(cover)
        x = list(x)
        y = ix2.intersects(cover)
        y = list(y)

        self.assertEqual(len(x), 3, "Didn't pull back all ids with first index")
        self.assertEqual(sorted(x), [1,2,3], "Didn't pull back all ids with first index")
        self.assertEqual(len(y), 3, "didn't pull back all ids with second index")
        self.assertEqual(sorted(y), [1,2,3], "Didn't pull back all ids with second index")

        ix1.drop()
        ix2.drop()

    def test_uniquity(self):
        ix = GeoIndex('./test_indices', 'test_unique', int, int)

        ix.insert(1L, poly1)
        self.assertTrue(ix.exists(1))

        try:
            self.assertRaises(IntegrityError, ix.insert(1, poly1))
        except IntegrityError:
            pass

        try:
            self.assertRaises(IntegrityError, ix.bulk_insert([(1,poly1), (1,poly1)]))
        except IntegrityError:
            pass

        ix.drop()

mongo = pymongo.Connection()

@skip('skipping mongodb tests for now')
class GeoMongoTests(TestCase):
    def setUp(self):
        self.collection = GeoJSONCollection(mongo.test, 'test_collection', './test_indices', srid=4326, clear=True)
        self.collection.insert_features({
            'type' : "FeatureCollection",
            'property3' : 3,
            "features" : [
                { 'type' : 'Feature', 'geometry' : json.loads(poly1.json), 'properties' : {'name' : 'poly1'} },
                { 'type' : 'Feature', 'geometry' : json.loads(poly2.json), 'properties' : {'name' : 'poly2'} },
                { 'type' : 'Feature', 'geometry' : json.loads(poly3.json), 'properties' : {'name' : 'poly3'} }
            ]
        })

    def tearDown(self):
        self.collection.drop()

    def test_load(self):
        """Test loading with a few features"""
        self.assertEquals(self.collection.count(), 3, 'count() failed basic test')
        self.assertEquals(self.collection.count(cover), 3, "count() failed cover test")

    def test_geoquery(self):
        result = list(self.collection.find_features(geo_spec={'$within' : cover}))
        self.assertEquals(len(result),3)
        self.assertEquals(result[0]['properties']['name'], 'poly1')
        self.assertEquals(result[1]['properties']['name'], 'poly2')
        self.assertEquals(result[2]['properties']['name'], 'poly3')

    def test_combined_query(self):
        result = list(self.collection.find_features(geo_spec={'$within' : cover}, spec={'properties.name' : 'poly1'}))
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0]['properties']['name'], 'poly1')

    def test_plain_query(self):
        """Query based on properties instead of geometry"""
        result = list(self.collection.find_features(spec={'properties.name' : 'poly1'}))
        result2 = list(self.collection.find_features())
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0]['properties']['name'], 'poly1')
        self.assertEquals(len(result2), 3)

    def test_empty_resultset(self):
        """Make a query that comes out with nothing"""
        result1 = list(self.collection.find_features(spec={'properties.name' : 'nonexistent'}))
        result2 = list(self.collection.find_features(spec={'properties.name' : 'nonexistent'}, geo_spec={'$within' : cover}))
        result3 = list(self.collection.find_features(spec={'properties.name' : 'nonexistent'}, geo_spec={'$within' : poly0}))
        result4 = list(self.collection.find_features(geo_spec={'$within' : poly0}))

        self.assertEquals([], result1)
        self.assertEquals([], result2)
        self.assertEquals([], result3)
        self.assertEquals([], result4)


    def test_insert_individual_features(self):
        """Test the insertion of individual features instead of feature collection"""
        self.collection.insert_features([{ 'type' : 'Feature', 'geometry' : json.loads(poly1.json), 'properties' : {'name' : 'poly4'} }])
        result = list(self.collection.find_features(spec={'properties.name' : 'poly4'}))
        self.assertEqual(len(result), 1)


    def test_properties(self):
        """Test loading a feature-collection with extra properties."""
        self.collection['property1'] = 1
        self.collection['property2'] = 2
        self.assertEquals(self.collection['property1'], 1)
        self.assertEquals(self.collection['property2'], 2)

        del self.collection['property1']
        self.assertFalse('property1' in self.collection.meta['properties'])
        self.assertFalse('property1' in self.collection)

        self.assertEquals(len(self.collection.keys()), 2)

    def test_delete_feature(self):
        """Test deleting a feature"""
        result = list(self.collection.find_features(spec={'properties.name' : 'poly1'})) [0]
        self.collection.delete_feature(result['_id'])

    def test_delete_features(self):
        """Test deleting some features"""
        self.collection.delete_features(geo_spec={"$within" : cover})
        result1 = list(self.collection.find_features(geo_spec={'$within' : cover}))
        result2 = list(self.collection.find_features(spec={'properties.name' : 'poly1'}))

        self.assertEquals([], result1)
        self.assertEquals([], result2)


class BasicApiTests(TestCase):
    def test_db_list(self):
        db_list = self.client.get('/base/test_connection/')
        self.assertEqual(db_list.status_code, 200, db_list.content)
        if db_list.content:
            dbs = json.loads(db_list.content)
            for db in dbs:
                self.client.delete('/base/test_connection/' + db)

        # create a collection as a POST with properties
        r1 = self.client.post('/base/test_connection/test_db/', data={
            'collection' : 'test_collection1',
            'properties' : json.dumps({
                'prop1' : 'str',
                'prop2': 2,
                'prop3' : [],
                'prop4' : {}
            })
        })
        self.assertEqual(r1.status_code, 200, r1.content)

        # create a collection as a POST without properties
        r2 = self.client.post('/base/test_connection/test_db/', data= {'collection' : 'test_collection2'})
        self.assertEqual(r2.status_code, 200, r2.content)

        # create a collection as a GET incidentally
        r3 = self.client.get('/base/test_connection/test_db/test_collection3/')
        self.assertEqual(r3.status_code, 200, r3.content)

        # make sure the one we created with properties retains those properties
        properties_list = self.client.get('/base/test_connection/test_db/test_collection1/properties/')
        self.assertEqual(properties_list.status_code, 200, properties_list.content)
        properties = json.loads(properties_list.content)
        self.assertIn('prop1', properties)
        self.assertIn('prop2', properties)
        self.assertIn('prop3', properties)
        self.assertIn('prop4', properties)
        self.assertEqual(properties['prop1'], 'str')
        self.assertEqual(properties['prop2'], 2)
        self.assertEqual(properties['prop3'], [])
        self.assertEqual(properties['prop4'], {})

        prop1 = self.client.get('/base/test_connection/test_db/test_collection1/properties/prop1/')
        self.assertEqual(prop1.status_code, 200)
        prop1 = json.loads(prop1.content)
        self.assertEqual(prop1['prop1'], 'str')

        # make sure all of them are in the collection list now
        collection_list = self.client.get('/base/test_connection/test_db/')
        self.assertEqual(collection_list.status_code, 200, collection_list.content)
        collections = json.loads(collection_list.content)
        self.assertIn('test_collection1',collections)
        self.assertIn('test_collection2',collections)
        self.assertIn('test_collection3',collections)

        # make sure that when we get a lsiting of databases our test_db is in there
        db_list = self.client.get('/base/test_connection/')
        self.assertEqual(db_list.status_code, 200, db_list.content)
        dbs = json.loads(db_list.content)
        self.assertIn('test_db', dbs)

        # make sure that when we get a listing of connections our test connection is in there
        connection_list = self.client.get('/base/')
        self.assertEqual(connection_list.status_code, 200, connection_list.content)
        conns = json.loads(connection_list.content)
        self.assertIn('test_connection', conns)

        # delete all our collections and make sure they were deleted
        self.client.delete('/base/test_connection/test_db/test_collection1/')
        self.client.delete('/base/test_connection/test_db/test_collection2/')
        self.client.delete('/base/test_connection/test_db/test_collection3/')

        collection_list = self.client.get('/base/test_connection/test_db/')
        self.assertEqual(collection_list.status_code, 200, collection_list.content)
        collections = json.loads(collection_list.content)
        self.assertNotIn('test_collection1',collections)
        self.assertNotIn('test_collection2',collections)
        self.assertNotIn('test_collection3',collections)

        # delete the database and make sure it's gone
        self.client.delete('/base/test_connection/test_db/')
        db_list = self.client.get('/base/test_connection/')
        self.assertEqual(db_list.status_code, 200, db_list.content)
        dbs = json.loads(db_list.content)
        self.assertNotIn('test_db',dbs)

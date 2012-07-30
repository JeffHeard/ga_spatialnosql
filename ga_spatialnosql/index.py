
__author__ = 'jeff'

import os
from pyspatialite import dbapi2 as db

class Transaction(object):
    def __init__(self, db, editing=False):
        self.db = db
        self.editing = editing

    def __enter__(self):
        self.cursor = self.db.cursor()
        return self

    def __exit__(self, extype, ex, val):
        if self.editing:
            self.db.commit()
            self.cursor.close()

    def execute(self, *args, **kwargs):
        self.cursor.execute(*args, **kwargs)
        return self

    def executemany(self, *args, **kwargs):
        self.cursor.executemany(*args, **kwargs)
        return self

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchmany(self):
        return self.cursor.fetchmany()

    def __iter__(self):
        for item in self.cursor:
            yield item
        self.cursor.close()

    def __del__(self):
        self.cursor.close()

def _select_basic(function):
    def selection(self, geom, srid=None):
        if srid:
            geom.srid = srid
        elif not hasattr(geom, 'srid'):
            geom.srid = self.srid
        elif not geom.srid or geom.srid == -1:
            geom.srid = self.srid

        with self.cursor() as c:
            c.execute('select oid from spatialindex_geometry where {function}(geom, GeomFromWkb(?, ?))'.format(function=function), (geom.wkb, geom.srid))
            for item in c:
                yield self._to_id(item[0])

    return selection

def _select_relate(relation):
    def selection(self, geom, srid=None):
        if srid:
            geom.srid = srid
        elif not hasattr(geom, 'srid'):
            geom.srid = self.srid
        elif not geom.srid or geom.srid == -1:
            geom.srid = self.srid

        with self.cursor() as c:
            c.execute('select oid from spatialindex_geometry where Relate(geom, GeomFromWkb(?, ?), ?)', (geom.wkb, geom.srid, relation))
            for item in c:
                yield self._to_id(item[0])

    return selection


class GeoIndex(object):
    def __init__(self, path, name, from_id, to_id, srid=4326, clear=False, cardinality=2):
        self.index_path = os.path.join(path, name + '.spatialite')
        self._from_id = from_id
        self._to_id = to_id
        self.srid = srid
        self.clear = clear
        self.cardinality=cardinality

        if clear:
            self.drop()

        self.open()

    def open(self):
        new = not os.path.exists(self.index_path)
        self.db = db.connect(self.index_path)

        if new:
            with self.transaction() as c:
                c.execute('SELECT InitSpatialMetadata()') # TODO this doesn't actually seem to load the SRID sql.  Should probably find and load that...
                c.execute("CREATE TABLE spatialindex_geometry (oid text unique not null primary key)")
                c.execute("SELECT AddGeometryColumn('spatialindex_geometry', 'geom', ?, 'GEOMETRY', ?)", (self.srid, 'XY' if self.cardinality==2 else 'XYZ'))
                c.execute("SELECT CreateSpatialIndex('spatialindex_geometry', 'geom')")
                c.execute("CREATE INDEX forward_index ON spatialindex_geometry (oid)")

    @property
    def bounds(self):
        with self.cursor() as c:
            c.execute('''SELECT
                MIN(idx.xmin) As bxmin,
                MIN(idx.ymin) As bymin,
                MAX(idx.xmax) As bxmax,
                MAX(idx.ymax) As bymax
            from spatialindex_geometry as t
            INNER JOIN idx_spatialindex_geometry_geom as idx on t.rowid = idx.pkid''')
            return c.fetchall()[0]


    def close(self):
        self.db.close()

    def drop(self):
        if os.path.exists(self.index_path):
            os.unlink(self.index_path)

    def __del__(self):
        self.db.close()

    def cursor(self):
        transaction = Transaction(self.db)
        return transaction

    def transaction(self):
        return Transaction(self.db, editing=True)

    def clear(self):
        with self.transaction() as c:
            c.execute('delete from spatialindex_geometry')

    def bulk_insert(self, oids_and_geoms):
        with self.transaction() as c:
            c.executemany('INSERT INTO spatialindex_geometry (oid, geom) VALUES (?,GeomFromWKB(?, ?))', ((self._from_id(oid), geom.wkb, self.srid) for oid, geom in oids_and_geoms))

    def insert(self, oid, geom, srid=None):
        if srid:
            geom.srid = srid
        elif not hasattr(geom, 'srid'):
            geom.srid = self.srid
        elif not geom.srid or geom.srid == -1:
            geom.srid = self.srid

        with self.transaction() as c:
            c.execute('insert into spatialindex_geometry (oid, geom) values (?, GeomFromWKB(?, ?))', (self._from_id(oid), geom.wkb, geom.srid))

    def exists(self, oid):
        oid = self._from_id(oid)
        with self.cursor() as c:
            c.execute('select oid from spatialindex_geometry where oid = ?', (oid,))
            return c.cursor.arraysize > 0

    def delete(self, oid):
        with self.transaction() as c:
            c.execute('delete from spatialindex_geometry where oid=?', (self._from_id(oid),))

    def bulk_delete(self, oids):
        with self.transaction() as c:
            c.executemany('delete from spatialindex_geometry where oid=?', ((self._from_id(oid),) for oid in oids))

    def count(self, geom=None):
        with self.cursor() as c:
            if geom is None:
                c.execute('select count(oid) from spatialindex_geometry')
                return c.fetchone()[0]
            else:
                c.execute('select count(oid) from spatialindex_geometry where Within(geom, GeomFromWkb(?, ?))', (geom.wkb, geom.srid))
                return c.fetchone()[0]

    def relate(self, relation, geom, srid=None):
        if srid:
            geom.srid = srid
        elif not hasattr(geom, 'srid'):
            geom.srid = self.srid
        elif not geom.srid or geom.srid == -1:
            geom.srid = self.srid

        with self.cursor() as c:
            c.execute('select oid from spatialindex_geometry where Relate(geom, GeomFromWkb(?, ?), ?)', (geom, geom.srid, relation))
            for item in c:
                yield self._to_id(item)

    bbcontains = _select_basic("mbrcontains")
    bboverlaps = _select_basic('mbroverlaps')
    contains = _select_basic('contains')
    overlaps = _select_basic('overlaps')
    contained = _select_basic('contained')
    containsproperly = _select_basic('containsproperly')
    coveredby = _select_basic('coveredby')
    covers = _select_basic('covers')
    crosses = _select_basic('crosses')
    disjoint = _select_basic('disjoint')
    equals = _select_basic('equals')
    exact =  _select_basic('exact')
    intersects = _select_basic('intersects')
    overlaps = _select_basic('overlaps')
    touches = _select_basic('touches')
    within = _select_basic('within')

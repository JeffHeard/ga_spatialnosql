__author__ = 'jeff'

import pymongo

INIT_SQL_PATH = '/Users/jeff/Source/geoanalytics/init_spatialite-2.3.sql'
INDEX_TEMPLATE_PATH = '/Users/jeff/Source/geoanalytics/spatialindex_template.spatialite'
INDEX_PATH = "/Users/jeff/Source/geoanalytics/ga/"
DEFAULT_SRID = 4326
DEFAULT_DB_QUOTA = 2**30*5 # five gigabytes


APP_LOGGING = {
    'ga_spatialnosql.db.mongo' : {
        'handlers': ['console'],
        'level': 'DEBUG',
        'propagate': False,
    },
    'ga_spatialnosql.index' : {
        'handlers': ['console'],
        'level': 'DEBUG',
        'propagate': False,
    },
}
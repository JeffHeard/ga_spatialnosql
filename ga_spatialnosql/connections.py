from ga_spatialnosql.db.mongo import Connection
import pymongo

CONNECTIONS = {
    'test_connection' : Connection( pymongo.Connection() )
}

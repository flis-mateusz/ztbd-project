from pymongo import MongoClient
from pymongo import MongoClient
cli = MongoClient(
        "mongodb://5b8a12db8090:27017/?replicaSet=rsLatest",
        serverSelectionTimeoutMS=5000)
db = cli.testdb


with cli.start_session() as s:
    s.start_transaction()
    before = db.users_favourite.count_documents({}, session=s)
    db.users_favourite.delete_many({}, session=s)
    s.abort_transaction()

after = db.users_favourite.count_documents({})
print("Rollback OK:", before == after)

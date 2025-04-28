import datetime
from benchmarks.core.base_test import BasePerformanceTest


class AddFavouriteTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="INSERT 1 rekord do users_favourite (user 1 ↦ recipe 1)",
            operation="CREATE")

    _sql = "INSERT INTO users_favourite (id_recipe,id_user,saved_at) VALUES (1,1,NOW());"

    def test_mysql(self):
        cur = self._mysql.cursor()
        cur.execute(self._sql)
        cur.close()
        return 1

    def test_postgres(self):
        cur = self._postgres.cursor()
        cur.execute(self._sql)
        cur.close()
        return 1

    def test_mongo(self, db):
        doc = {
            "id": db.users_favourite.estimated_document_count()+1,
            "id_recipe": 1,
            "id_user": 1,
            "saved_at": datetime.datetime.utcnow()
        }
        return db.users_favourite.insert_one(doc).inserted_id

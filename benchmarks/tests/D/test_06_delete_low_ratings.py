from benchmarks.core.base_test import BasePerformanceTest


class DeleteLowRatingsTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="Usuń wszystkie oceny niższe niż 2,0 ★",
            operation="DELETE")

    _sql = "DELETE FROM rating WHERE value < 2.0;"

    def test_mysql(self):    return self.fetch_mysql(self._sql)

    def test_postgres(self): return self.fetch_postgres(self._sql)

    def test_mongo(self, db):
        return db.rating.delete_many({"value": {"$lt": 2.0}}).deleted_count

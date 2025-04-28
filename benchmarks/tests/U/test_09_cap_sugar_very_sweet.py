from benchmarks.core.base_test import BasePerformanceTest


class CapSugarVerySweetTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="Ustaw sugars = 30 g, jeśli wartość przekracza 30 g",
            operation="UPDATE")

    _sql = "UPDATE nutrition SET sugars = 30 WHERE sugars > 30;"

    def test_mysql(self):    return self.fetch_mysql(self._sql)

    def test_postgres(self): return self.fetch_postgres(self._sql)

    def test_mongo(self, db):
        return db.nutrition.update_many(
            {"sugars": {"$gt": 30}},
            {"$set": {"sugars": 30}}
        ).modified_count

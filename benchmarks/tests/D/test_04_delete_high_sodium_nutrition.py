from benchmarks.core.base_test import BasePerformanceTest


class DeleteHighSodiumNutritionTest(BasePerformanceTest):
    """
    Usuń wiersze w tabeli nutrition, w których zawartość sodu (salt)
    przekracza 4 g na porcję.
    """

    def __init__(self):
        super().__init__(
            description="Usuń wiersze w tabeli nutrition, w których zawartość sodu (salt) przekracza 4 g",
            operation="DELETE")

    _sql = "DELETE FROM nutrition WHERE salt > 4.0;"

    def test_mysql(self):    return self.fetch_mysql(self._sql)
    def test_postgres(self): return self.fetch_postgres(self._sql)
    def test_mongo(self, db):
        return db.nutrition.delete_many({"salt": {"$gt": 4.0}}).deleted_count

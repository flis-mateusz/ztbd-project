from benchmarks.core.base_test import BasePerformanceTest


class BoostProteinLowFatTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="Zwiększ protein o 3 g, gdy fat < 10 g (tabela nutrition)",
            operation="UPDATE")

    _sql = """
        UPDATE nutrition
        SET protein = protein + 3
        WHERE fat < 10;
    """

    def test_mysql(self):    return self.fetch_mysql(self._sql)

    def test_postgres(self): return self.fetch_postgres(self._sql)

    def test_mongo(self, db):
        return db.nutrition.update_many(
            {"fat": {"$lt": 10}},
            {"$inc": {"protein": 3}}
        ).modified_count

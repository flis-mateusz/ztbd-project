from benchmarks.core.base_test import BasePerformanceTest


class AddSaltBufferHighCalorieTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="Zwiększ nutrition.salt o 0,2 g dla przepisów calories > 700 kcal",
            operation="UPDATE")

    _sql = """
        UPDATE nutrition
        SET salt = salt + 0.2
        WHERE calories > 700;
    """

    def test_mysql(self):    return self.fetch_mysql(self._sql)

    def test_postgres(self): return self.fetch_postgres(self._sql)

    def test_mongo(self, db):
        return db.nutrition.update_many(
            {"calories": {"$gt": 700}},
            {"$inc": {"salt": 0.2}}
        ).modified_count

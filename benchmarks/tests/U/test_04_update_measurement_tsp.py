from benchmarks.core.base_test import BasePerformanceTest


class UpdateMeasurementUnitTspTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="Zamień measurement='tsp' na 'teaspoon' w recipes_ingredients",
            operation="UPDATE")

    _sql = """
        UPDATE recipes_ingredients
        SET measurement = 'teaspoon'
        WHERE measurement = 'tsp';
    """

    def test_mysql(self):    return self.fetch_mysql(self._sql)

    def test_postgres(self): return self.fetch_postgres(self._sql)

    def test_mongo(self, db):
        return db.recipes_ingredients.update_many(
            {"measurement": "tsp"},
            {"$set": {"measurement": "teaspoon"}}
        ).modified_count

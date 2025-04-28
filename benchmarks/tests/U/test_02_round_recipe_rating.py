from benchmarks.core.base_test import BasePerformanceTest


class NormalizeRecipeRatingPrecisionTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="Zaokrąglij recipes.rating do jednego miejsca po przecinku",
            operation="UPDATE")

    _sql_mysql = "UPDATE recipes SET rating = ROUND(rating, 1);"
    _sql_pg = "UPDATE recipes SET rating = ROUND(rating::numeric, 1);"

    def test_mysql(self):
        return self.fetch_mysql(self._sql_mysql)

    def test_postgres(self):
        return self.fetch_postgres(self._sql_pg)

    def test_mongo(self, db):
        pipeline = [{"$set": {"rating": {"$round": ["$rating", 1]}}}]
        return db.recipes.update_many({}, pipeline).modified_count

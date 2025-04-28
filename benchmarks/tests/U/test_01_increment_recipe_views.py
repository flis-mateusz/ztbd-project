from benchmarks.core.base_test import BasePerformanceTest


class IncrementRecipeViewsTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="Zwiększ kolumnę views o 1 w przepisach z rating ≥ 4,0",
            operation="UPDATE")

    _sql = "UPDATE recipes SET views = views + 1 WHERE rating >= 4.0;"

    def test_mysql(self):    return self.fetch_mysql(self._sql)

    def test_postgres(self): return self.fetch_postgres(self._sql)

    def test_mongo(self, db):
        return db.recipes.update_many(
            {"rating": {"$gte": 4.0}},
            {"$inc": {"views": 1}}
        ).modified_count

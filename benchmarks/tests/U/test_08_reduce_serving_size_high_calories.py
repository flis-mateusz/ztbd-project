from benchmarks.core.base_test import BasePerformanceTest


class ReduceServingSizeHighCaloriesTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="Zmniejsz serving_size o 1 (min 1) dla przepisów calories > 900",
            operation="UPDATE")

    _sql_mysql = """
        UPDATE recipes r
        JOIN nutrition n ON n.id_recipe = r.id
        SET r.serving_size = GREATEST(1, r.serving_size - 1)
        WHERE n.calories > 900;
    """
    _sql_pg = """
        UPDATE recipes
        SET serving_size = GREATEST(1, serving_size - 1)
        WHERE id IN (
            SELECT id_recipe FROM nutrition WHERE calories > 900
        );
    """

    def test_mysql(self):    return self.fetch_mysql(self._sql_mysql)

    def test_postgres(self): return self.fetch_postgres(self._sql_pg)

    def test_mongo(self, db):
        return db.recipes.update_many(
            {"nutrition.calories": {"$gt": 900}},
            [{"$set": {
                "serving_size": {"$max": [1, {"$subtract": ["$serving_size", 1]}]}
            }}]
        ).modified_count

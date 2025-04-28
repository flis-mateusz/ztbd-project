from benchmarks.core.base_test import BasePerformanceTest


class MostActiveAuthorsTest(BasePerformanceTest):
    """
    Liczba przepisów stworzonych przez autora (recipes.id_user) – TOP 100.
    Test dużego GROUP BY oraz indeksu po obcym kluczu.
    """

    def __init__(self):
        super().__init__(description="Najbardziej płodni autorzy (liczba przepisów) – TOP 100")

    _sql = """
        SELECT id_user, COUNT(*) AS recipes_count
        FROM recipes
        GROUP BY id_user
        ORDER BY recipes_count DESC
        LIMIT 100;
    """

    def test_mysql(self):
        return self.fetch_mysql(self._sql)

    def test_postgres(self):
        return self.fetch_postgres(self._sql)

    def test_mongo(self, db):
        pipeline = [
            {"$group": {"_id": "$id_user", "recipes_count": {"$sum": 1}}},
            {"$sort": {"recipes_count": -1}},
            {"$limit": 100},
            {"$project": {"_id": 0, "id_user": "$_id", "recipes_count": 1}}
        ]
        return list(db.recipes.aggregate(pipeline))

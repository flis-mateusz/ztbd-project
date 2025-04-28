from benchmarks.core.base_test import BasePerformanceTest


class LongestRecipesByStepsTest(BasePerformanceTest):
    """
    Przepisy z największą liczbą kroków instrukcji – TOP 20.
    Wymaga COUNT(*) po instrukcjach.
    """

    def __init__(self):
        super().__init__(description="Najdłuższe przepisy (liczba kroków) – TOP 20")

    _sql = """
        SELECT r.id, r.title, COUNT(i.id) AS steps
        FROM recipes r
        JOIN instructions i ON i.id_recipe = r.id
        GROUP BY r.id, r.title
        ORDER BY steps DESC
        LIMIT 20;
    """

    def test_mysql(self):
        return self.fetch_mysql(self._sql)

    def test_postgres(self):
        return self.fetch_postgres(self._sql)

    def test_mongo(self, db):
        pipeline = [
            {"$project": {
                "title": 1,
                "steps": {"$size": "$instructions"}}},
            {"$sort": {"steps": -1}},
            {"$limit": 20},
            {"$project": {"_id": 0, "id": "$id", "title": 1, "steps": 1}}
        ]
        return list(db.recipes.aggregate(pipeline))

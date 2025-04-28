from benchmarks.core.base_test import BasePerformanceTest


class AvgCaloriesByMealTypeTest(BasePerformanceTest):
    """
    Średnie kalorie na porcję w każdym typie posiłku.
    Łączy recipes ↔ nutrition, grupuje po meal_type.
    """

    def __init__(self):
        super().__init__(description="Średnie kalorie wg meal_type")

    _sql = """
        SELECT r.id_meal_type, AVG(n.calories) AS avg_cal
        FROM recipes r
        JOIN nutrition n ON n.id_recipe = r.id
        GROUP BY r.id_meal_type
        ORDER BY r.id_meal_type;
    """

    def test_mysql(self):
        return self.fetch_mysql(self._sql)

    def test_postgres(self):
        a = self.fetch_postgres(self._sql)
        return a

    def test_mongo(self, db):
        pipeline = [
            {"$unwind": "$nutrition"},
            {"$group": {
                "_id": "$id_meal_type",
                "avg_cal": {"$avg": "$nutrition.calories"}}},
            {"$project": {"_id": 0, "id_meal_type": "$_id", "avg_cal": 1}},
            {"$sort": {"id_meal_type": 1}}
        ]
        return list(db.recipes.aggregate(pipeline))

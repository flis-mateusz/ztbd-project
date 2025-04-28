from benchmarks.core.base_test import BasePerformanceTest


class MostCommonIngredientsTest(BasePerformanceTest):
    """
    Top-50 składników używanych w największej liczbie przepisów.
    Duży GROUP BY → koszt sortowania i agregacji.
    """

    def __init__(self):
        super().__init__(description="Top-50 najczęstszych składników w przepisach")

    _sql = """
        SELECT i.name   AS ingredient,
               COUNT(*) AS usage_count
        FROM recipes_ingredients ri
        JOIN ingredients i ON i.id = ri.id_ingredient
        GROUP BY i.name
        ORDER BY usage_count DESC
        LIMIT 50;
    """

    def test_mysql(self):
        return self.fetch_mysql(self._sql)

    def test_postgres(self):
        return self.fetch_postgres(self._sql)

    def test_mongo(self, db):
        pipeline = [
            {"$unwind": "$ingredients"},
            {"$group": {"_id": "$ingredients.id_ingredient", "usage_count": {"$sum": 1}}},
            {"$sort": {"usage_count": -1}},
            {"$limit": 50},
            {"$lookup": {
                "from": "ingredients",
                "localField": "_id",
                "foreignField": "id",
                "as": "ing"}},
            {"$unwind": "$ing"},
            {"$project": {"_id": 0, "ingredient": "$ing.name", "usage_count": 1}},
        ]
        return list(db.recipes.aggregate(pipeline))

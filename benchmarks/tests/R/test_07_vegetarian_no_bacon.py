from benchmarks.core.base_test import BasePerformanceTest


class VegetarianNoBaconTest(BasePerformanceTest):
    """
    Przepisy z dietą Vegetarian bez składnika „Bacon”, sortowane po rating+views.
    Pokazuje koszt NOT EXISTS / $nin.
    """

    def __init__(self):
        super().__init__(description="Vegetarian bez ‘Bacon’ – TOP 100")

    _sql = """
        SELECT r.id, r.title, r.rating, r.views
        FROM recipes r
        WHERE r.id_diet = (SELECT id FROM diet WHERE type = 'Vegetarian')
          AND NOT EXISTS (
              SELECT 1
              FROM recipes_ingredients ri
              JOIN ingredients i ON i.id = ri.id_ingredient
              WHERE ri.id_recipe = r.id AND i.name = 'Bacon')
        ORDER BY r.rating DESC, r.views DESC
        LIMIT 100;
    """

    def test_mysql(self):
        return self.fetch_mysql(self._sql)

    def test_postgres(self):
        return self.fetch_postgres(self._sql)

    def test_mongo(self, db):
        bacon = db.ingredients.find_one({"name": "Bacon"})
        if not bacon:
            return []

        bacon_id = bacon["id"]

        pipeline = [
            {"$match": {
                "diet": "Vegetarian",
                "ingredients.id_ingredient": {"$ne": bacon_id}
            }},
            {"$sort": {"rating": -1, "views": -1}},
            {"$limit": 100},
            {"$project": {"_id": 0, "title": 1, "rating": 1, "views": 1}},
        ]
        return list(db.recipes.aggregate(pipeline))

from benchmarks.core.base_test import BasePerformanceTest


class TopRecipesPerCuisineTest(BasePerformanceTest):
    """
    W każdej kuchni wybierz 5 najlepiej ocenianych (rating → views) przepisów.
    Window-function (SQL) vs. $group+$push+$slice (Mongo).
    """

    def __init__(self):
        super().__init__(description="TOP-5 przepisów w każdej kuchni (rating + views)")

    _sql = """
        WITH ranked AS (
            SELECT r.id, r.title, r.rating, r.views, r.id_cuisine,
                   ROW_NUMBER() OVER (PARTITION BY r.id_cuisine
                                      ORDER BY r.rating DESC, r.views DESC) AS rk
            FROM recipes r
        )
        SELECT id, title, rating, views, id_cuisine
        FROM ranked
        WHERE rk <= 5
        ORDER BY id_cuisine, rk;
    """

    def test_mysql(self):
        return self.fetch_mysql(self._sql)

    def test_postgres(self):
        return self.fetch_postgres(self._sql)

    def test_mongo(self, db):
        pipeline = [
            {"$sort": {"rating": -1, "views": -1}},
            {"$group": {
                "_id": "$id_cuisine",
                "recipes": {"$push": {
                    "id": "$id",
                    "title": "$title",
                    "rating": "$rating",
                    "views": "$views"}}
            }},
            {"$unwind": {"path": "$recipes", "includeArrayIndex": "rk"}},
            {"$match": {"rk": {"$lt": 5}}},
            {"$project": {
                "_id": 0,
                "id_cuisine": "$_id",
                "id": "$recipes.id",
                "title": "$recipes.title",
                "rating": "$recipes.rating",
                "views": "$recipes.views"
            }},
            {"$sort": {"id_cuisine": 1, "rating": -1, "views": -1}}
        ]
        return list(db.recipes.aggregate(pipeline))


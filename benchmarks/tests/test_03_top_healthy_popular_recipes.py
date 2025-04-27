from benchmarks.core.base_test import BasePerformanceTest


class TopHealthyPopularRecipesTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(description="Znajdź najzdrowsze i najpopularniejsze przepisy (Top 50)")

    # 📘 Test: znajdź najzdrowsze i najpopularniejsze przepisy (Top 50)
    # Warunki:
    # - niskie kalorie, cukry i tłuszcz (z tabeli nutrition)
    # - wysokie oceny i dużo wyświetleń (z tabeli recipes)
    # JOIN + WHERE + ORDER + LIMIT = kosztowna operacja
    def test_mysql(self):
        return self.fetch_mysql("""
            SELECT r.id, r.title, r.rating, r.views
            FROM recipes r
            JOIN nutrition n ON r.id = n.id_recipe
            WHERE
                n.calories < 500 AND
                n.sugars < 5 AND
                n.fat < 10 AND
                r.views > 1000 AND
                r.rating > 4.0
            ORDER BY r.rating DESC, r.views DESC
            LIMIT 50;
        """)

    def test_postgres(self):
        return self.fetch_postgres("""
            SELECT r.id, r.title, r.rating, r.views
            FROM recipes r
            JOIN nutrition n ON r.id = n.id_recipe
            WHERE
                n.calories < 500 AND
                n.sugars < 5 AND
                n.fat < 10 AND
                r.views > 1000 AND
                r.rating > 4.0
            ORDER BY r.rating DESC, r.views DESC
            LIMIT 50;
        """)

    def test_mongo(self, db):
        pipeline = [
            {
                "$match": {
                    "nutrition.calories": {"$lt": 500},
                    "nutrition.sugars": {"$lt": 5},
                    "nutrition.fat": {"$lt": 10},
                    "views": {"$gt": 1000},
                    "rating": {"$gt": 4.0}
                }
            },
            {
                "$sort": {"rating": -1, "views": -1}
            },
            {"$limit": 50},
            {
                "$project": {
                    "_id": 0,
                    "title": 1,
                    "rating": 1,
                    "views": 1
                }
            }
        ]
        return list(db.recipes.aggregate(pipeline))

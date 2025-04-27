from benchmarks.core.base_test import BasePerformanceTest


class HighlyRatedUnlikedRecipesTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(description="Najlepiej oceniane przepisy, które nie zostały zapisane przez żadnego użytkownika")

    # 📘 Test: Najlepiej oceniane przepisy, które nie zostały zapisane przez żadnego użytkownika
    # Warunki:
    # - minimum 3 oceny
    # - średnia ocena > 4.5
    # - brak powiązania z users_recipes
    def test_mysql(self):
        return self.fetch_mysql("""
            SELECT r.id, r.title, AVG(rt.value) AS avg_rating, COUNT(rt.id) AS num_ratings
            FROM recipes r
            JOIN rating rt ON r.id = rt.id_recipe
            LEFT JOIN users_recipes ur ON r.id = ur.id_recipe
            WHERE ur.id IS NULL
            GROUP BY r.id, r.title
            HAVING COUNT(rt.id) >= 3 AND AVG(rt.value) > 4.5
            ORDER BY avg_rating DESC
            LIMIT 50;
        """)

    def test_postgres(self):
        return self.fetch_postgres("""
            SELECT r.id, r.title, AVG(rt.value) AS avg_rating, COUNT(rt.id) AS num_ratings
            FROM recipes r
            JOIN rating rt ON r.id = rt.id_recipe
            LEFT JOIN users_recipes ur ON r.id = ur.id_recipe
            WHERE ur.id IS NULL
            GROUP BY r.id, r.title
            HAVING COUNT(rt.id) >= 3 AND AVG(rt.value) > 4.5
            ORDER BY avg_rating DESC
            LIMIT 50;
        """)

    def test_mongo(self, db):
        pipeline = [
            {
                "$match": {
                    "ratings.2": {"$exists": True}  # co najmniej 3 oceny
                }
            },
            {
                "$addFields": {
                    "avg_rating": {"$avg": "$ratings.value"}
                }
            },
            {
                "$match": {
                    "avg_rating": {"$gt": 4.5}
                }
            },
            {
                "$lookup": {
                    "from": "users_recipes",
                    "localField": "id",
                    "foreignField": "id_recipe",
                    "as": "liked"
                }
            },
            {
                "$match": {
                    "liked": {"$eq": []}  # brak polubień
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "id": 1,
                    "title": 1,
                    "avg_rating": 1,
                    "num_ratings": {"$size": "$ratings"}
                }
            },
            {
                "$sort": {"avg_rating": -1}
            },
            {
                "$limit": 50
            }
        ]
        return list(db.recipes.aggregate(pipeline))

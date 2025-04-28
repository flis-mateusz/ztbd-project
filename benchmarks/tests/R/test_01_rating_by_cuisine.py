from benchmarks.core.base_test import BasePerformanceTest

class AverageRatingByCuisineTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(description="Obliczenie średniej oceny przepisów pogrupowanej po typie kuchni")

    # 📘 Test: obliczenie średniej oceny przepisów pogrupowanej po kuchni (id_cuisine)
    # W relacyjnych bazach wykorzystuje GROUP BY, w Mongo agregację z $group
    # Cel: porównanie wydajności operacji agregujących

    def test_mysql(self):
        return self.fetch_mysql("""
            SELECT id_cuisine, AVG(rating) AS avg_rating
            FROM recipes
            GROUP BY id_cuisine
        """)

    def test_postgres(self):
        return self.fetch_postgres("""
            SELECT id_cuisine, AVG(rating) AS avg_rating
            FROM recipes
            GROUP BY id_cuisine
        """)

    def test_mongo(self, db):
        pipeline = [
            {
                "$group": {
                    "_id": "$id_cuisine",
                    "avg_rating": { "$avg": "$rating" }
                }
            },
            {
                "$project": {
                    "id_cuisine": "$_id",
                    "avg_rating": 1,
                    "_id": 0
                }
            }
        ]
        return list(db.recipes.aggregate(pipeline))

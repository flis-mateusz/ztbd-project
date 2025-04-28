from benchmarks.core.base_test import BasePerformanceTest


class TopCuisinesByFavouritesTest(BasePerformanceTest):
    """
    Która kuchnia ma łącznie najwięcej zapisów do ulubionych (users_favourite)?
    """

    def __init__(self):
        super().__init__(description="Popularność kuchni (liczba ulubionych) – TOP 10")

    _sql = """
        SELECT r.id_cuisine, COUNT(*) AS favs
        FROM users_favourite uf
        JOIN recipes r ON r.id = uf.id_recipe
        GROUP BY r.id_cuisine
        ORDER BY favs DESC
        LIMIT 10;
    """

    def test_mysql(self):
        return self.fetch_mysql(self._sql)

    def test_postgres(self):
        return self.fetch_postgres(self._sql)

    def test_mongo(self, db):
        pipeline = [
            {"$lookup": {
                "from": "recipes",
                "localField": "id_recipe",
                "foreignField": "id",
                "as": "rec"}},
            {"$unwind": "$rec"},
            {"$group": {"_id": "$rec.id_cuisine", "favs": {"$sum": 1}}},
            {"$sort": {"favs": -1}},
            {"$limit": 10},
            {"$project": {"_id": 0, "id_cuisine": "$_id", "favs": 1}},
        ]
        return list(db.users_favourite.aggregate(pipeline))

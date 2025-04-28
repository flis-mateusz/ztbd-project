from benchmarks.core.base_test import BasePerformanceTest


class TopUsersByRatingsTest(BasePerformanceTest):
    """
    Którzy użytkownicy wystawili najwięcej ocen
    """

    def __init__(self):
        super().__init__(
            description="Użytkownicy z największą liczbą ocen – TOP 10")

    # ─────────── SQL (MySQL / PostgreSQL) ────────────────────────────
    _sql = """
        SELECT id_user, COUNT(*) AS ratings
        FROM rating
        GROUP BY id_user
        ORDER BY ratings DESC
        LIMIT 10;
    """

    def test_mysql(self):
        return self.fetch_mysql(self._sql)

    def test_postgres(self):
        return self.fetch_postgres(self._sql)

    # ─────────── MongoDB (bez $lookup) ───────────────────────────────
    def test_mongo(self, db):
        pipeline = [
            {"$group": {"_id": "$id_user", "ratings": {"$sum": 1}}},
            {"$sort": {"ratings": -1}},
            {"$limit": 10},
            {"$project": {"_id": 0, "id_user": "$_id", "ratings": 1}}
        ]
        return list(db.rating.aggregate(pipeline))

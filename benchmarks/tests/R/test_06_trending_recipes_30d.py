from datetime import datetime, timedelta
from benchmarks.core.base_test import BasePerformanceTest


class TrendingRecipesLast30DaysTest(BasePerformanceTest):
    """
    Przepisy, które w ostatnich 30 dniach trafiły ≥ 10 razy do ulubionych.
    Łańcuch: filtr po dacie → GROUP BY → HAVING → TOP-20.
    """

    def __init__(self):
        super().__init__(description="Trending (zapisy do ulubionych 30 dni) – TOP 20")

    _sql = """
        WITH recent AS (
            SELECT id_recipe
            FROM users_favourite
            WHERE saved_at >= NOW() - INTERVAL '30 days'
        )
        SELECT r.id, r.title, COUNT(*) AS saves_30d
        FROM recent rec
        JOIN recipes r ON r.id = rec.id_recipe
        GROUP BY r.id, r.title
        HAVING COUNT(*) >= 10
        ORDER BY saves_30d DESC
        LIMIT 20;
    """

    def test_mysql(self):
        return self.fetch_mysql("""
            SELECT r.id,
                   r.title,
                   COUNT(*) AS saves_30d
            FROM recipes r
            JOIN users_favourite uf
                  ON uf.id_recipe = r.id
            WHERE uf.saved_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
            GROUP BY r.id, r.title
            HAVING saves_30d >= 10
            ORDER BY saves_30d DESC
            LIMIT 20;
        """)

    def test_postgres(self):
        return self.fetch_postgres(self._sql)

    def test_mongo(self, db):
        since = datetime.utcnow() - timedelta(days=30)
        pipeline = [
            {"$match": {"saved_at": {"$gte": since}}},
            {"$group": {"_id": "$id_recipe", "cnt": {"$sum": 1}}},
            {"$match": {"cnt": {"$gte": 10}}},
            {"$sort": {"cnt": -1}},
            {"$limit": 20},
            {"$lookup": {
                "from": "recipes", "localField": "_id",
                "foreignField": "id", "as": "recipe"}},
            {"$unwind": "$recipe"},
            {"$project": {
                "_id": 0,
                "id": "$recipe.id",
                "title": "$recipe.title",
                "saves_30d": "$cnt"}}
        ]
        return list(db.users_favourite.aggregate(pipeline))

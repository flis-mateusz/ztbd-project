import datetime
from benchmarks.core.base_test import BasePerformanceTest


class BulkSaveTop50FavouritesTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="USER 1 zapisuje TOP-50 przepisów (views)",
            operation="CREATE")

    _sql = """
        INSERT INTO users_favourite (id_recipe,id_user,saved_at)
        SELECT id, 1, NOW()
        FROM recipes ORDER BY views DESC LIMIT 50;
    """

    # ─────────── MySQL / PG (po 1 poleceniu)
    def test_mysql(self):
        cur = self._mysql.cursor()
        cur.execute(self._sql)
        rc = cur.rowcount
        cur.close()
        return rc

    def test_postgres(self):
        cur = self._postgres.cursor()
        cur.execute(self._sql)
        rc = cur.rowcount
        cur.close()
        return rc

    # ─────────── Mongo – pipeline-delete-merge z własnym _id
    def test_mongo(self, db):
        now = datetime.datetime.utcnow()
        pipeline = [
            {"$sort": {"views": -1}},
            {"$limit": 50},
            {"$addFields": {
                "id_user":   1,
                "saved_at":  now,
                "_id":       {"$concat": [{"$toString": "$id"}, "_1"]}}},
            {"$project": {
                "id_recipe": "$id",
                "id_user":   1,
                "saved_at":  1,
                "_id":       1}},
            {"$merge": {
                "into": "users_favourite",
                "whenMatched": "replace",
                "whenNotMatched": "insert"}}
        ]
        db.recipes.aggregate(pipeline)
        return 50

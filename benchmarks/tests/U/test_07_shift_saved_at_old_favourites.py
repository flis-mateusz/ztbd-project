from benchmarks.core.base_test import BasePerformanceTest


class ShiftSavedAtOldFavouritesTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="Przesuń saved_at o +30 dni w users_favourite starszych niż rok",
            operation="UPDATE")

    _sql_mysql = """
        UPDATE users_favourite
        SET saved_at = DATE_ADD(saved_at, INTERVAL 30 DAY)
        WHERE saved_at < (NOW() - INTERVAL 365 DAY);
    """
    _sql_pg = """
        UPDATE users_favourite
        SET saved_at = saved_at + INTERVAL '30 days'
        WHERE saved_at < (NOW() - INTERVAL '365 days');
    """

    def test_mysql(self):    return self.fetch_mysql(self._sql_mysql)

    def test_postgres(self): return self.fetch_postgres(self._sql_pg)

    def test_mongo(self, db):
        from datetime import datetime, timedelta
        cutoff = datetime.utcnow() - timedelta(days=365)
        pipeline = [
            {"$set": {"saved_at": {"$dateAdd": {
                "startDate": "$saved_at",
                "unit": "day",
                "amount": 30
            }}}}
        ]
        return db.users_favourite.update_many(
            {"saved_at": {"$lt": cutoff}}, pipeline).modified_count

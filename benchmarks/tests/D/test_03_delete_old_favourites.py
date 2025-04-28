from datetime import datetime, timedelta
from benchmarks.core.base_test import BasePerformanceTest


class DeleteOldFavouritesTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description=("Usuń polubienia zapisane ponad rok temu "
                         "(`saved_at < NOW()-365 dni`). "
                         "To typowy housekeeping, gdy użytkownik dawno "
                         "nie zaglądał do zakładki „Favourite”."),
            operation="DELETE")

    _sql_mysql = (
        "DELETE FROM users_favourite "
        "WHERE saved_at < (NOW() - INTERVAL 365 DAY);")
    _sql_pg = (
        "DELETE FROM users_favourite "
        "WHERE saved_at < (NOW() - INTERVAL '365 days');")

    def test_mysql(self):    return self.fetch_mysql(self._sql_mysql)
    def test_postgres(self): return self.fetch_postgres(self._sql_pg)
    def test_mongo(self, db):
        cutoff = datetime.utcnow() - timedelta(days=365)
        return db.users_favourite.delete_many({"saved_at": {"$lt": cutoff}}).deleted_count

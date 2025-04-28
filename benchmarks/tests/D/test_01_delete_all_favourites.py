from benchmarks.core.base_test import BasePerformanceTest


class DeleteAllFavouritesTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="Usuń wszystkie rekordy z listy ulubionych ",
            operation="DELETE")

    _sql = "DELETE FROM users_favourite;"

    def test_mysql(self):    return self.fetch_mysql(self._sql)
    def test_postgres(self): return self.fetch_postgres(self._sql)
    def test_mongo(self, db): return db.users_favourite.delete_many({}).deleted_count

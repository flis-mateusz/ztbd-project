from benchmarks.core.base_test import BasePerformanceTest


class BasicReadTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(False, "Bazowy test sprawdzający czas odczytu 10000 rekordów", operation='CREATE')

    # Test: podstawowy odczyt 10 000 rekordów z tabeli 'recipes'

    def test_mysql(self):
        return self.fetch_mysql("SELECT * FROM recipes LIMIT 10000")

    def test_postgres(self):
        return self.fetch_postgres("SELECT * FROM recipes LIMIT 10000")

    def test_mongo(self, db):
        return list(db.recipes.find({}, {"_id": 0}).limit(10000))

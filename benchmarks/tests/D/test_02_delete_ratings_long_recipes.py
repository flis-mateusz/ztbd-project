from benchmarks.core.base_test import BasePerformanceTest


class DeleteRatingsForLongRecipesTest(BasePerformanceTest):
    """
    Usuń oceny (`rating`) dla przepisów, których czas gotowania przekracza
    180 minut.  Generator danych losuje cook_time 5-240 → rekordy spełniające
    warunek faktycznie istnieją.
    """

    def __init__(self):
        super().__init__(
            description="Usuń rating dla czasu przyrządzania > 180 min",
            operation="DELETE")

    _sql = """
        DELETE FROM rating
        WHERE id_recipe IN (
            SELECT id FROM recipes WHERE cook_time > 180
        );
    """

    # ── SQL
    def test_mysql(self):    return self.fetch_mysql(self._sql)
    def test_postgres(self): return self.fetch_postgres(self._sql)

    # ── Mongo
    def test_mongo(self, db):
        long_ids = db.recipes.distinct("id", {"cook_time": {"$gt": 180}})
        if not long_ids:
            return 0
        return db.rating.delete_many({"id_recipe": {"$in": long_ids}}).deleted_count

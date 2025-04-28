from benchmarks.core.base_test import BasePerformanceTest


class DeleteHighCalorieFavouritesTest(BasePerformanceTest):
    """
    Usuń polubienia (`users_favourite`) do przepisów, których kaloryczność
    przekracza 800 kcal (tabela nutrition).  Zamiast JOIN użyto podzapytania
    `EXISTS`, co przy kluczu obcym na `id_recipe` działa szybciej w PostgreSQL.
    """

    def __init__(self):
        super().__init__(
            description="Usuń polubienia (users_favourite) przypisane do przepisów o kaloryczności przekraczającej 800 kcal.",
            operation="DELETE")

    _sql = """
        DELETE FROM users_favourite uf
        WHERE EXISTS (
            SELECT 1 FROM nutrition n
            WHERE n.id_recipe = uf.id_recipe
              AND n.calories  > 800
        );
    """

    def test_mysql(self):    return self.fetch_mysql(self._sql)
    def test_postgres(self): return self.fetch_postgres(self._sql)

    def test_mongo(self, db):
        bad_rec = db.nutrition.distinct("id_recipe", {"calories": {"$gt": 800}})
        if not bad_rec:
            return 0
        return db.users_favourite.delete_many({"id_recipe": {"$in": bad_rec}}).deleted_count

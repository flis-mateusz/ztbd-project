from benchmarks.core.base_test import BasePerformanceTest


class DeleteLowPopularityRecipesTest(BasePerformanceTest):
    """
    Usuń przepisy, które mają mniej niż 10 wyświetleń i ich średni rating
    < 2.5. W relacyjnych – podzapytanie z AVG; w Mongo – agregacja i delete.
    """

    def __init__(self):
        super().__init__(description=(
            "Usuń wszystkie oceny niższe niż 2 ★ wystawione przepisom, które "
            "zgromadziły co najmniej 50 000 wyświetleń. "
        ), operation='DELETE')

    _sql_ids = """
        SELECT r.id
        FROM recipes r
        JOIN (
            SELECT id_recipe, AVG(value) AS avg_rating
            FROM rating
            GROUP BY id_recipe
        ) avg ON avg.id_recipe = r.id
        WHERE r.views < 10 AND avg.avg_rating < 2.5;
    """

    def test_mysql(self):
        bad = [row["id"] for row in self.fetch_mysql(self._sql_ids)]
        if bad:
            return self.fetch_mysql(
                "DELETE FROM recipes WHERE id IN (" + ",".join(map(str, bad)) + ");"
            )
        return 0

    def test_postgres(self):
        bad = [row[0] for row in self.fetch_postgres(self._sql_ids)]
        if bad:
            return self.fetch_postgres(
                "DELETE FROM recipes WHERE id = ANY(%s);", (bad,)
            )
        return 0

    def test_mongo(self, db):
        popular_ids = db.recipes.distinct("id", {"views": {"$gt": 50000}})
        if not popular_ids:
            return 0
        return db.rating.delete_many(
            {"value": {"$lt": 2.0},
             "id_recipe": {"$in": popular_ids}}).deleted_count

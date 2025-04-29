from benchmarks.core.base_test import BasePerformanceTest


class DeleteRatingsForUnpopularRecipesTest(BasePerformanceTest):
    """
    Usuń oceny (rating) przypisane do przepisów, które mają mniej niż
    10 wyświetleń i średnią ocenę poniżej 2,5.
    """

    def __init__(self):
        super().__init__(
            description="Usuń oceny (rating) przypisane do przepisów, które mają mniej niż 10 wyświetleń i średnią ocenę poniżej 2,5.",
            operation="DELETE")

    _sql = """
        DELETE FROM rating
        WHERE id_recipe IN (
            SELECT r.id
            FROM recipes r
            JOIN (
                SELECT id_recipe, AVG(value) AS avg_rating
                FROM rating
                GROUP BY id_recipe
            ) a ON a.id_recipe = r.id
            WHERE r.views < 10
              AND a.avg_rating < 2.5
        );
    """

    def test_mysql(self):
        return self.fetch_mysql(self._sql)

    def test_postgres(self):
        return self.fetch_postgres(self._sql)

    def test_mongo(self, db):
        unpopular = db.recipes.aggregate([
            {"$lookup": {
                "from": "rating",
                "localField": "id",
                "foreignField": "id_recipe",
                "as": "rat"}},
            {"$match": {"views": {"$lt": 10}}},
            {"$project": {
                "id": 1,
                "avg": {"$avg": "$rat.value"}}},
            {"$match": {"avg": {"$lt": 2.5}}},
            {"$project": {"_id": 0, "id": 1}}
        ])
        ids = [doc["id"] for doc in unpopular]
        if not ids:
            return 0

        return db.rating.delete_many({"id_recipe": {"$in": ids}}).deleted_count

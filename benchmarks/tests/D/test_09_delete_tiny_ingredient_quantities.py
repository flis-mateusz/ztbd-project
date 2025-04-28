from benchmarks.core.base_test import BasePerformanceTest


class DeleteTinyIngredientQuantitiesTest(BasePerformanceTest):
    """
    Usuń powiązania `recipes_ingredients`, w których ilość składnika jest
    mniejsza niż 0.5 (g/ml/itp.). Takie rekordy traktujemy jako błąd danych.
    """

    def __init__(self):
        super().__init__(
            description="Usuń rekordy recipes_ingredients, w których ilość składnika jest mniejsza niż 0,5 jednostki",
            operation="DELETE")

    _sql = "DELETE FROM recipes_ingredients WHERE quantity < 0.5;"

    def test_mysql(self):    return self.fetch_mysql(self._sql)

    def test_postgres(self): return self.fetch_postgres(self._sql)

    def test_mongo(self, db):
        return db.recipes_ingredients.delete_many(
            {"quantity": {"$lt": 0.5}}
        ).deleted_count

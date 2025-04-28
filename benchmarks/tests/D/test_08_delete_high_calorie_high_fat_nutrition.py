from benchmarks.core.base_test import BasePerformanceTest


class DeleteHighCalorieHighFatNutritionTest(BasePerformanceTest):
    """
    Usuń rekordy z **nutrition**, w których jednocześnie:
    ─ kaloryczność przekracza 800 kcal
    ─ zawartość tłuszczu przekracza 50 g
    """

    def __init__(self):
        super().__init__(
            description=(
                "Usuń wpisy o wartościach odżywczych (nutrition) dla przepisów, "
                "w których jedna porcja przekracza 800 kcal oraz zawiera ponad 50 g tłuszczu."
            ),
            operation="DELETE")

    _sql = (
        "DELETE FROM nutrition "
        "WHERE calories > 800 AND fat > 50;"
    )

    def test_mysql(self):    return self.fetch_mysql(self._sql)

    def test_postgres(self): return self.fetch_postgres(self._sql)

    def test_mongo(self, db):
        return db.nutrition.delete_many(
            {"calories": {"$gt": 800}, "fat": {"$gt": 50}}
        ).deleted_count

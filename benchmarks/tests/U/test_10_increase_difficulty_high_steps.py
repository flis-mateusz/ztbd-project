from benchmarks.core.base_test import BasePerformanceTest


class IncreaseDifficultyHighStepsTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="Ustaw id_difficulty = 3 (Hard) w przepisach, "
                        "które mają więcej niż 8 kroków",
            operation="UPDATE")

    _sql_mysql = """
        UPDATE recipes r
        JOIN (
            SELECT id_recipe FROM instructions
            GROUP BY id_recipe HAVING COUNT(*) > 8
        ) t ON t.id_recipe = r.id
        SET r.id_difficulty = 3;
    """
    _sql_pg = """
        UPDATE recipes
        SET id_difficulty = 3
        WHERE id IN (
            SELECT id_recipe FROM instructions
            GROUP BY id_recipe HAVING COUNT(*) > 8
        );
    """

    def test_mysql(self):    return self.fetch_mysql(self._sql_mysql)

    def test_postgres(self): return self.fetch_postgres(self._sql_pg)

    def test_mongo(self, db):
        return db.recipes.update_many(
            {"instructions.8": {"$exists": True}},
            {"$set": {"id_difficulty": 3}}
        ).modified_count

from benchmarks.core.base_test import BasePerformanceTest


class FlagLongCookTimeRecipesTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="Dodaj prefix '[LONG] ' w description gdy cook_time > 180 min",
            operation="UPDATE")

    _sql_mysql = """
        UPDATE recipes
        SET description = CONCAT('[LONG] ', description)
        WHERE cook_time > 180;
    """
    _sql_pg = """
        UPDATE recipes
        SET description = '[LONG] ' || description
        WHERE cook_time > 180;
    """

    def test_mysql(self):    return self.fetch_mysql(self._sql_mysql)

    def test_postgres(self): return self.fetch_postgres(self._sql_pg)

    def test_mongo(self, db):
        pipeline = [
            {"$set": {
                "description": {"$cond": [
                    {"$and": [
                        {"$gt": ["$cook_time", 180]},
                        {"$not": [{"$regexMatch": {
                            "input": "$description",
                            "regex": r"^\[LONG\] "}}]}
                    ]},
                    {"$concat": ["[LONG] ", "$description"]},
                    "$description"
                ]}}}
        ]
        return db.recipes.update_many({}, pipeline).modified_count

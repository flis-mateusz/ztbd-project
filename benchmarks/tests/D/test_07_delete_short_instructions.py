from benchmarks.core.base_test import BasePerformanceTest


class DeleteShortInstructionsTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description=("Usuń instrukcje kulinarne tak krótkie, że nie wnoszą "
                         "żadnej wartości (mniej niż 10 znaków)."),
            operation="DELETE")

    def test_mysql(self):
        return self.fetch_mysql(
            "DELETE FROM instructions WHERE CHAR_LENGTH(description) < 10;")

    def test_postgres(self):
        return self.fetch_postgres(
            "DELETE FROM instructions WHERE LENGTH(description) < 10;")

    def test_mongo(self, db):
        return db.instructions.delete_many(
            {"$expr": {"$lt": [{"$strLenCP": "$description"}, 10]}}
        ).deleted_count

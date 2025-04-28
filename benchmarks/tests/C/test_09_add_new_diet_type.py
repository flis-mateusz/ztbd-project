from benchmarks.core.base_test import BasePerformanceTest


class AddNewDietTypeTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="INSERT nowy typ diety do tabeli diet",
            operation="CREATE")

    def test_mysql(self):
        cur = self._mysql.cursor()
        cur.execute("INSERT INTO diet (type) VALUES ('Carnivore_Plan');")
        cur.close()
        return 1

    def test_postgres(self):
        cur = self._postgres.cursor()
        cur.execute("INSERT INTO diet (type) VALUES ('Carnivore_Plan');")
        cur.close()
        return 1

    def test_mongo(self, db):
        doc = {
            "id": db.diet.estimated_document_count()+1,
            "type": "Carnivore_Plan"
        }
        return db.diet.insert_one(doc).inserted_id

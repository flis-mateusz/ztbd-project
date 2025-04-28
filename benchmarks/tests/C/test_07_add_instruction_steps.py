from benchmarks.core.base_test import BasePerformanceTest


class AddInstructionStepsTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="INSERT 3 instrukcje do ostatniego przepisu",
            operation="CREATE")

    _sql_mysql = """
        INSERT INTO instructions (id_recipe,step_number,description)
        SELECT rid, n, CONCAT('Krok ',n)
        FROM (SELECT MAX(id) AS rid FROM recipes) r,
             (SELECT 1 n UNION ALL SELECT 2 UNION ALL SELECT 3) nums;
    """

    _sql_pg = _sql_mysql.replace("CONCAT", ""  # PG używa || do konkatenacji
                                 ).replace("CONCAT('Krok ',n)", "'Krok '||n")

    def test_mysql(self):
        cur = self._mysql.cursor()
        cur.execute(self._sql_mysql)
        rc = cur.rowcount
        cur.close()
        return rc

    def test_postgres(self):
        cur = self._postgres.cursor()
        cur.execute(self._sql_pg)
        rc = cur.rowcount
        cur.close()
        return rc

    def test_mongo(self, db):
        rid = db.recipes.find_one(sort=[("id", -1)])["id"]
        docs = [{"id": db.instructions.estimated_document_count()+1+i,
                 "id_recipe": rid,
                 "step_number": i+1,
                 "description": f"Krok {i+1}"} for i in range(3)]
        return db.instructions.insert_many(docs).inserted_ids.__len__()

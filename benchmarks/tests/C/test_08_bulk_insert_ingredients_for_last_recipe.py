import random
from benchmarks.core.base_test import BasePerformanceTest


class BulkInsertIngredientsForLastRecipeTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="INSERT 10 składników do ostatniego przepisu (losowo)",
            operation="CREATE")

    _sql_mysql = """
        INSERT INTO recipes_ingredients (id_recipe,id_ingredient,quantity,measurement)
        SELECT rid, id, 1.0, 'g'
        FROM (SELECT MAX(id) AS rid FROM recipes) r
        JOIN (
            SELECT id FROM ingredients ORDER BY RAND() LIMIT 10
        ) s ON 1=1;
    """
    _sql_pg = _sql_mysql.replace("RAND()", "random()")

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
        ing_ids = [doc["id"] for doc in db.ingredients.aggregate([{"$sample": {"size": 10}}])]
        docs = [{"id": db.recipes_ingredients.estimated_document_count()+1+i,
                 "id_recipe": rid,
                 "id_ingredient": iid,
                 "quantity": 1.0,
                 "measurement": "g"} for i, iid in enumerate(ing_ids)]
        return db.recipes_ingredients.insert_many(docs).inserted_ids.__len__()

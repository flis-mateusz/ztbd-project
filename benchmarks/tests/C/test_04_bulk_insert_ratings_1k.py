import random
from benchmarks.core.base_test import BasePerformanceTest


class BulkInsertRatings1kTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="INSERT 1000 ratingów user-1 ➜ przepis-1",
            operation="CREATE")

    _recipe_id = 1
    _user_id   = 1
    _rows      = 1000

    # ─────────── MySQL
    def test_mysql(self):
        values = ",".join(
            f"({self._user_id},{self._recipe_id},{round(random.uniform(1,5),1)})"
            for _ in range(self._rows))
        sql = f"INSERT INTO rating (id_user,id_recipe,value) VALUES {values};"
        cur = self._mysql.cursor()
        cur.execute(sql)
        cur.close()
        return self._rows

    # ─────────── PostgreSQL
    def test_postgres(self):
        values = ",".join(
            f"({self._user_id},{self._recipe_id},{round(random.uniform(1,5),1)})"
            for _ in range(self._rows))
        sql = f"INSERT INTO rating (id_user,id_recipe,value) VALUES {values};"
        cur = self._postgres.cursor()
        cur.execute(sql)
        cur.close()
        return self._rows

    # ─────────── Mongo
    def test_mongo(self, db):
        start = db.rating.estimated_document_count() + 1
        docs = [{"id": start+i,
                 "id_user":   self._user_id,
                 "id_recipe": self._recipe_id,
                 "value":     round(random.uniform(1,5),1)}
                for i in range(self._rows)]
        db.rating.insert_many(docs, ordered=False)
        return self._rows

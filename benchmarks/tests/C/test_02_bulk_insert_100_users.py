import random, string
from benchmarks.core.base_test import BasePerformanceTest


def _rnd(n=8):
    return ''.join(random.choices(string.ascii_lowercase, k=n))


class BulkInsert100UsersTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="INSERT 100 użytkowników (jeden batch)",
            operation="CREATE")

    # ── MySQL & PostgreSQL – budujemy jedną instrukcję VALUES
    @staticmethod
    def _values(rows=100):
        return [
            f"('{_rnd()}@ex.com','{_rnd()}','{_rnd(12)}')"
            for _ in range(rows)
        ]

    def test_mysql(self):
        sql = (
            "INSERT INTO users (email, login, password) VALUES " +
            ",".join(self._values()))
        cur = self._mysql.cursor()
        cur.execute(sql)
        n = cur.rowcount
        cur.close()
        return n

    def test_postgres(self):
        sql = (
            "INSERT INTO users (email, login, password) VALUES " +
            ",".join(self._values()) + ";")
        cur = self._postgres.cursor()
        cur.execute(sql)
        n = cur.rowcount
        cur.close()
        return n

    # ── Mongo – insert_many
    def test_mongo(self, db):
        docs = [{"id": db.users.estimated_document_count() + 1 + i,
                 "email": f"{_rnd()}@ex.com",
                 "login": _rnd(),
                 "password": _rnd(12)}
                for i in range(100)]
        return db.users.insert_many(docs).inserted_ids.__len__()

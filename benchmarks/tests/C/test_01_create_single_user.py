import random, string, datetime
from benchmarks.core.base_test import BasePerformanceTest


def _rnd(n=10):
    return ''.join(random.choices(string.ascii_lowercase, k=n))


class CreateSingleUserTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="INSERT jednego użytkownika (email, login, password)",
            operation="CREATE")

    # ── MySQL
    def test_mysql(self):
        sql = (
            "INSERT INTO users (email, login, password) "
            "VALUES (CONCAT(UUID(),'@mail.com'), UUID(), UUID());"
        )
        cur = self._mysql.cursor()
        cur.execute(sql)
        cnt = cur.rowcount
        cur.close()
        return cnt

    # ── PostgreSQL
    def test_postgres(self):
        sql = (
            "INSERT INTO users (email, login, password) "
            "SELECT md5(random()::text)||'@mail.com', md5(random()::text), md5(random()::text);"
        )
        cur = self._postgres.cursor()
        cur.execute(sql)
        cnt = cur.rowcount
        cur.close()
        return cnt

    # ── Mongo
    def test_mongo(self, db):
        doc = {
            "id": db.users.estimated_document_count() + 1,
            "email": f"{_rnd()}@mail.com",
            "login": _rnd(),
            "password": _rnd(12),
            "created_at": datetime.datetime.utcnow()
        }
        return db.users.insert_one(doc).inserted_id

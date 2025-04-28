"""
DELETE-test #01  – Usunięcie **wszystkich** zapisów z ulubionych
---------------------------------------------------------------
• Operacja DELETE bez warunków na tabeli / kolekcji `users_favourite`.
• Dzięki sandboxowi (transakcja / abortTransaction) po benchmarku
  baza wraca do pierwotnego stanu, więc liczba polubień nie zmienia się
  “na stałe”. Zwracam jedynie liczbę wierszy/dokumentów,
  które zostały objęte operacją DELETE.
"""

from benchmarks.core.base_test import BasePerformanceTest


class DeleteAllFavouritesTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(description="DELETE * z users_favourite (rollback sandbox)", operation='DELETE')

    # ─── MySQL ────────────────────────────────────────────────────────────
    def test_mysql(self):
        deleted = self.fetch_mysql("SELECT COUNT(*) AS c FROM users_favourite")[0]["c"]
        self.fetch_mysql("DELETE FROM users_favourite;")
        return {"deleted": deleted}

    # ─── PostgreSQL ───────────────────────────────────────────────────────
    def test_postgres(self):
        deleted = self.fetch_postgres("SELECT COUNT(*) FROM users_favourite")[0][0]
        self.fetch_postgres("DELETE FROM users_favourite;")
        return {"deleted": deleted}

    # ─── MongoDB (6.x / latest) ───────────────────────────────────────────
    def test_mongo(self, db):
        deleted = db.users_favourite.count_documents({})
        db.users_favourite.delete_many({})
        return {"deleted": deleted}

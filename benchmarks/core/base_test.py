import contextlib
import json
import time
from abc import ABC, abstractmethod
from decimal import Decimal

import mysql.connector as mysql_connector
import psycopg2
import psycopg2.extras
from bson import ObjectId
from psycopg2.extras import DictRow
from pymongo import MongoClient


class BasePerformanceTest(ABC):
    """
    • operation –  'READ' (domyślnie) | 'CREATE' | 'UPDATE' | 'DELETE'
    • READ:   brak zmian → brak sandboxa
    • C/U/D:  sandbox → rollback po pomiarze
    """

    def __init__(self, *, save_output=True, description=None, operation="READ"):
        self.save_output = save_output
        self.description = description
        self.operation = operation.upper()

    # ───────────────────────────────────── run
    def run(self):
        # ─────────── połączenia ───────────
        self._mysql = mysql_connector.connect(
            host="127.0.0.1", port=3306, user="root",
            password="example", database="testdb"
        )
        self._postgres = psycopg2.connect(
            host="127.0.0.1", port=5432, user="postgres",
            password="example", dbname="testdb"
        )
        self._mongo = {
            "mongo_latest": MongoClient("localhost", 27017)["testdb"],
            "mongo_old": MongoClient("localhost", 27018)["testdb"]
        }

        print(f"\n🔬 Running test [{self.operation}]: {self.__class__.__name__}")
        self.results, self.outputs = {"test": self.__class__.__name__}, {}

        # SQL
        engines = {"mysql": self.test_mysql, "postgres": self.test_postgres}
        for name, fn in engines.items():
            self._execute(name, fn)

        # Mongo (x2)
        for name, db in self._mongo.items():
            self._execute(name, lambda d=db: self.test_mongo(d))

        # -------- rollback Mongo snapshot --------
        if self.operation != "READ":
            from database_scripts.reset_databases import restore_mongo_all
            restore_mongo_all()

        self._close_all()

        # zapisz wyjścia
        if self.save_output:
            with open(f"benchmarks/results/details/{self.__class__.__name__}_output.json",
                      "w", encoding="utf-8") as f:
                json.dump(self.outputs, f, ensure_ascii=False, indent=2)

        return self.results

    # ───────────────────────────────────── sandbox wrapper
    def _execute(self, engine_name, fn):
        """
        • dla READ – zwykły pomiar
        • dla C/U/D – sandbox (transakcja / abort / manual undo)
        """
        if self.operation == "READ":
            return self._timed_call(engine_name, fn)

        # ------------- sandbox -------------
        with self._sandbox(engine_name):
            return self._timed_call(engine_name, fn)

    # ───────────────────────────────────── _timed_call
    def _timed_call(self, engine, fn):
        start = time.perf_counter()
        try:
            result = fn()
            elapsed = round(time.perf_counter() - start, 4)
            self.results[engine] = elapsed
            if self.save_output:
                self.outputs[engine] = _jsonify(result)
            print(f"⏱️  {engine}: {elapsed:.4f}s")
        except NotImplementedError:
            self.results[engine] = "NOT_IMPLEMENTED"
            print(f"⚠️  {engine}: test not implemented.")
        except Exception as exc:
            self.results[engine] = f"ERROR: {exc}"
            print(f"❌  {engine}: ERROR – {exc}")

    # ───────────────────────────────────── sandbox (context manager)
    def _sandbox(self, engine):
        if engine == "mysql":
            return self._sql_tx(self._mysql)
        if engine == "postgres":
            return self._sql_tx(self._postgres)

        return contextlib.nullcontext()

    # ---- SQL transakcja
    @staticmethod
    @contextlib.contextmanager
    def _sql_tx(conn):
        cur = conn.cursor()
        cur.execute("START TRANSACTION")
        try:
            yield
        finally:
            cur.execute("ROLLBACK")
            cur.close()

    # ───────────────────────────────────── helpers SQL
    def fetch_mysql(self, sql, params=None):
        cur = self._mysql.cursor(dictionary=True)
        cur.execute(sql, params or {})
        rows = cur.fetchall()
        cur.close()
        return rows

    def fetch_postgres(self, sql, params=None):
        cur = self._postgres.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(sql, params or {})
        rows = cur.fetchall() if cur.description else cur.rowcount
        cur.close()
        return rows

    # ───────────────────────────────────── clean-up
    def _close_all(self):
        self._mysql.close()
        self._postgres.close()
        for db in self._mongo.values():
            db.client.close()

    # ───────────────────────────────────── abstrakcyjne
    @abstractmethod
    def test_mysql(self):
        ...

    @abstractmethod
    def test_postgres(self):
        ...

    @abstractmethod
    def test_mongo(self, db):
        ...


# ───────────────────────────────────────── jsonify helper
def _jsonify(obj):
    if isinstance(obj, DictRow):
        return {k: _jsonify(v) for k, v in obj.items()}
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, list):
        return [_jsonify(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _jsonify(v) for k, v in obj.items()}
    return obj

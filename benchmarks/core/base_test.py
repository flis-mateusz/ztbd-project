import json
import time
from abc import ABC, abstractmethod

import mysql.connector
import psycopg2
from pymongo import MongoClient


class BasePerformanceTest(ABC):
    def __init__(self, save_output=True, description=None):
        self.save_output = save_output
        self.results = {}
        self.description = description

        self.__mysql = mysql.connector.connect(
            host="localhost", port=3306, user="root", password="example", database="testdb"
        )
        self.__postgres = psycopg2.connect(
            host="localhost", port=5432, user="postgres", password="example", dbname="testdb"
        )
        self.__mongo_clients = {
            "mongo_latest": MongoClient("localhost", 27017)["testdb"],
            "mongo_old": MongoClient("localhost", 27018)["testdb"]
        }

    def run(self):
        print(f"\n🔬 Running test: {self.__class__.__name__}")
        self.results["test"] = self.__class__.__name__
        self.outputs = {}

        engines = {
            "mysql": self.test_mysql,
            "postgres": self.test_postgres,
        }

        for name, test in engines.items():
            self._run_single_test(name, test)
        for name, mongo_db in self.__mongo_clients.items():
            self._run_single_test(name, lambda db=mongo_db: self.test_mongo(db))

        self.close_connections()

        if self.save_output:
            with open(f"benchmarks/results/details/{self.__class__.__name__}_output.json", "w", encoding="utf-8") as f:
                json.dump(self.outputs, f, ensure_ascii=False, indent=2)

        return self.results

    def _run_single_test(self, name, test_fn):
        start = time.perf_counter()
        try:
            result = test_fn()
            elapsed = round(time.perf_counter() - start, 4)
            self.results[name] = elapsed

            if self.save_output:
                self.outputs[name] = _to_json_compatible(result)
            print(f"⏱️  {name}: {elapsed:.4f}s")
        except NotImplementedError:
            self.results[name] = "NOT_IMPLEMENTED"
            print(f"⚠️  {name}: test not implemented.")
        except Exception as e:
            self.results[name] = f"ERROR: {e}"
            print(f"❌  {name}: ERROR - {e}")

    def fetch_mysql(self, sql):
        cursor = self.__mysql.cursor(dictionary=True)
        cursor.execute(sql)
        raw = cursor.fetchall()
        cursor.close()
        return raw

    def fetch_postgres(self, sql):
        import psycopg2.extras
        cursor = self.__postgres.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(sql)
        raw = cursor.fetchall()
        cursor.close()
        return raw

    def close_connections(self):
        self.__mysql.close()
        self.__postgres.close()
        for db in self.__mongo_clients.values():
            db.client.close()

    @abstractmethod
    def test_mysql(self):
        raise NotImplementedError()

    @abstractmethod
    def test_postgres(self):
        raise NotImplementedError()

    @abstractmethod
    def test_mongo(self, db):
        raise NotImplementedError()


from bson import ObjectId
from psycopg2.extras import DictRow

def _to_json_compatible(obj):
    if isinstance(obj, DictRow):
        return dict(obj)
    elif isinstance(obj, dict):
        return {k: _to_json_compatible(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_to_json_compatible(i) for i in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj


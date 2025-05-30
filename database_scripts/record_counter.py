import mysql.connector as mysql_connector
import psycopg2
from pymongo import MongoClient


def get_record_counts():
    counts = {}

    try:
        mysql_conn = mysql_connector.connect(
            host="127.0.0.1", port=3306, user="root", password="example", database="testdb"
        )
        cursor = mysql_conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        total = 0
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total += cursor.fetchone()[0]
        counts["MySQL"] = total
        cursor.close()
        mysql_conn.close()
    except Exception as e:
        counts["MySQL"] = f"❌ {e}"

    # try:
    #     postgres_conn = psycopg2.connect(
    #         host="127.0.0.1", port=5432, user="postgres", password="example", dbname="testdb"
    #     )
    #     cursor = postgres_conn.cursor()
    #     cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")
    #     tables = [row[0] for row in cursor.fetchall()]
    #     total = 0
    #     for table in tables:
    #         cursor.execute(f"SELECT COUNT(*) FROM {table}")
    #         total += cursor.fetchone()[0]
    #     counts["Postgres"] = total
    #     cursor.close()
    #     postgres_conn.close()
    # except Exception as e:
    #     counts["Postgres"] = f"❌ {e}"
    #
    # try:
    #     mongo_db = MongoClient("127.0.0.1", 27017)["testdb"]
    #     total = sum(mongo_db[coll].count_documents({}) for coll in mongo_db.list_collection_names())
    #     counts["Mongo (latest)"] = total
    # except Exception as e:
    #     counts["Mongo (latest)"] = f"❌ {e}"
    #
    # try:
    #     mongo_old_db = MongoClient("127.0.0.1", 27018)["testdb"]
    #     total = sum(mongo_old_db[coll].count_documents({}) for coll in mongo_old_db.list_collection_names())
    #     counts["Mongo (old)"] = total
    # except Exception as e:
    #     counts["Mongo (old)"] = f"❌ {e}"

    return counts

def human_readable(n: int):
    if n >= 1_000_000:
        return f"{round(n/1_000_000,1)} mln"
    if n >= 1_000:
        return f"{round(n/1_000,1)} tys."
    return str(n)

def round_records(n: int):
    if n >= 1_000_000:
        return round(n, -6)
    if n >= 1_000:
        return round(n, -3)
    return round(n)
import psycopg2

def create_postgres_tables(cursor):
    table_queries = [
        "CREATE TABLE IF NOT EXISTS cuisine (id INTEGER PRIMARY KEY, name VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS meal_type (id INTEGER PRIMARY KEY, name VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS diet (id INTEGER PRIMARY KEY, type VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS difficulty (id INTEGER PRIMARY KEY, level VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS ingredients (id INTEGER PRIMARY KEY, name VARCHAR(100));",
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            email VARCHAR(255),
            login VARCHAR(100),
            password VARCHAR(255)
        );""",
        """
        CREATE TABLE IF NOT EXISTS recipes (
            id INTEGER PRIMARY KEY,
            title VARCHAR(255),
            description TEXT,
            cook_time INTEGER,
            serving_size INTEGER,
            views INTEGER,
            rating FLOAT,
            id_cuisine INTEGER REFERENCES cuisine(id),
            id_diet INTEGER REFERENCES diet(id),
            id_difficulty INTEGER REFERENCES difficulty(id),
            id_meal_type INTEGER REFERENCES meal_type(id)
        );""",
        """
        CREATE TABLE IF NOT EXISTS recipes_ingredients (
            id INTEGER PRIMARY KEY,
            id_recipe INTEGER REFERENCES recipes(id),
            id_ingredient INTEGER REFERENCES ingredients(id),
            quantity FLOAT,
            measurement VARCHAR(50)
        );""",
        """
        CREATE TABLE IF NOT EXISTS instructions (
            id INTEGER PRIMARY KEY,
            recipe_id INTEGER REFERENCES recipes(id),
            step_number INTEGER,
            description TEXT
        );"""
    ]

    for query in table_queries:
        cursor.execute(query)

def truncate_postgres_tables(cursor):
    tables = ["instructions", "recipes_ingredients", "recipes", "users", "cuisine", "meal_type", "diet", "difficulty", "ingredients"]
    for table in tables:
        cursor.execute(f"TRUNCATE TABLE {table} CASCADE;")

def import_postgres_data(data: dict):
    conn = psycopg2.connect(host="localhost", port=5432, user="postgres", password="example", dbname="testdb")
    cursor = conn.cursor()

    create_postgres_tables(cursor)
    truncate_postgres_tables(cursor)

    def insert(table, columns, df):
        if df.empty:
            print(f"⚠️ DataFrame dla {table} jest pusty – pominięto.")
            return 0
        values = [tuple(row[col] for col in columns) for _, row in df.iterrows()]
        placeholders = ", ".join(["%s"] * len(columns))
        cols = ", ".join(columns)
        cursor.executemany(
            f"INSERT INTO {table} ({cols}) VALUES ({placeholders});",
            values
        )
        return len(values)

    total_inserted = 0

    total_inserted += insert("cuisine", ["id", "name"], data["cuisine"])
    total_inserted += insert("meal_type", ["id", "name"], data["meal_type"])
    total_inserted += insert("diet", ["id", "type"], data["diet"])
    total_inserted += insert("difficulty", ["id", "level"], data["difficulty"])
    total_inserted += insert("ingredients", ["id", "name"], data["ingredients"])

    total_inserted += insert("users", ["id", "email", "login", "password"], data["users"])
    total_inserted += insert("recipes", ["id", "title", "description", "cook_time", "serving_size", "views", "rating",
                                           "id_cuisine", "id_diet", "id_difficulty", "id_meal_type"], data["recipes"])

    cursor.executemany(
        "INSERT INTO recipes_ingredients (id, id_recipe, id_ingredient, quantity, measurement) VALUES (%s, %s, %s, %s, %s);",
        list(data["recipes_ingredients"].itertuples(index=False, name=None))
    )
    total_inserted += len(data["recipes_ingredients"])

    cursor.executemany(
        "INSERT INTO instructions (id, recipe_id, step_number, description) VALUES (%s, %s, %s, %s);",
        list(data["instructions"].itertuples(index=False, name=None))
    )
    total_inserted += len(data["instructions"])

    conn.commit()
    cursor.close()
    conn.close()
    print(f"\n✅ PostgreSQL. Zaimportowano dokładnie {total_inserted:,} rekordów.")

if __name__ == "__main__":
    print("Ten moduł powinien być uruchamiany z poziomu import_unstable.py, który przekazuje dane.")
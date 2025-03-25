import psycopg2
import io

def create_postgres_tables(cursor):
    print("🔧 Tworzenie tabel...")
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
            id_recipe INTEGER REFERENCES recipes(id),
            step_number INTEGER,
            description TEXT
        );""",
        """
        CREATE TABLE IF NOT EXISTS nutrition (
            id INTEGER PRIMARY KEY,
            id_recipe INTEGER REFERENCES recipes(id),
            calories INTEGER,
            carbohydrates FLOAT,
            protein FLOAT,
            fat FLOAT,
            fiber FLOAT,
            salt FLOAT,
            saturated_fat FLOAT,
            sugars FLOAT
        );""",
        """
        CREATE TABLE IF NOT EXISTS users_recipes (
            id INTEGER PRIMARY KEY,
            id_recipe INTEGER REFERENCES recipes(id),
            id_user INTEGER REFERENCES users(id),
            saved_at TIMESTAMP
        );""",
        """
        CREATE TABLE IF NOT EXISTS rating (
            id INTEGER PRIMARY KEY,
            id_recipe INTEGER REFERENCES recipes(id),
            id_user INTEGER REFERENCES users(id),
            value FLOAT
        );"""
    ]

    for query in table_queries:
        cursor.execute(query)

def truncate_postgres_tables(cursor):
    print("🧨 Usuwanie istniejących tabel...")
    tables = ["instructions", "recipes_ingredients", "nutrition", "users_recipes", "rating",
              "recipes", "users", "cuisine", "meal_type", "diet", "difficulty", "ingredients"]
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")

def copy_from_dataframe(cursor, df, table, columns):
    print(f"📥 COPY -> {table} ({len(df):,} rekordów)")
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, sep='\t')
    buffer.seek(0)
    cursor.copy_expert(f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, DELIMITER '\t');", buffer)
    return len(df)

def import_postgres_data(data: dict):
    print("\n🚀 Rozpoczynanie importu do PostgreSQL...")
    conn = psycopg2.connect(host="localhost", port=5432, user="postgres", password="example", dbname="testdb")
    cursor = conn.cursor()

    truncate_postgres_tables(cursor)
    create_postgres_tables(cursor)

    def insert(table, columns, df):
        print(f"📄 INSERT -> {table} ({len(df):,} rekordów)")
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

    total_inserted += copy_from_dataframe(cursor, data["users"], "users", ["id", "email", "login", "password"])
    total_inserted += copy_from_dataframe(cursor, data["recipes"], "recipes", ["id", "title", "description", "cook_time", "serving_size", "views", "rating", "id_cuisine", "id_diet", "id_difficulty", "id_meal_type"])
    total_inserted += copy_from_dataframe(cursor, data["recipes_ingredients"], "recipes_ingredients", ["id", "id_recipe", "id_ingredient", "quantity", "measurement"])
    total_inserted += copy_from_dataframe(cursor, data["instructions"], "instructions", ["id", "id_recipe", "step_number", "description"])
    total_inserted += copy_from_dataframe(cursor, data["nutrition"], "nutrition", ["id", "id_recipe", "calories", "carbohydrates", "protein", "fat", "fiber", "salt", "saturated_fat", "sugars"])
    total_inserted += copy_from_dataframe(cursor, data["users_recipes"], "users_recipes", ["id", "id_recipe", "id_user", "saved_at"])
    total_inserted += copy_from_dataframe(cursor, data["rating"], "rating", ["id", "id_recipe", "id_user", "value"])

    conn.commit()
    cursor.close()
    conn.close()
    print(f"\n✅ PostgreSQL. Zaimportowano dokładnie {total_inserted:,} rekordów.")

if __name__ == "__main__":
    print("Ten moduł powinien być uruchamiany z poziomu import.py, który przekazuje dane.")

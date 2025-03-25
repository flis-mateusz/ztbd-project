import mysql.connector
from helpers.csv_loader import load_csv
from tqdm import tqdm

def create_mysql_tables(cursor):
    print("🔧 Tworzenie tabel...")
    table_queries = [
        "CREATE TABLE IF NOT EXISTS cuisine (id INT PRIMARY KEY, name VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS meal_type (id INT PRIMARY KEY, name VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS diet (id INT PRIMARY KEY, type VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS difficulty (id INT PRIMARY KEY, level VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS ingredients (id INT PRIMARY KEY, name VARCHAR(100));",
        """
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            email VARCHAR(255),
            login VARCHAR(100),
            password VARCHAR(255)
        );""",
        """
        CREATE TABLE IF NOT EXISTS recipes (
            id INT PRIMARY KEY,
            title VARCHAR(255),
            description TEXT,
            cook_time INT,
            serving_size INT,
            views INT,
            rating FLOAT,
            id_cuisine INT,
            id_diet INT,
            id_difficulty INT,
            id_meal_type INT,
            FOREIGN KEY (id_cuisine) REFERENCES cuisine(id),
            FOREIGN KEY (id_diet) REFERENCES diet(id),
            FOREIGN KEY (id_difficulty) REFERENCES difficulty(id),
            FOREIGN KEY (id_meal_type) REFERENCES meal_type(id)
        );""",
        """
        CREATE TABLE IF NOT EXISTS recipes_ingredients (
            id INT PRIMARY KEY,
            id_recipe INT,
            id_ingredient INT,
            quantity FLOAT,
            measurement VARCHAR(50),
            FOREIGN KEY (id_recipe) REFERENCES recipes(id),
            FOREIGN KEY (id_ingredient) REFERENCES ingredients(id)
        );""",
        """
        CREATE TABLE IF NOT EXISTS instructions (
            id INT PRIMARY KEY,
            recipe_id INT,
            step_number INT,
            description TEXT,
            FOREIGN KEY (recipe_id) REFERENCES recipes(id)
        );"""
    ]

    for query in table_queries:
        cursor.execute(query)

def truncate_mysql_tables(cursor):
    print("🧹 Czyszczenie tabel...")
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    tables = ["instructions", "recipes_ingredients", "recipes", "users", "cuisine", "meal_type", "diet", "difficulty", "ingredients"]
    for table in tables:
        cursor.execute(f"TRUNCATE TABLE {table};")
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

def import_mysql_data(data: dict):
    print("\n🚀 Rozpoczynanie importu do MySQL...")
    conn = mysql.connector.connect(host="localhost", port=3306, user="root", password="example", database="testdb")
    cursor = conn.cursor()

    create_mysql_tables(cursor)
    truncate_mysql_tables(cursor)

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

    total_inserted += insert("users", ["id", "email", "login", "password"], data["users"])
    total_inserted += insert("recipes", ["id", "title", "description", "cook_time", "serving_size", "views", "rating",
                                           "id_cuisine", "id_diet", "id_difficulty", "id_meal_type"], data["recipes"])

    print(f"📄 INSERT -> recipes_ingredients ({len(data['recipes_ingredients']):,} rekordów)")
    cursor.executemany(
        "INSERT INTO recipes_ingredients (id, id_recipe, id_ingredient, quantity, measurement) VALUES (%s, %s, %s, %s, %s);",
        list(data["recipes_ingredients"].itertuples(index=False, name=None))
    )
    total_inserted += len(data["recipes_ingredients"])

    print(f"📄 INSERT -> instructions ({len(data['instructions']):,} rekordów)")
    cursor.executemany(
        "INSERT INTO instructions (id, recipe_id, step_number, description) VALUES (%s, %s, %s, %s);",
        list(data["instructions"].itertuples(index=False, name=None))
    )
    total_inserted += len(data["instructions"])

    conn.commit()
    cursor.close()
    conn.close()
    print(f"\n✅ MySQL. Zaimportowano dokładnie {total_inserted:,} rekordów.")

if __name__ == "__main__":
    print("Ten moduł powinien być uruchamiany z poziomu import_unstable.py, który przekazuje dane.")

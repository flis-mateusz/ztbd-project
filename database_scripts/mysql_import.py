import csv
import io

import mysql.connector


# ────────────────────────────────────────────────────────────────────
#  Tworzenie tabel
# ────────────────────────────────────────────────────────────────────
def create_mysql_tables(cursor):
    print("🔧 Tworzenie tabel...")
    table_queries = [
        "CREATE TABLE IF NOT EXISTS cuisine      (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS meal_type    (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS diet         (id INT PRIMARY KEY AUTO_INCREMENT, type  VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS difficulty   (id INT PRIMARY KEY AUTO_INCREMENT, level VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS ingredients  (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100));",
        """
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY AUTO_INCREMENT,
            email VARCHAR(255),
            login VARCHAR(100),
            password VARCHAR(255)
        );""",
        """
        CREATE TABLE IF NOT EXISTS recipes (
            id INT PRIMARY KEY AUTO_INCREMENT,
            id_user INT,
            title VARCHAR(255),
            description TEXT,
            created_at DATETIME,
            cook_time INT,
            serving_size INT,
            views INT,
            rating FLOAT,
            id_cuisine INT,
            id_diet INT,
            id_difficulty INT,
            id_meal_type INT,
            FOREIGN KEY (id_user)       REFERENCES users(id),
            FOREIGN KEY (id_cuisine)    REFERENCES cuisine(id),
            FOREIGN KEY (id_diet)       REFERENCES diet(id),
            FOREIGN KEY (id_difficulty) REFERENCES difficulty(id),
            FOREIGN KEY (id_meal_type)  REFERENCES meal_type(id)
        );""",
        """
        CREATE TABLE IF NOT EXISTS recipes_ingredients (
            id INT PRIMARY KEY AUTO_INCREMENT,
            id_recipe INT,
            id_ingredient INT,
            quantity FLOAT,
            measurement VARCHAR(50),
            FOREIGN KEY (id_recipe)     REFERENCES recipes(id),
            FOREIGN KEY (id_ingredient) REFERENCES ingredients(id)
        );""",
        """
        CREATE TABLE IF NOT EXISTS instructions (
            id INT PRIMARY KEY AUTO_INCREMENT,
            id_recipe INT,
            step_number INT,
            description TEXT,
            FOREIGN KEY (id_recipe) REFERENCES recipes(id)
        );""",
        """
        CREATE TABLE IF NOT EXISTS nutrition (
            id INT PRIMARY KEY AUTO_INCREMENT,
            id_recipe INT,
            calories INT,
            carbohydrates FLOAT,
            protein FLOAT,
            fat FLOAT,
            fiber FLOAT,
            salt FLOAT,
            saturated_fat FLOAT,
            sugars FLOAT,
            FOREIGN KEY (id_recipe) REFERENCES recipes(id)
        );""",
        """
        CREATE TABLE IF NOT EXISTS users_favourite (
            id INT PRIMARY KEY AUTO_INCREMENT,
            id_recipe INT,
            id_user INT,
            saved_at DATETIME,
            FOREIGN KEY (id_recipe) REFERENCES recipes(id),
            FOREIGN KEY (id_user)   REFERENCES users(id)
        );""",
        """
        CREATE TABLE IF NOT EXISTS rating (
            id INT PRIMARY KEY AUTO_INCREMENT,
            id_recipe INT,
            id_user INT,
            value FLOAT,
            FOREIGN KEY (id_recipe) REFERENCES recipes(id),
            FOREIGN KEY (id_user)   REFERENCES users(id)
        );"""
    ]
    for query in table_queries:
        cursor.execute(query)


# ────────────────────────────────────────────────────────────────────
#  DROP wszystkich tabel
# ────────────────────────────────────────────────────────────────────
def truncate_mysql_tables(cursor):
    print("🧨 Usuwanie istniejących tabel...")
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    tables = [
        "instructions", "recipes_ingredients", "nutrition",
        "users_favourite", "rating",
        "recipes", "users",
        "cuisine", "meal_type", "diet", "difficulty", "ingredients", "users_recipes"  # OLD
    ]
    for tbl in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {tbl};")
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")


# ────────────────────────────────────────────────────────────────────
#  Szybki LOAD DATA dla dużych plików
# ────────────────────────────────────────────────────────────────────
def load_from_dataframe_mysql(cursor, df, table, columns):
    print(f"📥 LOAD -> {table} ({len(df):,} rekordów)")
    buf = io.StringIO()
    if "id" in df.columns:
        df = df.drop(columns=["id"])
    df.to_csv(
        buf, index=False, header=False, sep="\t",
        quoting=csv.QUOTE_MINIMAL, escapechar="\\", lineterminator="\n"
    )
    buf.seek(0)
    with open("generated_data/temp_load_file.csv", "w", encoding="utf-8") as f:
        f.write(buf.getvalue())

    cursor.execute(f"""
        LOAD DATA LOCAL INFILE 'generated_data/temp_load_file.csv'
        INTO TABLE {table}
        FIELDS TERMINATED BY '\t'
        OPTIONALLY ENCLOSED BY '"'
        ESCAPED BY '\\\\'
        LINES TERMINATED BY '\n'
        ({', '.join(columns)})
    """)
    return len(df)


# ────────────────────────────────────────────────────────────────────
#  Główny importer
# ────────────────────────────────────────────────────────────────────
def import_mysql_data(data: dict, total_records: int):
    print("\n🚀 Import do MySQL…")
    conn = mysql.connector.connect(
        host="127.0.0.1", port=3306, user="root", password="example",
        database="testdb", allow_local_infile=True
    )
    cur = conn.cursor()
    cur.execute("SET GLOBAL local_infile = 1;")

    truncate_mysql_tables(cur)
    create_mysql_tables(cur)

    # ---------- helper insert ----------
    def insert(table, columns, df):
        print(f"📄 INSERT -> {table} ({len(df):,} rekordów)")
        if df.empty:
            print(f"⚠️  DataFrame {table} pusty – pominięto.")
            return 0
        rows = [tuple(row[col] for col in columns) for _, row in df.iterrows()]
        placeholders = ", ".join(["%s"] * len(columns))
        cur.executemany(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders});",
            rows
        )
        return len(rows)

    total = 0
    total += insert("cuisine", ["name"], data["cuisine"])
    total += insert("meal_type", ["name"], data["meal_type"])
    total += insert("diet", ["type"], data["diet"])
    total += insert("difficulty", ["level"], data["difficulty"])
    total += insert("ingredients", ["name"], data["ingredients"])
    total += insert("users", ["email", "login", "password"], data["users"])

    # ---------- recipes & zależne ----------
    recipe_cols = ["id_user", "title", "description", "created_at", "cook_time",
                   "serving_size", "views", "rating",
                   "id_cuisine", "id_diet", "id_difficulty", "id_meal_type"]

    if total_records > 1_000_000:
        total += load_from_dataframe_mysql(cur, data["recipes"], "recipes", recipe_cols)
        total += load_from_dataframe_mysql(cur, data["recipes_ingredients"],
                                           "recipes_ingredients",
                                           ["id_recipe", "id_ingredient", "quantity", "measurement"])
        total += load_from_dataframe_mysql(cur, data["instructions"],
                                           "instructions",
                                           ["id_recipe", "step_number", "description"])
        total += load_from_dataframe_mysql(cur, data["users_favourite"],
                                           'users_favourite',
                                           ["id_recipe", "id_user", "saved_at"])
    else:
        total += insert("recipes", recipe_cols, data["recipes"])
        total += insert("recipes_ingredients",
                        ["id_recipe", "id_ingredient", "quantity", "measurement"],
                        data["recipes_ingredients"])
        total += insert("instructions",
                        ["id_recipe", "step_number", "description"],
                        data["instructions"])
        total += insert("users_favourite",
                        ["id_recipe", "id_user", "saved_at"],
                        data["users_favourite"])

    total += insert("nutrition",
                    ["id_recipe", "calories", "carbohydrates", "protein", "fat",
                     "fiber", "salt", "saturated_fat", "sugars"],
                    data["nutrition"])
    total += insert("rating",
                    ["id_recipe", "id_user", "value"],
                    data["rating"])

    conn.commit()
    cur.close()
    conn.close()
    print(f"\n✅ MySQL – zaimportowano {total:,} rekordów.")


if __name__ == "__main__":
    print("Ten moduł wywołuje się z importera nadrzędnego – nie uruchamiaj bez przekazania danych.")

import csv
import io

import mysql.connector


def create_mysql_tables(cursor):
    print("🔧 Tworzenie tabel...")
    table_queries = [
        "CREATE TABLE IF NOT EXISTS cuisine (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS meal_type (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS diet (id INT PRIMARY KEY AUTO_INCREMENT, type VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS difficulty (id INT PRIMARY KEY AUTO_INCREMENT, level VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS ingredients (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100));",
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
            id INT PRIMARY KEY AUTO_INCREMENT,
            id_recipe INT,
            id_ingredient INT,
            quantity FLOAT,
            measurement VARCHAR(50),
            FOREIGN KEY (id_recipe) REFERENCES recipes(id),
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
        CREATE TABLE IF NOT EXISTS users_recipes (
            id INT PRIMARY KEY AUTO_INCREMENT,
            id_recipe INT,
            id_user INT,
            saved_at DATETIME,
            FOREIGN KEY (id_recipe) REFERENCES recipes(id),
            FOREIGN KEY (id_user) REFERENCES users(id)
        );""",
        """
        CREATE TABLE IF NOT EXISTS rating (
            id INT PRIMARY KEY AUTO_INCREMENT,
            id_recipe INT,
            id_user INT,
            value FLOAT,
            FOREIGN KEY (id_recipe) REFERENCES recipes(id),
            FOREIGN KEY (id_user) REFERENCES users(id)
        );"""
    ]

    for query in table_queries:
        cursor.execute(query)


def truncate_mysql_tables(cursor):
    print("🧨 Usuwanie istniejących tabel...")
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    tables = ["instructions", "recipes_ingredients", "nutrition", "users_recipes", "rating",
              "recipes", "users", "cuisine", "meal_type", "diet", "difficulty", "ingredients"]
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table};")
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")


def load_from_dataframe_mysql(cursor, df, table, columns):
    print(f"📥 LOAD -> {table} ({len(df):,} rekordów)")
    buffer = io.StringIO()
    if 'id' in df.columns:
        df = df.drop(columns=['id'])
    df.to_csv(
        buffer,
        index=False,
        header=False,
        sep='\t',
        quoting=csv.QUOTE_MINIMAL,
        escapechar='\\',
        lineterminator='\n'
    )
    buffer.seek(0)

    # zapisz do pliku tymczasowego
    with open('generated_data/temp_load_file.csv', 'w', encoding='utf-8') as f:
        f.write(buffer.getvalue())

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


def import_mysql_data(data: dict, total_records):
    print("\n🚀 Rozpoczynanie importu do MySQL...")
    conn = mysql.connector.connect(host="localhost", port=3306, user="root", password="example", database="testdb",
                                   allow_local_infile=True)
    cursor = conn.cursor()
    cursor.execute("SET GLOBAL local_infile = 1;")

    truncate_mysql_tables(cursor)
    create_mysql_tables(cursor)

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

    total_inserted += insert("cuisine", ["name"], data["cuisine"])
    total_inserted += insert("meal_type", ["name"], data["meal_type"])
    total_inserted += insert("diet", ["type"], data["diet"])
    total_inserted += insert("difficulty", ["level"], data["difficulty"])
    total_inserted += insert("ingredients", ["name"], data["ingredients"])

    total_inserted += insert("users", ["email", "login", "password"], data["users"])

    if total_records > 1_000_000:
        total_inserted += load_from_dataframe_mysql(cursor, data["recipes"],
                                                    "recipes",
                                                    ["title", "description", "cook_time", "serving_size", "views",
                                                     "rating", "id_cuisine", "id_diet", "id_difficulty",
                                                     "id_meal_type"])

        total_inserted += load_from_dataframe_mysql(cursor, data["recipes_ingredients"],
                                                    "recipes_ingredients",
                                                    ["id_recipe", "id_ingredient", "quantity", "measurement"])

        total_inserted += load_from_dataframe_mysql(cursor, data["instructions"],
                                                    "instructions",
                                                    ["id_recipe", "step_number", "description"])
    else:
        total_inserted += insert("recipes",
                                 ["title", "description", "cook_time", "serving_size", "views", "rating",
                                  "id_cuisine", "id_diet", "id_difficulty", "id_meal_type"], data["recipes"])

        total_inserted += insert("recipes_ingredients", ["id_recipe", "id_ingredient", "quantity", "measurement"],
                                 data["recipes_ingredients"])

        total_inserted += insert("instructions", ["id_recipe", "step_number", "description"],
                                 data["instructions"])

    total_inserted += insert("nutrition",
                             ["id_recipe", "calories", "carbohydrates", "protein", "fat", "fiber", "salt",
                              "saturated_fat", "sugars"], data["nutrition"])
    total_inserted += insert("users_recipes", ["id_recipe", "id_user", "saved_at"], data["users_recipes"])
    total_inserted += insert("rating", ["id_recipe", "id_user", "value"], data["rating"])

    conn.commit()
    cursor.close()
    conn.close()
    print(f"\n✅ MySQL. Zaimportowano dokładnie {total_inserted:,} rekordów.")


if __name__ == "__main__":
    print("Ten moduł powinien być uruchamiany z poziomu import.py, który przekazuje dane.")

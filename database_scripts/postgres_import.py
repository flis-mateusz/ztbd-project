import io
import psycopg2


# ─────────────────────────────────────────────────────────────────────────────
#  Tworzenie tabel (nowy schemat)
# ─────────────────────────────────────────────────────────────────────────────
def create_postgres_tables(cur):
    print("🔧 Tworzenie tabel...")
    q = [
        "CREATE TABLE IF NOT EXISTS cuisine      (id SERIAL PRIMARY KEY, name VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS meal_type    (id SERIAL PRIMARY KEY, name VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS diet         (id SERIAL PRIMARY KEY, type  VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS difficulty   (id SERIAL PRIMARY KEY, level VARCHAR(100));",
        "CREATE TABLE IF NOT EXISTS ingredients  (id SERIAL PRIMARY KEY, name VARCHAR(100));",
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255),
            login VARCHAR(100),
            password VARCHAR(255)
        );""",
        """
        CREATE TABLE IF NOT EXISTS recipes (
            id SERIAL PRIMARY KEY,
            id_user      INTEGER REFERENCES users(id),
            title        VARCHAR(255),
            description  TEXT,
            created_at   TIMESTAMP,
            cook_time    INTEGER,
            serving_size INTEGER,
            views        INTEGER,
            rating       FLOAT,
            id_cuisine   INTEGER REFERENCES cuisine(id),
            id_diet      INTEGER REFERENCES diet(id),
            id_difficulty INTEGER REFERENCES difficulty(id),
            id_meal_type  INTEGER REFERENCES meal_type(id)
        );""",
        """
        CREATE TABLE IF NOT EXISTS recipes_ingredients (
            id SERIAL PRIMARY KEY,
            id_recipe    INTEGER REFERENCES recipes(id),
            id_ingredient INTEGER REFERENCES ingredients(id),
            quantity     FLOAT,
            measurement  VARCHAR(50)
        );""",
        """
        CREATE TABLE IF NOT EXISTS instructions (
            id SERIAL PRIMARY KEY,
            id_recipe   INTEGER REFERENCES recipes(id),
            step_number INTEGER,
            description TEXT
        );""",
        """
        CREATE TABLE IF NOT EXISTS nutrition (
            id SERIAL PRIMARY KEY,
            id_recipe     INTEGER REFERENCES recipes(id),
            calories      INTEGER,
            carbohydrates FLOAT,
            protein       FLOAT,
            fat           FLOAT,
            fiber         FLOAT,
            salt          FLOAT,
            saturated_fat FLOAT,
            sugars        FLOAT
        );""",
        """
        CREATE TABLE IF NOT EXISTS users_favourite (
            id SERIAL PRIMARY KEY,
            id_recipe INTEGER REFERENCES recipes(id),
            id_user   INTEGER REFERENCES users(id),
            saved_at  TIMESTAMP
        );""",
        """
        CREATE TABLE IF NOT EXISTS rating (
            id SERIAL PRIMARY KEY,
            id_recipe INTEGER REFERENCES recipes(id),
            id_user   INTEGER REFERENCES users(id),
            value     FLOAT
        );"""
    ]
    for stmt in q:
        cur.execute(stmt)


# ─────────────────────────────────────────────────────────────────────────────
#  Drop & truncate
# ─────────────────────────────────────────────────────────────────────────────
def truncate_postgres_tables(cur):
    print("🧨 Usuwanie istniejących tabel...")
    tables = [
        "instructions", "recipes_ingredients", "nutrition",
        "users_favourite", "rating",
        "recipes", "users",
        "cuisine", "meal_type", "diet", "difficulty", "ingredients", "users_recipes" # OLD
    ]
    for t in tables:
        cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE;")


# ─────────────────────────────────────────────────────────────────────────────
#  COPY helper
# ─────────────────────────────────────────────────────────────────────────────
def copy_from_dataframe(cur, df, table, columns):
    print(f"📥 COPY -> {table} ({len(df):,} rekordów)")
    if "id" in df.columns:
        df = df.drop(columns=["id"])
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, sep="\t")
    buf.seek(0)
    cols = ", ".join(columns)
    cur.copy_expert(
        f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv, DELIMITER '\t');",
        buf
    )
    return len(df)


# ─────────────────────────────────────────────────────────────────────────────
#  INSERT helper
# ─────────────────────────────────────────────────────────────────────────────
def insert(cur, table, columns, df):
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


# ─────────────────────────────────────────────────────────────────────────────
#  Główny importer
# ─────────────────────────────────────────────────────────────────────────────
def import_postgres_data(data: dict):
    print("\n🚀 Import do PostgreSQL…")
    conn = psycopg2.connect(
        host="127.0.0.1", port=5432,
        user="postgres", password="example", dbname="testdb"
    )
    cur = conn.cursor()

    truncate_postgres_tables(cur)
    create_postgres_tables(cur)

    total = 0
    total += insert(cur, "cuisine",     ["name"],  data["cuisine"])
    total += insert(cur, "meal_type",   ["name"],  data["meal_type"])
    total += insert(cur, "diet",        ["type"],  data["diet"])
    total += insert(cur, "difficulty",  ["level"], data["difficulty"])
    total += insert(cur, "ingredients", ["name"],  data["ingredients"])

    total += copy_from_dataframe(cur, data["users"],
                                 "users", ["email", "login", "password"])

    recipe_cols = ["id_user", "title", "description", "created_at", "cook_time",
                   "serving_size", "views", "rating",
                   "id_cuisine", "id_diet", "id_difficulty", "id_meal_type"]

    total += copy_from_dataframe(cur, data["recipes"], "recipes", recipe_cols)
    total += copy_from_dataframe(cur, data["recipes_ingredients"],
                                 "recipes_ingredients",
                                 ["id_recipe", "id_ingredient", "quantity", "measurement"])
    total += copy_from_dataframe(cur, data["instructions"],
                                 "instructions",
                                 ["id_recipe", "step_number", "description"])
    total += copy_from_dataframe(cur, data["nutrition"],
                                 "nutrition",
                                 ["id_recipe", "calories", "carbohydrates", "protein",
                                  "fat", "fiber", "salt", "saturated_fat", "sugars"])

    total += copy_from_dataframe(cur, data["users_favourite"],
                                 "users_favourite",
                                 ["id_recipe", "id_user", "saved_at"])
    total += copy_from_dataframe(cur, data["rating"],
                                 "rating",
                                 ["id_recipe", "id_user", "value"])

    conn.commit()
    cur.close()
    conn.close()
    print(f"\n✅ PostgreSQL – zaimportowano {total:,} rekordów.")


if __name__ == "__main__":
    print("Uruchamiaj ten moduł z importera głównego – potrzebuje słownika `data`.")

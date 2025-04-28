from pymongo import MongoClient
from tqdm import tqdm


# ─────────────────────────────────────────────────────────────────────────────
#  Główny importer
# ─────────────────────────────────────────────────────────────────────────────
def import_mongo_data(data: dict, port: int = 27017, db_name: str = "testdb"):
    print(f"\n🚀 Import danych do MongoDB (port {port})...")
    client = MongoClient("localhost", port)
    db = client[db_name]

    # ---------- kolekcje słownikowe + users + users_favourite ----------
    collections = [
        ("cuisine", ["id", "name"]),
        ("meal_type", ["id", "name"]),
        ("diet", ["id", "type"]),
        ("difficulty", ["id", "level"]),
        ("ingredients", ["id", "name"]),
        ("users", ["id", "email", "login", "password"]),
        ("users_favourite", ["id", "id_recipe", "id_user", "saved_at"]),
    ]

    total_inserted = 0
    for name, _ in collections:
        coll = db[name]
        coll.delete_many({})
        df = data[name]
        if not df.empty:
            coll.insert_many(df.to_dict("records"))
            total_inserted += len(df)

    # ---------- budowanie dokumentów recipes ----------
    print("🛠️  Budowanie dokumentów 'recipes' z relacyjnych danych…")

    recipes_df = data["recipes"]
    ingredients_df = data["recipes_ingredients"]
    instructions_df = data["instructions"]
    nutrition_df = data["nutrition"]
    ratings_df = data["rating"]

    db["recipes"].delete_many({})

    docs = []
    for _, row in tqdm(recipes_df.iterrows(),
                       total=len(recipes_df), desc="📦 Tworzenie dokumentów"):
        rid = row["id"]
        recipe = row.to_dict()  # zawiera id_user i created_at

        recipe["ingredients"] = ingredients_df[
            ingredients_df["id_recipe"] == rid
            ][["id_ingredient", "quantity", "measurement"]].to_dict("records")

        recipe["instructions"] = instructions_df[
            instructions_df["id_recipe"] == rid
            ][["step_number", "description"]].sort_values("step_number").to_dict("records")

        nrow = nutrition_df[nutrition_df["id_recipe"] == rid]
        recipe["nutrition"] = (nrow.drop(columns=["id", "id_recipe"])
                               .iloc[0].to_dict() if not nrow.empty else {})

        recipe["ratings"] = ratings_df[
            ratings_df["id_recipe"] == rid
            ][["id_user", "value"]].to_dict("records")

        docs.append(recipe)

    if docs:
        db["recipes"].insert_many(docs)
        total_inserted += len(docs)

    print(f"✅ MongoDB (port {port}): zaimportowano {total_inserted:,} rekordów.")


if __name__ == "__main__":
    print("Uruchamiaj ten moduł z nadrzędnego importera – wymaga słownika `data`.")

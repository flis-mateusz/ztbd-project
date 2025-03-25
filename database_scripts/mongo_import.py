from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm


def import_mongo_data(data: dict, port: int = 27017, db_name: str = "testdb"):
    print(f"\n🚀 Import danych do MongoDB (port {port})...")
    client = MongoClient("localhost", port)
    db = client[db_name]

    # Kolekcje słownikowe i users
    collections = [
        ("cuisine", ["id", "name"]),
        ("meal_type", ["id", "name"]),
        ("diet", ["id", "type"]),
        ("difficulty", ["id", "level"]),
        ("ingredients", ["id", "name"]),
        ("users", ["id", "email", "login", "password"]),
        ("users_recipes", ["id", "id_recipe", "id_user", "saved_at"])
    ]

    total_inserted = 0

    for name, _ in collections:
        collection = db[name]
        collection.delete_many({})
        df = data[name]
        if not df.empty:
            collection.insert_many(df.to_dict("records"))
            total_inserted += len(df)

    # Budowanie dokumentu recipes
    print("🛠️  Budowanie dokumentów 'recipes' z relacyjnych danych...")

    recipes_df = data["recipes"]
    ingredients_df = data["recipes_ingredients"]
    instructions_df = data["instructions"]
    nutrition_df = data["nutrition"]
    ratings_df = data["rating"]

    db["recipes"].delete_many({})

    recipe_docs = []
    for _, row in tqdm(recipes_df.iterrows(), total=len(recipes_df), desc="📦 Tworzenie dokumentów"):
        recipe_id = row["id"]

        recipe = row.to_dict()

        recipe["ingredients"] = ingredients_df[ingredients_df["id_recipe"] == recipe_id][
            ["id_ingredient", "quantity", "measurement"]
        ].to_dict("records")

        recipe["instructions"] = instructions_df[instructions_df["id_recipe"] == recipe_id][
            ["step_number", "description"]
        ].sort_values("step_number").to_dict("records")

        nutri_row = nutrition_df[nutrition_df["id_recipe"] == recipe_id]
        recipe["nutrition"] = nutri_row.drop(columns=["id", "id_recipe"]).iloc[0].to_dict() if not nutri_row.empty else {}

        recipe["ratings"] = ratings_df[ratings_df["id_recipe"] == recipe_id][
            ["id_user", "value"]
        ].to_dict("records")

        recipe_docs.append(recipe)

    if recipe_docs:
        db["recipes"].insert_many(recipe_docs)
        total_inserted += len(recipe_docs)

    print(f"MongoDB (port {port}): zaimportowano ", f"{total_inserted:,} rekordów.")


if __name__ == "__main__":
    print("Ten moduł powinien być uruchamiany z poziomu import.py, który przekazuje dane.")
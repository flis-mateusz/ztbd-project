from helpers.csv_loader import load_csv
from mysql_import import import_mysql_data
from postgres_import import import_postgres_data

def prepare_shared_data():
    print("📦 Wczytywanie wszystkich danych z CSV...")

    return {
        "users": load_csv("users"),
        "recipes": load_csv("recipes"),
        "recipes_ingredients": load_csv("recipes_ingredients"),
        "instructions": load_csv("instructions"),
        "cuisine": load_csv("cuisine"),
        "meal_type": load_csv("meal_type"),
        "diet": load_csv("diet"),
        "difficulty": load_csv("difficulty"),
        "ingredients": load_csv("ingredients")
    }

if __name__ == "__main__":
    data = prepare_shared_data()

    import_mysql_data(data)
    import_postgres_data(data)

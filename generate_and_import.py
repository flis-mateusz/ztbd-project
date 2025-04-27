import os
import random
from pathlib import Path

import pandas as pd
from faker import Faker
from tqdm import tqdm

from database_scripts.helpers.csv_loader import load_csv
from database_scripts.mysql_import import import_mysql_data
from database_scripts.postgres_import import import_postgres_data
from database_scripts.mongo_import import import_mongo_data

fake = Faker()

# ──────────────────────────────────────────────────────────────────────────────
#  Konfiguracja stałych list
# ──────────────────────────────────────────────────────────────────────────────
CUISINES = [
    "Italian", "Mexican", "French", "Chinese", "Japanese", "Indian", "Thai",
    "Spanish", "Greek", "Turkish", "Vietnamese", "Moroccan", "Polish",
    "Korean", "Brazilian"
]
MEAL_TYPES = ["Breakfast", "Lunch", "Dinner", "Snack", "Dessert", "Brunch"]
DIETS = ["Vegetarian", "Vegan", "Gluten-Free", "Keto", "Paleo", "Low-Carb", "Low-Fat"]
DIFFICULTY_LEVELS = ["Easy", "Medium", "Hard", "Expert"]
INGREDIENTS = [
    "Tomato", "Chicken", "Rice", "Onion", "Garlic", "Beef", "Cheese", "Salt",
    "Pepper", "Olive Oil", "Basil", "Pasta", "Mushroom", "Carrot", "Cucumber",
    "Yogurt", "Potato", "Milk", "Butter", "Fish", "Eggs", "Lemon", "Spinach",
    "Avocado", "Corn", "Beans", "Bacon", "Oregano", "Parsley", "Honey"
]

# ──────────────────────────────────────────────────────────────────────────────
#  Główna funkcja
# ──────────────────────────────────────────────────────────────────────────────
def generate_and_import(total_records: int) -> dict:
    """
    Generuje dane, zapisuje CSV w generated_data/, importuje do baz
    i zwraca podsumowanie (liczbę wierszy na tabelę).
    """
    # 1.  Generowanie ─────────────────────────────────────────────────────────
    Path("generated_data").mkdir(exist_ok=True)

    data_users, data_recipes, data_instructions = [], [], []
    data_ing_rel, data_nutrition, data_users_recipes, data_ratings = [], [], [], []

    user_id = recipe_id = instruction_id = ing_rel_id = nutrition_id = 1
    users_recipes_id = rating_id = 1
    total_so_far = 0

    with tqdm(total=total_records, dynamic_ncols=True, desc="Generowanie") as pbar:
        while total_so_far < total_records:
            # ── Users
            data_users.append({
                "id": user_id,
                "email": fake.unique.email(),
                "login": fake.unique.user_name(),
                "password": fake.password(length=12)
            })
            total_so_far += 1
            pbar.update(1)

            for _ in range(random.randint(2, 5)):
                if total_so_far >= total_records:
                    break

                # ── Recipes
                data_recipes.append({
                    "id": recipe_id,
                    "title": fake.sentence(nb_words=4),
                    "description": fake.text(max_nb_chars=300),
                    "cook_time": random.randint(5, 240),
                    "serving_size": random.randint(1, 12),
                    "views": random.randint(0, 100000),
                    "rating": round(random.uniform(1, 5), 1),
                    "id_cuisine": random.randint(1, len(CUISINES)),
                    "id_diet": random.randint(1, len(DIETS)),
                    "id_difficulty": random.randint(1, len(DIFFICULTY_LEVELS)),
                    "id_meal_type": random.randint(1, len(MEAL_TYPES)),
                })
                total_so_far += 1
                pbar.update(1)

                # ── Ingredients relation
                for ing in random.sample(range(1, len(INGREDIENTS) + 1), random.randint(5, 12)):
                    data_ing_rel.append({
                        "id": ing_rel_id,
                        "id_recipe": recipe_id,
                        "id_ingredient": ing,
                        "quantity": round(random.uniform(0.1, 10), 2),
                        "measurement": random.choice(["g", "ml", "tsp", "tbsp", "cup", "piece"])
                    })
                    ing_rel_id += 1
                    total_so_far += 1
                    pbar.update(1)

                # ── Instructions
                for step in range(1, random.randint(3, 10) + 1):
                    data_instructions.append({
                        "id": instruction_id,
                        "id_recipe": recipe_id,
                        "step_number": step,
                        "description": fake.sentence(nb_words=10)
                    })
                    instruction_id += 1
                    total_so_far += 1
                    pbar.update(1)

                # ── Nutrition
                data_nutrition.append({
                    "id": nutrition_id,
                    "id_recipe": recipe_id,
                    "calories": random.randint(100, 900),
                    "carbohydrates": round(random.uniform(10, 100), 1),
                    "protein": round(random.uniform(5, 50), 1),
                    "fat": round(random.uniform(5, 60), 1),
                    "fiber": round(random.uniform(1, 20), 1),
                    "salt": round(random.uniform(0.1, 5), 2),
                    "saturated_fat": round(random.uniform(1, 20), 1),
                    "sugars": round(random.uniform(1, 30), 1),
                })
                nutrition_id += 1
                total_so_far += 1
                pbar.update(1)

                # ── Users-recipes
                if random.random() < 0.7:
                    data_users_recipes.append({
                        "id": users_recipes_id,
                        "id_recipe": recipe_id,
                        "id_user": user_id,
                        "saved_at": fake.date_time_this_year()
                    })
                    users_recipes_id += 1
                    total_so_far += 1
                    pbar.update(1)

                # ── Ratings
                for _ in range(random.randint(0, 3)):
                    rater_id = random.randint(1, user_id)
                    data_ratings.append({
                        "id": rating_id,
                        "id_recipe": recipe_id,
                        "id_user": rater_id,
                        "value": round(random.uniform(1, 5), 1),
                    })
                    rating_id += 1
                    total_so_far += 1
                    pbar.update(1)

                recipe_id += 1
            user_id += 1

    # 2.  Eksport CSV ────────────────────────────────────────────────────────
    print("🚚  Eksport CSV…")
    pd.DataFrame(data_users).to_csv("generated_data/users.csv", index=False)
    pd.DataFrame(data_recipes).to_csv("generated_data/recipes.csv", index=False)
    pd.DataFrame(data_ing_rel).to_csv("generated_data/recipes_ingredients.csv", index=False)
    pd.DataFrame(data_instructions).to_csv("generated_data/instructions.csv", index=False)
    pd.DataFrame(data_nutrition).to_csv("generated_data/nutrition.csv", index=False)
    pd.DataFrame(data_users_recipes).to_csv("generated_data/users_recipes.csv", index=False)
    pd.DataFrame(data_ratings).to_csv("generated_data/rating.csv", index=False)

    # statyczne
    pd.DataFrame({"id": range(1, len(CUISINES) + 1), "name": CUISINES}).to_csv("generated_data/cuisine.csv", index=False)
    pd.DataFrame({"id": range(1, len(MEAL_TYPES) + 1), "name": MEAL_TYPES}).to_csv("generated_data/meal_type.csv", index=False)
    pd.DataFrame({"id": range(1, len(DIETS) + 1), "type": DIETS}).to_csv("generated_data/diet.csv", index=False)
    pd.DataFrame({"id": range(1, len(DIFFICULTY_LEVELS) + 1), "level": DIFFICULTY_LEVELS}).to_csv("generated_data/difficulty.csv", index=False)
    pd.DataFrame({"id": range(1, len(INGREDIENTS) + 1), "name": INGREDIENTS}).to_csv("generated_data/ingredients.csv", index=False)

    # 3.  Import do baz ─────────────────────────────────────────────────────
    print("📦 Import danych do baz…")
    shared = {
        "users": load_csv("users"),
        "recipes": load_csv("recipes"),
        "recipes_ingredients": load_csv("recipes_ingredients"),
        "instructions": load_csv("instructions"),
        "nutrition": load_csv("nutrition"),
        "users_recipes": load_csv("users_recipes"),
        "rating": load_csv("rating"),
        "cuisine": load_csv("cuisine"),
        "meal_type": load_csv("meal_type"),
        "diet": load_csv("diet"),
        "difficulty": load_csv("difficulty"),
        "ingredients": load_csv("ingredients"),
    }

    import_mysql_data(shared)
    import_postgres_data(shared)
    import_mongo_data(shared, port=27017)
    import_mongo_data(shared, port=27018)

    # 4.  Podsumowanie zwrotne ──────────────────────────────────────────────
    return {
        "users": len(data_users),
        "recipes": len(data_recipes),
        "recipes_ingredients": len(data_ing_rel),
        "instructions": len(data_instructions),
        "nutrition": len(data_nutrition),
        "users_recipes": len(data_users_recipes),
        "rating": len(data_ratings),
        "TOTAL": total_so_far,
    }

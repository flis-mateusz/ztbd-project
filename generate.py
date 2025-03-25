import random
import pandas as pd
from faker import Faker
from tqdm import tqdm

fake = Faker()

# Limit łączny wszystkich rekordów we wszystkich tabelach
TOTAL_RECORDS = 1_000_000

# Statyczne dane
CUISINES = ["Italian", "Mexican", "French", "Chinese", "Japanese", "Indian", "Thai",
            "Spanish", "Greek", "Turkish", "Vietnamese", "Moroccan", "Polish", "Korean", "Brazilian"]
MEAL_TYPES = ["Breakfast", "Lunch", "Dinner", "Snack", "Dessert", "Brunch"]
DIETS = ["Vegetarian", "Vegan", "Gluten-Free", "Keto", "Paleo", "Low-Carb", "Low-Fat"]
DIFFICULTY_LEVELS = ["Easy", "Medium", "Hard", "Expert"]
INGREDIENTS = ["Tomato", "Chicken", "Rice", "Onion", "Garlic", "Beef", "Cheese", "Salt", "Pepper", "Olive Oil",
               "Basil", "Pasta", "Mushroom", "Carrot", "Cucumber", "Yogurt", "Potato", "Milk", "Butter", "Fish",
               "Eggs", "Lemon", "Spinach", "Avocado", "Corn", "Beans", "Bacon", "Oregano", "Parsley", "Honey"]

# Listy na dane
data_users = []
data_recipes = []
data_instructions = []
data_ingredients = []
data_nutrition = []
data_users_recipes = []
data_ratings = []

# Id trackery
user_id = 1
recipe_id = 1
instruction_id = 1
ingredient_rel_id = 1
nutrition_id = 1
users_recipes_id = 1
rating_id = 1

total_records = 0

print("\n🚀 Generowanie danych...\n")

with tqdm(total=TOTAL_RECORDS, dynamic_ncols=True) as pbar:
    while total_records < TOTAL_RECORDS:
        # Dodaj użytkownika
        data_users.append({
            "id": user_id,
            "email": fake.unique.email(),
            "login": fake.unique.user_name(),
            "password": fake.password(length=12)
        })
        user_recipes = random.randint(2, 5)
        total_records += 1
        pbar.update(1)

        for _ in range(user_recipes):
            if total_records >= TOTAL_RECORDS:
                break

            # Dodaj przepis
            recipe = {
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
            }
            data_recipes.append(recipe)
            total_records += 1
            pbar.update(1)

            # Dodaj składniki
            for ing in random.sample(range(1, len(INGREDIENTS)+1), random.randint(5, 12)):
                data_ingredients.append({
                    "id": ingredient_rel_id,
                    "id_recipe": recipe_id,
                    "id_ingredient": ing,
                    "quantity": round(random.uniform(0.1, 10), 2),
                    "measurement": random.choice(["g", "ml", "tsp", "tbsp", "cup", "piece"])
                })
                ingredient_rel_id += 1
                total_records += 1
                pbar.update(1)

            # Dodaj instrukcje
            for step in range(1, random.randint(3, 10)+1):
                data_instructions.append({
                    "id": instruction_id,
                    "id_recipe": recipe_id,
                    "step_number": step,
                    "description": fake.sentence(nb_words=10)
                })
                instruction_id += 1
                total_records += 1
                pbar.update(1)

            # Dodaj nutrition
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
            total_records += 1
            pbar.update(1)

            # Dodaj users_recipes (ulubione)
            if random.random() < 0.7:
                data_users_recipes.append({
                    "id": users_recipes_id,
                    "id_recipe": recipe_id,
                    "id_user": user_id,
                    "saved_at": fake.date_time_this_year()
                })
                users_recipes_id += 1
                total_records += 1
                pbar.update(1)

            # Dodaj ratingi
            num_ratings = random.randint(0, 3)
            for _ in range(num_ratings):
                rater_id = random.randint(1, user_id)  # istniejący user
                data_ratings.append({
                    "id": rating_id,
                    "id_recipe": recipe_id,
                    "id_user": rater_id,
                    "value": round(random.uniform(1, 5), 1),
                })
                rating_id += 1
                total_records += 1
                pbar.update(1)

            recipe_id += 1

        user_id += 1

print("✅ Generowanie zakończone. Eksport danych...")

# Eksport CSV
pd.DataFrame(data_users).to_csv("generated_data/users.csv", index=False)
pd.DataFrame(data_recipes).to_csv("generated_data/recipes.csv", index=False)
pd.DataFrame(data_ingredients).to_csv("generated_data/recipes_ingredients.csv", index=False)
pd.DataFrame(data_instructions).to_csv("generated_data/instructions.csv", index=False)
pd.DataFrame(data_nutrition).to_csv("generated_data/nutrition.csv", index=False)
pd.DataFrame(data_users_recipes).to_csv("generated_data/users_recipes.csv", index=False)
pd.DataFrame(data_ratings).to_csv("generated_data/rating.csv", index=False)

# Statyczne dane
pd.DataFrame({"id": range(1, len(CUISINES)+1), "name": CUISINES}).to_csv("generated_data/cuisine.csv", index=False)
pd.DataFrame({"id": range(1, len(MEAL_TYPES)+1), "name": MEAL_TYPES}).to_csv("generated_data/meal_type.csv", index=False)
pd.DataFrame({"id": range(1, len(DIETS)+1), "type": DIETS}).to_csv("generated_data/diet.csv", index=False)
pd.DataFrame({"id": range(1, len(DIFFICULTY_LEVELS)+1), "level": DIFFICULTY_LEVELS}).to_csv("generated_data/difficulty.csv", index=False)
pd.DataFrame({"id": range(1, len(INGREDIENTS)+1), "name": INGREDIENTS}).to_csv("generated_data/ingredients.csv", index=False)

# Podsumowanie
summary = len(data_users) + len(data_recipes) + len(data_ingredients) + len(data_instructions) + len(data_nutrition) + len(data_users_recipes) + len(data_ratings)
print(f"Łącznie wygenerowano {summary:,} rekordów.")
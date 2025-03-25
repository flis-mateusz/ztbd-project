import random

import pandas as pd
from faker import Faker
from tqdm import tqdm

fake = Faker()

TOTAL_RECORDS = 1_000_000

NUM_RECIPES = int(TOTAL_RECORDS / 16.18) # estymacja

USER_RECIPE_RATIO = 0.2
NUM_USERS = max(1, int(NUM_RECIPES * USER_RECIPE_RATIO))

# Statyczne dane
CUISINES = ["Italian", "Mexican", "French", "Chinese", "Japanese", "Indian", "Thai",
            "Spanish", "Greek", "Turkish", "Vietnamese", "Moroccan", "Polish", "Korean", "Brazilian"]
MEAL_TYPES = ["Breakfast", "Lunch", "Dinner", "Snack", "Dessert", "Brunch"]
DIETS = ["Vegetarian", "Vegan", "Gluten-Free", "Keto", "Paleo", "Low-Carb", "Low-Fat"]
DIFFICULTY_LEVELS = ["Easy", "Medium", "Hard", "Expert"]
INGREDIENTS = ["Tomato", "Chicken", "Rice", "Onion", "Garlic", "Beef", "Cheese", "Salt", "Pepper", "Olive Oil",
               "Basil", "Pasta", "Mushroom", "Carrot", "Cucumber", "Yogurt", "Potato", "Milk", "Butter", "Fish",
               "Eggs", "Lemon", "Spinach", "Avocado", "Corn", "Beans", "Bacon", "Oregano", "Parsley", "Honey"]


# Dynamiczne generowanie nazw przepisów
def generate_recipe_title():
    adj = ["Delicious", "Tasty", "Homemade", "Traditional", "Easy", "Quick", "Savory", "Sweet", "Spicy", "Classic"]
    dish_type = ["Soup", "Stew", "Salad", "Pie", "Casserole", "Stir Fry", "Pizza", "Sandwich", "Wrap", "Pasta", "Curry"]
    ingredient = random.choice(INGREDIENTS)
    cuisine = random.choice(CUISINES)
    return f"{random.choice(adj)} {cuisine} {ingredient} {random.choice(dish_type)}"


# Generowanie danych
def generate_users():
    return [
        {
            "id": i,
            "email": fake.unique.email(),
            "login": fake.unique.user_name(),
            "password": fake.password(length=12)
        } for i in tqdm(range(1, NUM_USERS + 1), desc="Generating users", dynamic_ncols=True)
    ]


def generate_recipes():
    return [
        {
            "id": i,
            "title": generate_recipe_title(),
            "description": fake.text(max_nb_chars=300),
            "cook_time": random.randint(5, 240),
            "serving_size": random.randint(1, 12),
            "views": random.randint(0, 100000),
            "rating": round(random.uniform(1, 5), 1),
            "id_cuisine": random.randint(1, len(CUISINES)),
            "id_diet": random.randint(1, len(DIETS)),
            "id_difficulty": random.randint(1, len(DIFFICULTY_LEVELS)),
            "id_meal_type": random.randint(1, len(MEAL_TYPES))
        } for i in tqdm(range(1, NUM_RECIPES + 1), desc="Generating recipes", dynamic_ncols=True)
    ]


def generate_recipe_ingredients():
    data = []
    id_counter = 1
    for recipe_id in tqdm(range(1, NUM_RECIPES + 1), desc="Generating ingredients", dynamic_ncols=True):
        ingredients_sample = random.sample(range(1, len(INGREDIENTS) + 1), random.randint(5, 12))
        for ingredient_id in ingredients_sample:
            data.append({
                "id": id_counter,
                "id_recipe": recipe_id,
                "id_ingredient": ingredient_id,
                "quantity": round(random.uniform(0.1, 10), 2),
                "measurement": random.choice(["g", "ml", "tsp", "tbsp", "cup", "piece"])
            })
            id_counter += 1
    return data


def generate_instructions():
    data = []
    instruction_id = 1
    for recipe_id in tqdm(range(1, NUM_RECIPES + 1), desc="Generating instructions", dynamic_ncols=True):
        steps = random.randint(3, 10)
        for step_number in range(1, steps + 1):
            data.append({
                "id": instruction_id,
                "recipe_id": recipe_id,
                "step_number": step_number,
                "description": fake.sentence(nb_words=12)
            })
            instruction_id += 1
    return data


# Generowanie danych
users = generate_users()
recipes = generate_recipes()
recipes_ingredients = generate_recipe_ingredients()
instructions = generate_instructions()

# Eksport danych
df_users = pd.DataFrame(users)
df_recipes = pd.DataFrame(recipes)
df_ingredients = pd.DataFrame(recipes_ingredients)
df_instructions = pd.DataFrame(instructions)

df_users.to_csv("generated_data/users.csv", index=False)
df_recipes.to_csv("generated_data/recipes.csv", index=False)
df_ingredients.to_csv("generated_data/recipes_ingredients.csv", index=False)
df_instructions.to_csv("generated_data/instructions.csv", index=False)

# Eksport statycznych danych
pd.DataFrame({"id": range(1, len(CUISINES) + 1), "name": CUISINES}).to_csv("generated_data/cuisine.csv", index=False)
pd.DataFrame({"id": range(1, len(MEAL_TYPES) + 1), "name": MEAL_TYPES}).to_csv("generated_data/meal_type.csv",
                                                                               index=False)
pd.DataFrame({"id": range(1, len(DIETS) + 1), "type": DIETS}).to_csv("generated_data/diet.csv", index=False)
pd.DataFrame({"id": range(1, len(DIFFICULTY_LEVELS) + 1), "level": DIFFICULTY_LEVELS}).to_csv(
    "generated_data/difficulty.csv", index=False)
pd.DataFrame({"id": range(1, len(INGREDIENTS) + 1), "name": INGREDIENTS}).to_csv("generated_data/ingredients.csv",
                                                                                 index=False)

# Podsumowanie
summary_total = len(df_users) + len(df_recipes) + len(df_ingredients) + len(df_instructions)
print(f"\nŁącznie wygenerowano {summary_total:,} rekordów.")

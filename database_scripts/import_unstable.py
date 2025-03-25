from helpers.csv_loader import load_csv
from mysql_import import import_mysql_data
from postgres_import import import_postgres_data

from helpers.csv_loader import load_csv

def prepare_shared_data(total_limit):
    # Proporcje dla kazdej kategorii
    p_users = 0.2
    p_recipes = 0.2
    p_ingredients = 0.3
    p_instructions = 0.3

    n_users = int(total_limit * p_users)
    n_recipes = int(total_limit * p_recipes)
    n_ingredients = int(total_limit * p_ingredients)
    n_instructions = int(total_limit * p_instructions)

    # Ładowanie danych głównych
    users_df = load_csv("users").head(n_users)
    recipes_df = load_csv("recipes").head(n_recipes)
    recipe_ids = set(recipes_df['id'])

    # Relacje – filtracja po istniejących przepisach, dopiero potem ograniczenie
    recipes_ingredients_df = load_csv("recipes_ingredients")
    instructions_df = load_csv("instructions")

    filtered_ri = recipes_ingredients_df[recipes_ingredients_df['id_recipe'].isin(recipe_ids)].head(n_ingredients)
    filtered_instr = instructions_df[instructions_df['recipe_id'].isin(recipe_ids)].head(n_instructions)

    # Dane słownikowe – zawsze w całości
    static_data = {
        "cuisine": load_csv("cuisine"),
        "meal_type": load_csv("meal_type"),
        "diet": load_csv("diet"),
        "difficulty": load_csv("difficulty"),
        "ingredients": load_csv("ingredients")
    }

    return {
        "users": users_df,
        "recipes": recipes_df,
        "recipes_ingredients": filtered_ri,
        "instructions": filtered_instr,
        **static_data
    }

if __name__ == "__main__":
    LIMIT = 1000000
    data = prepare_shared_data(LIMIT)

    import_mysql_data(data)
    import_postgres_data(data)

from .test_01_create_single_user import CreateSingleUserTest
from .test_02_bulk_insert_100_users import BulkInsert100UsersTest
from .test_03_create_single_recipe import CreateSingleRecipeTest
from .test_04_bulk_insert_ratings_1k import BulkInsertRatings1kTest
from .test_05_add_favourite import AddFavouriteTest
from .test_06_add_nutrition_for_last_recipe import AddNutritionForLastRecipeTest
from .test_07_add_instruction_steps import AddInstructionStepsTest
from .test_08_bulk_insert_ingredients_for_last_recipe import BulkInsertIngredientsForLastRecipeTest
from .test_09_add_new_diet_type import AddNewDietTypeTest
from .test_10_bulk_save_top50_favourites import BulkSaveTop50FavouritesTest

all_create_tests = [
    CreateSingleUserTest,
    BulkInsert100UsersTest,
    CreateSingleRecipeTest,
    BulkInsertRatings1kTest,
    AddFavouriteTest,
    AddNutritionForLastRecipeTest,
    AddInstructionStepsTest,
    BulkInsertIngredientsForLastRecipeTest,
    AddNewDietTypeTest,
    BulkSaveTop50FavouritesTest,
]

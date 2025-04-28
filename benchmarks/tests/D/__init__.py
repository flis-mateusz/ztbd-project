from .test_01_delete_all_favourites import DeleteAllFavouritesTest
from .test_02_delete_ratings_long_recipes import DeleteRatingsForLongRecipesTest
from .test_03_delete_old_favourites import DeleteOldFavouritesTest
from .test_04_delete_high_sodium_nutrition import DeleteHighSodiumNutritionTest
from .test_05_delete_high_calorie_favourites import DeleteHighCalorieFavouritesTest
from .test_06_delete_low_ratings import DeleteLowRatingsTest
from .test_07_delete_short_instructions import DeleteShortInstructionsTest
from .test_08_delete_high_calorie_high_fat_nutrition import DeleteHighCalorieHighFatNutritionTest
from .test_09_delete_tiny_ingredient_quantities import DeleteTinyIngredientQuantitiesTest
from .test_10_delete_low_ratings_popular_recipes import DeleteLowPopularityRecipesTest

all_delete_tests = [
    DeleteAllFavouritesTest,
    DeleteRatingsForLongRecipesTest,
    DeleteOldFavouritesTest,
    DeleteHighSodiumNutritionTest,
    DeleteHighCalorieFavouritesTest,
    DeleteLowRatingsTest,
    DeleteShortInstructionsTest,
    DeleteHighCalorieHighFatNutritionTest,
    DeleteTinyIngredientQuantitiesTest,
    DeleteLowPopularityRecipesTest
]

from .test_01_increment_recipe_views import IncrementRecipeViewsTest
from .test_02_round_recipe_rating import NormalizeRecipeRatingPrecisionTest
from .test_03_add_salt_buffer_high_calorie import AddSaltBufferHighCalorieTest
from .test_04_update_measurement_tsp import UpdateMeasurementUnitTspTest
from .test_05_boost_protein_low_fat import BoostProteinLowFatTest
from .test_06_flag_long_cook_time_recipes import FlagLongCookTimeRecipesTest
from .test_07_shift_saved_at_old_favourites import ShiftSavedAtOldFavouritesTest
from .test_08_reduce_serving_size_high_calories import ReduceServingSizeHighCaloriesTest
from .test_09_cap_sugar_very_sweet import CapSugarVerySweetTest
from .test_10_increase_difficulty_high_steps import IncreaseDifficultyHighStepsTest

all_update_tests = [
    IncrementRecipeViewsTest,
    NormalizeRecipeRatingPrecisionTest,
    AddSaltBufferHighCalorieTest,
    UpdateMeasurementUnitTspTest,
    BoostProteinLowFatTest,
    FlagLongCookTimeRecipesTest,
    ShiftSavedAtOldFavouritesTest,
    ReduceServingSizeHighCaloriesTest,
    CapSugarVerySweetTest,
    IncreaseDifficultyHighStepsTest
]

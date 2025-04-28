from .test_01_rating_by_cuisine import AverageRatingByCuisineTest
from .test_02_top_healthy_popular_recipes import TopHealthyPopularRecipesTest
from .test_03_most_common_ingredients import MostCommonIngredientsTest
from .test_04_top_recipes_per_cuisine import TopRecipesPerCuisineTest
from .test_05_most_active_authors import MostActiveAuthorsTest
from .test_06_trending_recipes_30d import TrendingRecipesLast30DaysTest
from .test_07_vegetarian_no_bacon import VegetarianNoBaconTest
from .test_08_top_cuisines_by_favourites import TopCuisinesByFavouritesTest
from .test_09_longest_recipes_by_steps import LongestRecipesByStepsTest
from .test_10_avg_calories_by_meal_type import AvgCaloriesByMealTypeTest

all_read_tests = [
    AverageRatingByCuisineTest,
    TopHealthyPopularRecipesTest,
    MostCommonIngredientsTest,
    TopRecipesPerCuisineTest,
    MostActiveAuthorsTest,
    TrendingRecipesLast30DaysTest,
    VegetarianNoBaconTest,
    TopCuisinesByFavouritesTest,
    LongestRecipesByStepsTest,
    AvgCaloriesByMealTypeTest
]

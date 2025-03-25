import csv
from benchmarks.tests.test_01_basic_read import BasicReadTest
from benchmarks.tests.test_02_rating_by_cuisine import AverageRatingByCuisineTest
from benchmarks.tests.test_03_top_healthy_popular_recipes import TopHealthyPopularRecipesTest

def run_all_tests():
    results = [
        BasicReadTest().run(),
        AverageRatingByCuisineTest().run(),
        TopHealthyPopularRecipesTest().run()
    ]

    with open("results/benchmark_results.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["test", "mysql", "postgres", "mongo_latest", "mongo_old"])
        writer.writeheader()
        writer.writerows(results)

if __name__ == "__main__":
    run_all_tests()

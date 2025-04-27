import csv
from database_scripts.record_counter import get_record_counts, round_records
from benchmarks.tests.test_01_basic_read import BasicReadTest
from benchmarks.tests.test_02_rating_by_cuisine import AverageRatingByCuisineTest
from benchmarks.tests.test_03_top_healthy_popular_recipes import TopHealthyPopularRecipesTest
from benchmarks.tests.test_04_highly_rated_unliked_recipes import HighlyRatedUnlikedRecipesTest

def run_all_tests():
    records = get_record_counts().get("MySQL", None)
    if not records:
        raise Exception('No records found')
    records = round_records(records)

    results = [
        BasicReadTest().run(),
        AverageRatingByCuisineTest().run(),
        TopHealthyPopularRecipesTest().run(),
        HighlyRatedUnlikedRecipesTest().run()
    ]

    with open(f"benchmarks/results/benchmark_results_{records}_records.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["test", "mysql", "postgres", "mongo_latest", "mongo_old"])
        writer.writeheader()
        writer.writerows(results)

if __name__ == "__main__":
    run_all_tests()

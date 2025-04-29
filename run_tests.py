import csv
import pathlib
from collections import defaultdict
from statistics import mean

from benchmarks.tests import all_tests
from generate_and_import import generate_and_import

DATA_SIZES = [10_000, 100_000, 1_000_000, 10_000_000]
REPEATS = 3
ENGINES = ["mysql", "postgres", "mongo_latest", "mongo_old"]

RESULT_DIR = pathlib.Path("benchmarks/results")
RESULT_DIR.mkdir(parents=True, exist_ok=True)


def _avg(values):
    """Zwraca średnią z wartości numerycznych; jeżeli żadnej nie ma —
    zwraca pierwszy nienumeryczny wpis (ERROR / NOT_IMPLEMENTED)."""
    nums = [v for v in values if isinstance(v, (int, float))]
    if nums:
        return round(mean(nums), 4)
    return values[0] if values else ""


def write_csv(path: pathlib.Path, rows, header):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header, delimiter=";")
        w.writeheader()
        for row in rows:
            fixed = {k: (str(v).replace(".", ",") if isinstance(v, float) else v)
                     for k, v in row.items()}
            w.writerow(fixed)


summary_rows = []

for size in DATA_SIZES:
    print(f"\n🚀 DATASET {size:,} ------------------------------------------------")
    generate_and_import(size)

    by_cat = defaultdict(list)  # READ / CREATE / …
    for cls in all_tests:
        inst_dummy = cls()
        cat = getattr(inst_dummy, "operation", "READ").upper()

        runs = [cls().run() for _ in range(REPEATS)]

        avg_res = {"category": cat, "test": runs[0]["test"]}
        for eng in ENGINES:
            avg_res[eng] = _avg([r.get(eng, "") for r in runs])

        by_cat[cat].append(avg_res)

        for eng in ENGINES:
            summary_rows.append({
                "records": size,
                "category": cat,
                "test": avg_res["test"],
                "engine": eng,
                "time": avg_res[eng]
            })

    batch_rows = []
    for cat in ("READ", "CREATE", "UPDATE", "DELETE"):
        if cat in by_cat:
            batch_rows.extend(by_cat[cat])
            batch_rows.append({})  # pusty separator w CSV

    out = RESULT_DIR / f"benchmark_results_{size}_records.csv"
    write_csv(out, batch_rows,
              ["category", "test", *ENGINES])
    print("   → zapisano", out)

summary_path = RESULT_DIR / "summary_all.csv"
write_csv(summary_path, summary_rows,
          ["records", "category", "test", "engine", "time"])
print("\n✅  Podsumowanie →", summary_path)

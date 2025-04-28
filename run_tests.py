# run_tests.py  ──────────────────────────────────────────────────────────────
import csv
import pathlib
from collections import defaultdict

from benchmarks.tests import all_tests
from generate_and_import import generate_and_import

DATA_SIZES = [10_000, 100_000, 1_000_000, 10_000_000]
RESULT_DIR = pathlib.Path("benchmarks/results")
RESULT_DIR.mkdir(parents=True, exist_ok=True)


def write_csv(path, rows, header):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header, delimiter=';')
        w.writeheader()
        for row in rows:
            fixed = {}
            for k, v in row.items():
                if isinstance(v, float):
                    fixed[k] = str(v).replace(".", ",")
                else:
                    fixed[k] = v
            w.writerow(fixed)

summary_rows = []

for size in DATA_SIZES:
    print(f"\n🚀 DATASET {size:,} ------------------------------------------------")
    generate_and_import(size)

    # ── grupuj testy po kategorii (operation)
    by_cat = defaultdict(list)
    for cls in all_tests:
        inst = cls()
        cat = getattr(inst, "operation", "READ").upper()
        res = inst.run()
        res["category"] = cat
        by_cat[cat].append(res)

        # do mega-pliku
        for eng in ["mysql", "postgres", "mongo_latest", "mongo_old"]:
            summary_rows.append({
                "records": size,
                "category": cat,
                "test": res["test"],
                "engine": eng,
                "time": res.get(eng, "")
            })

    # ── zapis pliku dla tej paczki (z odstępami)
    batch_rows = []
    for cat in ("READ", "CREATE", "UPDATE", "DELETE"):
        if cat in by_cat:
            batch_rows.extend(by_cat[cat])
            batch_rows.append({})  # pusty wiersz jako separator

    out = RESULT_DIR / f"benchmark_results_{size}_records.csv"
    write_csv(out, batch_rows,
              ["category", "test", "mysql", "postgres", "mongo_latest", "mongo_old"])
    print("   → zapisano", out)

# ── summary zbiorczy (łatwy pivot w Excelu)
summary_path = RESULT_DIR / "summary_all.csv"
write_csv(summary_path, summary_rows,
          ["records", "category", "test", "engine", "time"])
print("\n✅  Podsumowanie →", summary_path)

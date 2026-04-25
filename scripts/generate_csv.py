#!/usr/bin/env python3
"""
generate_csv.py — Generate benchmark CSV files for the Conduit ETL demos.

Usage (from the repo root):
    python scripts/generate_csv.py

Output files are written to Conduit/data/ and are excluded from git.
Generate them once before running the Conduit benchmark.

  employees_1m.csv    —     1,000,000 rows
  employees_10m.csv   —    10,000,000 rows
  employees_50m.csv   —    50,000,000 rows
  employees_100m.csv  —   100,000,000 rows

Schema: Id, Name, Department, Salary
"""

import csv
import os
import random
import time

DEPARTMENTS = ["Engineering", "Marketing", "HR", "Finance", "Sales"]
SALARY_RANGES = {
    "Engineering": (70_000, 150_000),
    "Marketing":   (50_000,  95_000),
    "HR":          (45_000,  80_000),
    "Finance":     (60_000, 120_000),
    "Sales":       (40_000, 100_000),
}
FIRST_NAMES = ["Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Henry"]
LAST_NAMES  = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]

CONFIGS = [
    (1_000_000,    "employees_1m.csv"),
    (10_000_000,   "employees_10m.csv"),
    (50_000_000,   "employees_50m.csv"),
    (100_000_000,  "employees_100m.csv"),
]

# Resolve output dir relative to this script's location
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Conduit", "data")


def generate(path: str, count: int) -> None:
    rng = random.Random(42)
    t0 = time.time()

    size_label = f"{count:,}"
    print(f"  Writing {size_label} rows → {os.path.basename(path)} ...", end="", flush=True)

    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Id", "Name", "Department", "Salary"])

        for i in range(1, count + 1):
            dept = DEPARTMENTS[i % len(DEPARTMENTS)]
            lo, hi = SALARY_RANGES[dept]
            name = (
                f"{FIRST_NAMES[(i * 7) % len(FIRST_NAMES)]} "
                f"{LAST_NAMES[(i * 13) % len(LAST_NAMES)]}"
            )
            writer.writerow([i, name, dept, rng.randint(lo, hi)])

    elapsed = time.time() - t0
    size_mb = os.path.getsize(path) / 1_048_576
    print(f" done  ({elapsed:.1f}s, {size_mb:.0f} MB)")


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"Output directory: {os.path.abspath(OUT_DIR)}\n")

    total_start = time.time()
    for count, filename in CONFIGS:
        generate(os.path.join(OUT_DIR, filename), count)

    total_elapsed = time.time() - total_start
    print(f"\nAll files generated in {total_elapsed:.1f}s.")


if __name__ == "__main__":
    main()

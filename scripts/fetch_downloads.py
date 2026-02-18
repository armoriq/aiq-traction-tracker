#!/usr/bin/env python3
"""Fetch daily download counts from PyPI and npm and append to CSV."""

import csv
import os
import sys
from datetime import date, timedelta

import requests
import yaml

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(ROOT_DIR, "config.yaml")
DATA_DIR = os.path.join(ROOT_DIR, "data")
CSV_PATH = os.path.join(DATA_DIR, "downloads.csv")

CSV_HEADERS = ["date", "package", "source", "downloads"]


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def load_existing_entries():
    """Return a set of (date, package, source) tuples already recorded."""
    entries = set()
    if not os.path.exists(CSV_PATH):
        return entries
    with open(CSV_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            entries.add((row["date"], row["package"], row["source"]))
    return entries


def fetch_pypi_downloads(package):
    """Fetch all available daily download counts from pypistats.org."""
    url = f"https://pypistats.org/api/packages/{package}/overall"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  [ERROR] PyPI fetch failed for {package}: {e}")
        return []

    results = []
    for entry in data.get("data", []):
        if entry.get("category") == "with_mirrors":
            results.append({"date": entry["date"], "downloads": entry["downloads"]})

    if not results:
        print(f"  [WARN] No download data found for PyPI package: {package}")
    return results


def fetch_npm_downloads(package):
    """Fetch last 30 days of daily download counts from npm registry."""
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=364)
    url = f"https://api.npmjs.org/downloads/range/{start.isoformat()}:{end.isoformat()}/{package}"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  [ERROR] npm fetch failed for {package}: {e}")
        return []

    results = []
    for entry in data.get("downloads", []):
        results.append({"date": entry["day"], "downloads": entry["downloads"]})

    if not results:
        print(f"  [WARN] No download data found for npm package: {package}")
    return results


def append_rows(rows):
    """Append rows to the CSV file, creating it with headers if needed."""
    os.makedirs(DATA_DIR, exist_ok=True)
    file_exists = os.path.exists(CSV_PATH)
    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def main():
    config = load_config()
    existing = load_existing_entries()
    new_rows = []

    pypi_packages = config.get("pypi", []) or []
    npm_packages = config.get("npm", []) or []

    print(f"Tracking {len(pypi_packages)} PyPI and {len(npm_packages)} npm packages")

    for pkg in pypi_packages:
        print(f"Fetching PyPI: {pkg}")
        results = fetch_pypi_downloads(pkg)
        added = 0
        for result in results:
            if (result["date"], pkg, "pypi") not in existing:
                new_rows.append({
                    "date": result["date"],
                    "package": pkg,
                    "source": "pypi",
                    "downloads": result["downloads"],
                })
                added += 1
        print(f"  -> {len(results)} data points fetched, {added} new")

    for pkg in npm_packages:
        print(f"Fetching npm: {pkg}")
        results = fetch_npm_downloads(pkg)
        added = 0
        for result in results:
            if (result["date"], pkg, "npm") not in existing:
                new_rows.append({
                    "date": result["date"],
                    "package": pkg,
                    "source": "npm",
                    "downloads": result["downloads"],
                })
                added += 1
        print(f"  -> {len(results)} data points fetched, {added} new")

    if new_rows:
        append_rows(new_rows)
        print(f"\nAppended {len(new_rows)} new entries to {CSV_PATH}")
    else:
        print("\nNo new data to append.")

    return 0


if __name__ == "__main__":
    sys.exit(main())

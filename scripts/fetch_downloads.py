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
    """Fetch yesterday's download count from pypistats.org."""
    url = f"https://pypistats.org/api/packages/{package}/overall"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  [ERROR] PyPI fetch failed for {package}: {e}")
        return None

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    for entry in data.get("data", []):
        if entry.get("date") == yesterday and entry.get("category") == "with_mirrors":
            return {"date": yesterday, "downloads": entry["downloads"]}

    # Fallback: use the most recent date with with_mirrors data
    with_mirrors = [
        e for e in data.get("data", []) if e.get("category") == "with_mirrors"
    ]
    if with_mirrors:
        latest = max(with_mirrors, key=lambda e: e["date"])
        return {"date": latest["date"], "downloads": latest["downloads"]}

    print(f"  [WARN] No download data found for PyPI package: {package}")
    return None


def fetch_npm_downloads(package):
    """Fetch yesterday's download count from npm registry."""
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    url = f"https://api.npmjs.org/downloads/point/{yesterday}:{yesterday}/{package}"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  [ERROR] npm fetch failed for {package}: {e}")
        return None

    downloads = data.get("downloads")
    if downloads is not None:
        return {"date": yesterday, "downloads": downloads}

    print(f"  [WARN] No download data found for npm package: {package}")
    return None


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
        result = fetch_pypi_downloads(pkg)
        if result and (result["date"], pkg, "pypi") not in existing:
            new_rows.append({
                "date": result["date"],
                "package": pkg,
                "source": "pypi",
                "downloads": result["downloads"],
            })
            print(f"  -> {result['downloads']:,} downloads on {result['date']}")
        elif result:
            print(f"  -> Already recorded for {result['date']}, skipping")

    for pkg in npm_packages:
        print(f"Fetching npm: {pkg}")
        result = fetch_npm_downloads(pkg)
        if result and (result["date"], pkg, "npm") not in existing:
            new_rows.append({
                "date": result["date"],
                "package": pkg,
                "source": "npm",
                "downloads": result["downloads"],
            })
            print(f"  -> {result['downloads']:,} downloads on {result['date']}")
        elif result:
            print(f"  -> Already recorded for {result['date']}, skipping")

    if new_rows:
        append_rows(new_rows)
        print(f"\nAppended {len(new_rows)} new entries to {CSV_PATH}")
    else:
        print("\nNo new data to append.")

    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Generate trend plots from download data."""

import csv
import os
import sys
from collections import defaultdict
from datetime import date, timedelta

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(ROOT_DIR, "data", "downloads.csv")
PLOTS_DIR = os.path.join(ROOT_DIR, "plots")

# (label, days or None for all-time)
TIME_WINDOWS = [
    ("7d", "Last 7 Days", 7),
    ("14d", "Last 14 Days", 14),
    ("30d", "Last 30 Days", 30),
    ("365d", "Last 365 Days", 365),
    ("all", "All Time", None),
]


def load_data():
    """Load CSV into a dict: {(package, source): [(date, downloads), ...]}."""
    series = defaultdict(list)
    if not os.path.exists(CSV_PATH):
        return series
    with open(CSV_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["package"], row["source"])
            d = date.fromisoformat(row["date"])
            downloads = int(row["downloads"])
            series[key].append((d, downloads))
    # Sort each series by date
    for key in series:
        series[key].sort(key=lambda x: x[0])
    return series


def filter_by_window(series, days):
    """Filter series to only include data within the last N days."""
    if days is None:
        return series
    cutoff = date.today() - timedelta(days=days)
    filtered = {}
    for key, points in series.items():
        pts = [(d, dl) for d, dl in points if d >= cutoff]
        if pts:
            filtered[key] = pts
    return filtered


def generate_plot(series, window_label, window_name, days):
    """Generate a single plot for a time window."""
    filtered = filter_by_window(series, days)
    if not filtered:
        print(f"  No data for {window_name}, skipping")
        return

    # Separate by source
    pypi_series = {k: v for k, v in filtered.items() if k[1] == "pypi"}
    npm_series = {k: v for k, v in filtered.items() if k[1] == "npm"}

    has_pypi = bool(pypi_series)
    has_npm = bool(npm_series)
    num_plots = has_pypi + has_npm

    if num_plots == 0:
        return

    fig, axes = plt.subplots(1, num_plots, figsize=(7 * num_plots, 5), squeeze=False)
    fig.suptitle(f"Package Downloads — {window_name}", fontsize=14, fontweight="bold")

    ax_idx = 0

    if has_pypi:
        ax = axes[0][ax_idx]
        ax_idx += 1
        for (pkg, _), points in sorted(pypi_series.items()):
            dates = [p[0] for p in points]
            downloads = [p[1] for p in points]
            ax.plot(dates, downloads, marker="o", markersize=3, linewidth=1.5, label=pkg)
        ax.set_title("PyPI")
        ax.set_xlabel("Date")
        ax.set_ylabel("Downloads")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.tick_params(axis="x", rotation=45)

    if has_npm:
        ax = axes[0][ax_idx]
        for (pkg, _), points in sorted(npm_series.items()):
            dates = [p[0] for p in points]
            downloads = [p[1] for p in points]
            ax.plot(dates, downloads, marker="o", markersize=3, linewidth=1.5, label=pkg)
        ax.set_title("npm")
        ax.set_xlabel("Date")
        ax.set_ylabel("Downloads")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.tick_params(axis="x", rotation=45)

    plt.tight_layout()
    os.makedirs(PLOTS_DIR, exist_ok=True)
    path = os.path.join(PLOTS_DIR, f"downloads_{window_label}.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {path}")


def update_readme(series):
    """Regenerate README.md with current plots and package table."""
    readme_path = os.path.join(ROOT_DIR, "README.md")

    # Build package table
    packages = sorted(set(series.keys()))
    table_rows = []
    for pkg, source in packages:
        points = series[(pkg, source)]
        latest_date, latest_dl = points[-1] if points else ("—", "—")
        table_rows.append(f"| {pkg} | {source} | {latest_dl:,} | {latest_date} |")

    table = (
        "| Package | Source | Latest Downloads | Date |\n"
        "|---------|--------|-----------------|------|\n"
        + "\n".join(table_rows)
    )

    # Build plot sections
    plot_sections = []
    for label, name, _ in TIME_WINDOWS:
        plot_path = os.path.join(PLOTS_DIR, f"downloads_{label}.png")
        if os.path.exists(plot_path):
            plot_sections.append(f"### {name}\n\n![Downloads — {name}](plots/downloads_{label}.png)")

    today = date.today().isoformat()

    readme = f"""# Package Downloads Dashboard

Automated daily tracking of package download counts from PyPI and npm.

**Last updated:** {today}

## Tracked Packages

{table}

## Download Trends

{"".join(chr(10) + s + chr(10) for s in plot_sections)}

---

*Updated daily by [GitHub Actions](.github/workflows/update.yml). Edit [config.yaml](config.yaml) to add or remove packages.*
"""

    with open(readme_path, "w") as f:
        f.write(readme)
    print(f"  Updated {readme_path}")


def main():
    print("Loading download data...")
    series = load_data()
    if not series:
        print("No data found. Run fetch_downloads.py first.")
        return 0

    print(f"Found data for {len(series)} package(s)")

    print("Generating plots...")
    for label, name, days in TIME_WINDOWS:
        generate_plot(series, label, name, days)

    print("Updating README...")
    update_readme(series)

    print("Done!")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Generate trend plots from traction data."""

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

SOURCE_LABELS = {
    "pypi": "PyPI",
    "npm": "npm",
    "github_stars": "GitHub Stars",
    "github_forks": "GitHub Forks",
    "github_open_issues": "GitHub Open Issues",
    "discord_members": "Discord Members",
    "discord_messages": "Discord Messages",
}

# Sources that represent point-in-time snapshots rather than daily increments.
SNAPSHOT_SOURCES = {"github_stars", "github_forks", "github_open_issues", "discord_members"}


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

    grouped_by_source = defaultdict(dict)
    for (pkg, source), points in filtered.items():
        grouped_by_source[source][(pkg, source)] = points

    ordered_sources = []
    for source in ["pypi", "npm", "github_stars", "github_forks", "github_open_issues", "discord_members", "discord_messages"]:
        if source in grouped_by_source:
            ordered_sources.append(source)
    for source in sorted(grouped_by_source.keys()):
        if source not in ordered_sources:
            ordered_sources.append(source)

    num_plots = len(ordered_sources)

    if num_plots == 0:
        return

    ncols = min(2, num_plots)
    nrows = (num_plots + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(7 * ncols, 5 * nrows), squeeze=False)
    fig.suptitle(f"Traction Metrics — {window_name}", fontsize=14, fontweight="bold")

    # Hide unused axes
    for i in range(num_plots, nrows * ncols):
        axes[i // ncols][i % ncols].set_visible(False)

    for ax_idx, source in enumerate(ordered_sources):
        ax = axes[ax_idx // ncols][ax_idx % ncols]
        source_series = grouped_by_source[source]
        for (pkg, _), points in sorted(source_series.items()):
            dates = [p[0] for p in points]
            values = [p[1] for p in points]
            ax.plot(dates, values, marker="o", markersize=3, linewidth=1.5, label=pkg)
        ax.set_title(SOURCE_LABELS.get(source, source))
        ax.set_xlabel("Date")
        ax.set_ylabel("Value")
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
        if source in SNAPSHOT_SOURCES:
            metric = "Latest Value"
            value = points[-1][1]
        else:
            metric = "Total Downloads"
            value = sum(dl for _, dl in points)
        table_rows.append(
            f"| {pkg} | {SOURCE_LABELS.get(source, source)} | {metric} | {value:,} |"
        )

    table = (
        "| Item | Source | Metric | Value |\n"
        "|------|--------|--------|-------|\n"
        + "\n".join(table_rows)
    )

    # Build plot sections
    plot_sections = []
    for label, name, _ in TIME_WINDOWS:
        plot_path = os.path.join(PLOTS_DIR, f"downloads_{label}.png")
        if os.path.exists(plot_path):
            plot_sections.append(f"### {name}\n\n![Downloads — {name}](plots/downloads_{label}.png)")

    today = date.today().isoformat()

    readme = f"""# Traction Dashboard

Automated daily tracking of package and repository traction metrics from PyPI, npm, and GitHub.

**Last updated:** {today}

## Tracked Items

{table}

## Metric Trends

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

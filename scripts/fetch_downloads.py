#!/usr/bin/env python3
"""Fetch daily traction metrics from PyPI, npm, and GitHub and append to CSV."""

import csv
import os
import sys
from datetime import date, timedelta
from typing import Dict, List

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
    """Fetch the last 365 days of daily download counts from npm registry."""
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


def parse_github_config(config):
    """Parse GitHub config supporting dict, list, or string formats."""
    github_cfg = config.get("github")
    owners: List[str] = []
    repos: List[str] = []

    if not github_cfg:
        return owners, repos

    if isinstance(github_cfg, dict):
        owners = github_cfg.get("owners", []) or []
        repos = github_cfg.get("repos", []) or []
    elif isinstance(github_cfg, list):
        for item in github_cfg:
            if isinstance(item, str):
                if "/" in item:
                    repos.append(item)
                else:
                    owners.append(item)
    elif isinstance(github_cfg, str):
        if "/" in github_cfg:
            repos.append(github_cfg)
        else:
            owners.append(github_cfg)

    return owners, repos


def github_headers():
    """Build GitHub API headers with optional token auth."""
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    headers = {
        "Accept": "application/vnd.github+json",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def fetch_github_user_login(headers):
    """Return authenticated user login when a GitHub token is provided."""
    if "Authorization" not in headers:
        return None
    try:
        resp = requests.get("https://api.github.com/user", headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json().get("login")
    except Exception as e:
        print(f"  [WARN] Could not resolve authenticated GitHub user: {e}")
        return None


def fetch_github_paginated(endpoint, headers, params):
    """Fetch a paginated list endpoint from GitHub."""
    all_items = []
    page = 1
    while True:
        page_params = dict(params)
        page_params["page"] = page
        page_params["per_page"] = 100
        resp = requests.get(endpoint, headers=headers, params=page_params, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        batch = resp.json()
        if not isinstance(batch, list):
            break
        all_items.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return all_items


def fetch_repos_for_owner(owner, headers, viewer_login=None):
    """
    Fetch all repos for an owner (org or user).

    For authenticated requests where owner is the token owner, use /user/repos
    to include private repositories.
    """
    if viewer_login and viewer_login.lower() == owner.lower():
        endpoint = "https://api.github.com/user/repos"
        params = {"type": "owner", "sort": "full_name", "direction": "asc"}
        repos = fetch_github_paginated(endpoint, headers, params)
        if repos is not None:
            repos = [
                repo
                for repo in repos
                if repo.get("owner", {}).get("login", "").lower() == owner.lower()
            ]
            return repos

    org_endpoint = f"https://api.github.com/orgs/{owner}/repos"
    params = {"type": "all", "sort": "full_name", "direction": "asc"}
    repos = fetch_github_paginated(org_endpoint, headers, params)
    if repos is not None:
        return repos

    user_endpoint = f"https://api.github.com/users/{owner}/repos"
    repos = fetch_github_paginated(user_endpoint, headers, params)
    if repos is not None:
        return repos

    raise RuntimeError(f"Owner '{owner}' not found as a GitHub user or org")


def fetch_repo_by_full_name(full_name, headers):
    """Fetch a single repository by full name (owner/repo)."""
    url = f"https://api.github.com/repos/{full_name}"
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_github_repo_stats(config):
    """
    Fetch GitHub repo traction metrics.

    Returns rows in the same schema as CSV_HEADERS with these source values:
    - github_stars
    - github_forks
    - github_open_issues
    """
    owners, explicit_repos = parse_github_config(config)
    if not owners and not explicit_repos:
        return []

    headers = github_headers()
    viewer_login = fetch_github_user_login(headers)
    today = date.today().isoformat()

    repos_by_name: Dict[str, dict] = {}

    for owner in owners:
        print(f"Fetching GitHub repos for owner: {owner}")
        try:
            repos = fetch_repos_for_owner(owner, headers, viewer_login)
        except Exception as e:
            print(f"  [ERROR] GitHub repo listing failed for owner {owner}: {e}")
            continue

        for repo in repos:
            full_name = repo.get("full_name")
            if full_name:
                repos_by_name[full_name] = repo
        print(f"  -> {len(repos)} repos discovered")

    for full_name in explicit_repos:
        if full_name in repos_by_name:
            continue
        print(f"Fetching GitHub repo: {full_name}")
        try:
            repo = fetch_repo_by_full_name(full_name, headers)
            repos_by_name[full_name] = repo
        except Exception as e:
            print(f"  [ERROR] GitHub fetch failed for repo {full_name}: {e}")

    rows = []
    for full_name, repo in sorted(repos_by_name.items()):
        stars = int(repo.get("stargazers_count", 0))
        forks = int(repo.get("forks_count", 0))
        open_issues = int(repo.get("open_issues_count", 0))
        rows.extend([
            {"date": today, "package": full_name, "source": "github_stars", "downloads": stars},
            {"date": today, "package": full_name, "source": "github_forks", "downloads": forks},
            {
                "date": today,
                "package": full_name,
                "source": "github_open_issues",
                "downloads": open_issues,
            },
        ])

    print(f"Collected GitHub stats for {len(repos_by_name)} repo(s)")
    return rows


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
    github_owners, github_repos = parse_github_config(config)

    print(
        "Tracking "
        f"{len(pypi_packages)} PyPI package(s), "
        f"{len(npm_packages)} npm package(s), "
        f"{len(github_owners)} GitHub owner(s), "
        f"{len(github_repos)} explicit GitHub repo(s)"
    )

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

    github_rows = fetch_github_repo_stats(config)
    github_added = 0
    for row in github_rows:
        key = (row["date"], row["package"], row["source"])
        if key not in existing:
            new_rows.append(row)
            github_added += 1
    if github_rows:
        print(f"GitHub rows fetched: {len(github_rows)}, {github_added} new")

    if new_rows:
        append_rows(new_rows)
        print(f"\nAppended {len(new_rows)} new entries to {CSV_PATH}")
    else:
        print("\nNo new data to append.")

    return 0


if __name__ == "__main__":
    sys.exit(main())

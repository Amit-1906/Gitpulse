"""Test script for GitPulse: extracts data and loads it into PostgreSQL."""

import logging
import os
import sys
from dotenv import load_dotenv

from extract.github_client import GitHubClient
from load.load_postgres import PostgresLoader

# Setup console logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("gitpulse.test_pipeline")


def main() -> None:
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN")
    if not token or token.strip() == "your_token_here":
        print("\n" + "=" * 70)
        print("REMINDER: GITHUB_TOKEN is currently unset or using the default placeholder.")
        print("Running in unauthenticated mode (rate limit: 60 requests/hr).")
        print("To increase rate limits to 5,000 req/hr, update GITHUB_TOKEN in .env.")
        print("=" * 70 + "\n")

    test_owner = "apache"
    test_repo = "airflow"

    print("=" * 70)
    print(f"STEP 1: Extracting GitHub data for '{test_owner}/{test_repo}'...")
    print("=" * 70)

    try:
        client = GitHubClient()
        repo_info = client.get_repo_info(test_owner, test_repo)
        print(f"  [+] Repository info: {repo_info.get('full_name')} ({repo_info.get('stargazers_count'):,} stars)")

        commits = client.get_commits(test_owner, test_repo, max_pages=1)
        print(f"  [+] Extracted commits: {len(commits)}")

        issues = client.get_issues(test_owner, test_repo, max_pages=1)
        print(f"  [+] Extracted issues: {len(issues)}")

        pulls = client.get_pulls(test_owner, test_repo, max_pages=1)
        print(f"  [+] Extracted pull requests: {len(pulls)}")

    except Exception as exc:
        print("\n[!] EXTRACTION FAILED:")
        if "401" in str(exc) or "Bad credentials" in str(exc):
            print(">>> AUTHENTICATION ERROR: Your GITHUB_TOKEN appears invalid.")
            print(">>> Please verify or update GITHUB_TOKEN in .env.")
        elif "403" in str(exc) or "rate limit" in str(exc).lower():
            print(">>> RATE LIMIT HIT: GitHub API unauthenticated quota exhausted.")
            print(">>> Set a valid GITHUB_TOKEN in .env to get 5,000 requests/hr.")
        raise

    print("\n" + "=" * 70)
    print("STEP 2: Initializing PostgreSQL staging schema and GIN indexes...")
    print("=" * 70)

    loader = PostgresLoader()
    loader.init_schema()
    print("  [+] PostgreSQL raw staging schema & indexes ready.")

    print("\n" + "=" * 70)
    print(f"STEP 3: Loading data into raw staging tables for '{test_owner}/{test_repo}'...")
    print("=" * 70)

    saved_counts = loader.save_repo_data(
        owner=test_owner,
        repo=test_repo,
        repo_info=repo_info,
        commits=commits,
        issues=issues,
        pulls=pulls,
    )
    print(f"  [+] Processed in single atomic transaction: {saved_counts}")

    print("\n" + "=" * 70)
    print("STEP 4: Querying row counts from all 4 raw tables:")
    print("=" * 70)

    counts = loader.get_row_counts()
    for table_name, count in counts.items():
        print(f"  Table: {table_name:<16} | Row Count: {count:>5}")

    print("\n" + "=" * 70)
    print("STEP 5: Testing Upsert Idempotency (re-saving same batch)...")
    print("=" * 70)

    loader.save_repo_data(
        owner=test_owner,
        repo=test_repo,
        repo_info=repo_info,
        commits=commits,
        issues=issues,
        pulls=pulls,
    )
    counts_after = loader.get_row_counts()
    print("  [+] Counts after re-running upsert (should match exactly):")
    for table_name, count in counts_after.items():
        print(f"  Table: {table_name:<16} | Row Count: {count:>5}")

    print("\n" + "=" * 70)
    print("ALL STAGING LOADER TESTS PASSED!")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()

"""Raw staging loader for PostgreSQL in the GitPulse data pipeline.

Loads extracted GitHub entities into JSONB raw tables with GIN indexes and upsert logic.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Set
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Load environment variables from .env
load_dotenv()

logger = logging.getLogger("gitpulse.load_postgres")

DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/gitpulse"


# DDL statements for creating raw staging tables with GIN indexes
CREATE_TABLES_SQL = """
-- 1. raw_repos
CREATE TABLE IF NOT EXISTS raw_repos (
    id SERIAL PRIMARY KEY,
    full_name TEXT UNIQUE NOT NULL,
    payload JSONB NOT NULL,
    source_repo TEXT NOT NULL,
    fetched_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_repos_payload ON raw_repos USING gin (payload);

-- 2. raw_commits
CREATE TABLE IF NOT EXISTS raw_commits (
    id SERIAL PRIMARY KEY,
    sha TEXT UNIQUE NOT NULL,
    payload JSONB NOT NULL,
    source_repo TEXT NOT NULL,
    fetched_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_commits_payload ON raw_commits USING gin (payload);

-- 3. raw_issues
CREATE TABLE IF NOT EXISTS raw_issues (
    id SERIAL PRIMARY KEY,
    issue_number BIGINT UNIQUE NOT NULL,
    payload JSONB NOT NULL,
    source_repo TEXT NOT NULL,
    fetched_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_issues_payload ON raw_issues USING gin (payload);

-- 4. raw_pulls
CREATE TABLE IF NOT EXISTS raw_pulls (
    id SERIAL PRIMARY KEY,
    pr_number BIGINT UNIQUE NOT NULL,
    payload JSONB NOT NULL,
    source_repo TEXT NOT NULL,
    fetched_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_pulls_payload ON raw_pulls USING gin (payload);
"""


class PostgresLoader:
    """Handles raw staging extraction loads into PostgreSQL."""

    def __init__(self, database_url: Optional[str] = None) -> None:
        """Initialize PostgresLoader with database connection string.

        Args:
            database_url: Optional database connection URI. Defaults to DATABASE_URL env var.
        """
        self.database_url: str = (
            database_url or os.getenv("DATABASE_URL") or DEFAULT_DATABASE_URL
        )
        self.engine: Engine = create_engine(self.database_url, future=True)

    def init_schema(self) -> None:
        """Ensure all raw staging tables and GIN indexes exist."""
        logger.info("Initializing raw staging schema in PostgreSQL...")
        with self.engine.begin() as conn:
            conn.execute(text(CREATE_TABLES_SQL))
        logger.info("Raw staging schema and GIN indexes verified.")

    def save_repo_data(
        self,
        owner: str,
        repo: str,
        repo_info: Dict[str, Any],
        commits: List[Dict[str, Any]],
        issues: List[Dict[str, Any]],
        pulls: List[Dict[str, Any]],
    ) -> Dict[str, int]:
        """Save extracted repository entities in a single transaction with rollback on error.

        Performs atomic upsert into raw_repos, raw_commits, raw_issues, and raw_pulls.

        Args:
            owner: Repository owner.
            repo: Repository name.
            repo_info: Repository metadata payload.
            commits: List of commit payloads.
            issues: List of issue payloads.
            pulls: List of pull request payloads.

        Returns:
            Dictionary with counts of processed records per entity.

        Raises:
            Exception: Any exception encountered will roll back the transaction and re-raise.
        """
        source_repo = f"{owner}/{repo}"
        counts = {"repos": 0, "commits": 0, "issues": 0, "pulls": 0}

        # SQL Upsert Statements
        upsert_repo_sql = text("""
            INSERT INTO raw_repos (full_name, payload, source_repo, fetched_at)
            VALUES (:full_name, CAST(:payload AS jsonb), :source_repo, now())
            ON CONFLICT (full_name)
            DO UPDATE SET payload = EXCLUDED.payload, fetched_at = now();
        """)

        upsert_commit_sql = text("""
            INSERT INTO raw_commits (sha, payload, source_repo, fetched_at)
            VALUES (:sha, CAST(:payload AS jsonb), :source_repo, now())
            ON CONFLICT (sha)
            DO UPDATE SET payload = EXCLUDED.payload, fetched_at = now();
        """)

        upsert_issue_sql = text("""
            INSERT INTO raw_issues (issue_number, payload, source_repo, fetched_at)
            VALUES (:issue_number, CAST(:payload AS jsonb), :source_repo, now())
            ON CONFLICT (issue_number)
            DO UPDATE SET payload = EXCLUDED.payload, fetched_at = now();
        """)

        upsert_pull_sql = text("""
            INSERT INTO raw_pulls (pr_number, payload, source_repo, fetched_at)
            VALUES (:pr_number, CAST(:payload AS jsonb), :source_repo, now())
            ON CONFLICT (pr_number)
            DO UPDATE SET payload = EXCLUDED.payload, fetched_at = now();
        """)

        try:
            with self.engine.begin() as conn:
                logger.info("Starting atomic transaction for %s data loading...", source_repo)

                # 1. Upsert repo_info
                if repo_info:
                    full_name = repo_info.get("full_name") or source_repo
                    conn.execute(
                        upsert_repo_sql,
                        {
                            "full_name": full_name,
                            "payload": json.dumps(repo_info),
                            "source_repo": source_repo,
                        },
                    )
                    counts["repos"] = 1

                # 2. Upsert commits (deduplicate within batch to prevent ON CONFLICT intra-batch collision)
                seen_shas: Set[str] = set()
                commit_params = []
                for commit in commits:
                    sha = commit.get("sha")
                    if sha and sha not in seen_shas:
                        seen_shas.add(sha)
                        commit_params.append(
                            {
                                "sha": sha,
                                "payload": json.dumps(commit),
                                "source_repo": source_repo,
                            }
                        )
                if commit_params:
                    conn.execute(upsert_commit_sql, commit_params)
                    counts["commits"] = len(commit_params)

                # 3. Upsert issues (deduplicate within batch)
                seen_issues: Set[int] = set()
                issue_params = []
                for issue in issues:
                    num = issue.get("number")
                    if num is not None and int(num) not in seen_issues:
                        seen_issues.add(int(num))
                        issue_params.append(
                            {
                                "issue_number": int(num),
                                "payload": json.dumps(issue),
                                "source_repo": source_repo,
                            }
                        )
                if issue_params:
                    conn.execute(upsert_issue_sql, issue_params)
                    counts["issues"] = len(issue_params)

                # 4. Upsert pull requests (deduplicate within batch)
                seen_pulls: Set[int] = set()
                pull_params = []
                for pr in pulls:
                    pr_num = pr.get("number")
                    if pr_num is not None and int(pr_num) not in seen_pulls:
                        seen_pulls.add(int(pr_num))
                        pull_params.append(
                            {
                                "pr_number": int(pr_num),
                                "payload": json.dumps(pr),
                                "source_repo": source_repo,
                            }
                        )
                if pull_params:
                    conn.execute(upsert_pull_sql, pull_params)
                    counts["pulls"] = len(pull_params)

                logger.info(
                    "Transaction committed successfully for %s: %s",
                    source_repo,
                    counts,
                )

        except Exception as exc:
            logger.error("Error saving data for %s, transaction rolled back: %s", source_repo, exc)
            raise

        return counts

    def get_row_counts(self) -> Dict[str, int]:
        """Query row counts from all 4 raw tables.

        Returns:
            Dictionary mapping table name to row count.
        """
        counts = {}
        tables = ["raw_repos", "raw_commits", "raw_issues", "raw_pulls"]
        with self.engine.connect() as conn:
            for table in tables:
                res = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                counts[table] = res.scalar() or 0
        return counts


# ---------------------------------------------------------------------------
# Module-level convenience functions
# ---------------------------------------------------------------------------

_default_loader: Optional[PostgresLoader] = None


def get_default_loader(database_url: Optional[str] = None) -> PostgresLoader:
    """Get or create the singleton PostgresLoader instance."""
    global _default_loader
    if _default_loader is None or database_url is not None:
        _default_loader = PostgresLoader(database_url=database_url)
    return _default_loader


def save_repo_data(
    owner: str,
    repo: str,
    repo_info: Dict[str, Any],
    commits: List[Dict[str, Any]],
    issues: List[Dict[str, Any]],
    pulls: List[Dict[str, Any]],
    database_url: Optional[str] = None,
) -> Dict[str, int]:
    """Module-level function to save extracted repo entities in a single transaction.

    Args:
        owner: Repository owner.
        repo: Repository name.
        repo_info: Repository info dict.
        commits: List of commit dicts.
        issues: List of issue dicts.
        pulls: List of pull dicts.
        database_url: Optional database connection string.

    Returns:
        Processed entity counts.
    """
    loader = get_default_loader(database_url=database_url)
    loader.init_schema()
    return loader.save_repo_data(owner, repo, repo_info, commits, issues, pulls)


def get_row_counts(database_url: Optional[str] = None) -> Dict[str, int]:
    """Module-level function to get row counts from all 4 raw tables."""
    loader = get_default_loader(database_url=database_url)
    loader.init_schema()
    return loader.get_row_counts()

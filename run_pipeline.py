"""GitPulse: End-to-end data pipeline orchestrator.

Extracts data from GitHub REST API, loads raw staging records into PostgreSQL,
maintains incremental state tracking, and runs dbt transformations and data tests.
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

# Ensure gitpulse root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from extract.github_client import GitHubClient, get_last_sync, load_state, save_state
from load.load_postgres import PostgresLoader

# Load environment
load_dotenv(PROJECT_ROOT / ".env")

# 1. Pipeline Target Repositories
TARGET_REPOS: List[str] = [
    "apache/airflow",
    "apache/spark",
    "microsoft/vscode",
    "facebook/react",
    "pytorch/pytorch",
    "golang/go",
    "kubernetes/kubernetes",
    "rust-lang/rust",
    "tensorflow/tensorflow",
    "apache/kafka",
]

# Configure directories
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOGS_DIR / "pipeline.log"

# 5. Logging Configuration (Console + pipeline.log, INFO level)
logger = logging.getLogger("gitpulse.pipeline")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Avoid duplicate handlers on rerun
if not logger.handlers:
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def check_github_token() -> str:
    """Validate that GITHUB_TOKEN is properly set.

    Returns:
        The validated token string.

    Raises:
        ValueError: If token is unset, blank, or placeholder.
    """
    token = os.getenv("GITHUB_TOKEN")
    if not token or not token.strip() or token.strip() == "your_token_here":
        msg = (
            "\n" + "=" * 70 + "\n"
            "CRITICAL: GITHUB_TOKEN is not configured!\n"
            f"Please open: {PROJECT_ROOT / '.env'}\n"
            "and set your personal access token:\n"
            "GITHUB_TOKEN=ghp_your_actual_token_here\n"
            + "=" * 70
        )
        logger.error(msg)
        raise ValueError("GITHUB_TOKEN is missing or set to placeholder.")
    return token.strip()


def run_dbt() -> None:
    """Execute dbt build via subprocess and stream/capture output."""
    dbt_dir = PROJECT_ROOT / "dbt" / "gitpulse_dbt"
    logger.info("Executing dbt transformations from %s...", dbt_dir)

    # Resolve dbt binary
    venv_dbt = PROJECT_ROOT / "venv" / "Scripts" / "dbt.exe"
    dbt_cmd = str(venv_dbt) if venv_dbt.is_file() else "dbt"

    cmd = [
        dbt_cmd,
        "build",
        "--project-dir",
        str(dbt_dir),
        "--profiles-dir",
        str(dbt_dir),
    ]

    logger.info("Running dbt command: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.stdout:
        for line in result.stdout.strip().split("\n"):
            logger.info("[dbt] %s", line)

    if result.returncode != 0:
        if result.stderr:
            for line in result.stderr.strip().split("\n"):
                logger.error("[dbt ERROR] %s", line)
        raise RuntimeError(f"dbt build failed with exit code {result.returncode}")

    logger.info("dbt build completed successfully.")


def run_pipeline() -> None:
    """Execute full extraction, loading, state persistence, and dbt build."""
    start_time = time.time()
    logger.info("=" * 70)
    logger.info("Starting GitPulse Data Pipeline Execution")
    logger.info("Target Repositories (%d): %s", len(TARGET_REPOS), ", ".join(TARGET_REPOS))
    logger.info("=" * 70)

    # Check Token
    check_github_token()

    # Initialize Client and Database Loader
    client = GitHubClient()
    loader = PostgresLoader()
    loader.init_schema()

    current_state = load_state()
    pipeline_run_ts = datetime.now(timezone.utc).isoformat()
    repo_summaries: List[Dict[str, Any]] = []

    for index, full_name in enumerate(TARGET_REPOS, start=1):
        try:
            owner, repo = full_name.split("/")
        except ValueError:
            logger.error("Invalid repository format '%s'. Expected 'owner/repo'. Skipping.", full_name)
            continue

        repo_start = time.time()
        logger.info("-" * 70)
        logger.info("[%d/%d] Processing repository: %s/%s", index, len(TARGET_REPOS), owner, repo)

        # Retrieve last sync timestamps for incremental extraction
        last_commit_sync = get_last_sync(owner, repo, "commits", filepath=PROJECT_ROOT / "data" / "state.json")
        last_issue_sync = get_last_sync(owner, repo, "issues", filepath=PROJECT_ROOT / "data" / "state.json")
        last_pull_sync = get_last_sync(owner, repo, "pulls", filepath=PROJECT_ROOT / "data" / "state.json")

        logger.info(
            "Incremental sync watermarks for %s/%s: commits=%s, issues=%s, pulls=%s",
            owner,
            repo,
            last_commit_sync,
            last_issue_sync,
            last_pull_sync,
        )

        # 2. Flow per repo: get_repo_info → get_commits(since=state) → get_issues(since) → get_pulls(since) → save_repo_data
        repo_info = client.get_repo_info(owner, repo)
        commits = client.get_commits(owner, repo, since=last_commit_sync, max_pages=10)
        issues = client.get_issues(owner, repo, since=last_issue_sync, max_pages=10)
        pulls = client.get_pulls(owner, repo, since=last_pull_sync, max_pages=10)

        # Save to PostgreSQL in single atomic transaction
        counts = loader.save_repo_data(
            owner=owner,
            repo=repo,
            repo_info=repo_info,
            commits=commits,
            issues=issues,
            pulls=pulls,
        )

        repo_duration = time.time() - repo_start
        logger.info(
            "[%d/%d] Completed %s/%s in %.1fs: repo=1, commits=%d, issues=%d, pulls=%d",
            index,
            len(TARGET_REPOS),
            owner,
            repo,
            repo_duration,
            counts.get("commits", 0),
            counts.get("issues", 0),
            counts.get("pulls", 0),
        )

        repo_summaries.append(
            {
                "repo": full_name,
                "commits": counts.get("commits", 0),
                "issues": counts.get("issues", 0),
                "pulls": counts.get("pulls", 0),
                "duration_sec": repo_duration,
            }
        )

        # Update in-memory state tracking
        if full_name not in current_state:
            current_state[full_name] = {}
        current_state[full_name]["repo_info"] = pipeline_run_ts
        current_state[full_name]["commits"] = pipeline_run_ts
        current_state[full_name]["issues"] = pipeline_run_ts
        current_state[full_name]["pulls"] = pipeline_run_ts

    # 3. After all repos: save_state() with current UTC timestamp
    save_state(current_state, filepath=PROJECT_ROOT / "data" / "state.json")
    logger.info("Persisted updated extraction watermarks for all repositories.")

    # 4. Run dbt build
    run_dbt()

    # 5. Summary and Execution Metrics
    total_runtime = time.time() - start_time
    logger.info("=" * 70)
    logger.info("GITPULSE PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 70)
    logger.info("Total Pipeline Runtime: %.1f seconds (%.2f minutes)", total_runtime, total_runtime / 60.0)
    logger.info("Per-repository summary:")
    for summary in repo_summaries:
        logger.info(
            "  * %-25s | Commits: %5d | Issues: %5d | Pulls: %5d | Time: %5.1fs",
            summary["repo"],
            summary["commits"],
            summary["issues"],
            summary["pulls"],
            summary["duration_sec"],
        )
    logger.info("=" * 70)


# 6. Full try/except with traceback, sys.exit(1) on failure
if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as exc:
        logger.error("FATAL: Pipeline execution failed with error: %s", exc)
        traceback.print_exc()
        sys.exit(1)

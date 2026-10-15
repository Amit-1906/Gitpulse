"""Production-grade GitHub REST API client for GitPulse data pipeline.

Handles authentication, rate limit monitoring, exponential backoff retries,
pagination, and state persistence for incremental extraction.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional, Union

from dotenv import load_dotenv
import requests
from requests.exceptions import RequestException
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

# Load environment variables from .env if present
load_dotenv()

logger = logging.getLogger("gitpulse.github_client")

# Default path for persistent extraction state
DEFAULT_STATE_FILE: Path = (
    Path(__file__).resolve().parent.parent / "data" / "state.json"
)


class GitHubRateLimitError(requests.exceptions.HTTPError):
    """Raised when GitHub returns a rate limit response (403 rate-limited or 429)."""

    pass


def _is_rate_limit_403(response: requests.Response) -> bool:
    """Check if a 403 HTTP response is specifically caused by rate limiting.

    Args:
        response: The HTTP response from GitHub.

    Returns:
        True if the 403 response indicates rate limiting, False otherwise.
    """
    remaining = response.headers.get("X-RateLimit-Remaining")
    if remaining == "0":
        return True

    text_lower = response.text.lower()
    return (
        "rate limit" in text_lower
        or "secondary rate limit" in text_lower
        or "retry-after" in response.headers
    )


def is_retryable_exception(exc: BaseException) -> bool:
    """Tenacity retry predicate for GitHub REST API requests.

    Retries on:
    - 403 HTTP status code ONLY if caused by rate limits.
    - 429 HTTP status code (Too Many Requests / Secondary Rate Limits).
    - Network/connection RequestExceptions.
    Does NOT retry non-rate-limit client errors (400, 401, non-rate-limit 403, 404, 422).

    Args:
        exc: Exception raised during request execution.

    Returns:
        True if the request should be retried, False otherwise.
    """
    if isinstance(exc, GitHubRateLimitError):
        return True

    if isinstance(exc, requests.exceptions.HTTPError):
        resp: Optional[requests.Response] = getattr(exc, "response", None)
        if resp is not None:
            if resp.status_code == 429:
                return True
            if resp.status_code == 403:
                return _is_rate_limit_403(resp)
            # Do not retry other HTTP errors
            return False

    # Retry on connection loss, timeout, chunk errors, etc.
    return isinstance(exc, RequestException)


class GitHubClient:
    """Production-grade GitHub REST API client with tenacity retries and rate limit safety."""

    BASE_URL: str = "https://api.github.com"
    API_VERSION: str = "2022-11-28"

    def __init__(
        self,
        token: Optional[str] = None,
        base_url: Optional[str] = None,
        rate_limit_threshold: Optional[int] = None,
        rate_limit_safety_buffer: float = 5.0,
    ) -> None:
        """Initialize the GitHub client.

        Args:
            token: Optional GitHub Personal Access Token. If omitted, read from GITHUB_TOKEN.
            base_url: Base API URL (defaults to https://api.github.com).
            rate_limit_threshold: Remaining requests threshold to trigger sleep safety (defaults to 50 when authenticated, 2 when unauthenticated).
            rate_limit_safety_buffer: Extra seconds added to reset sleep.
        """
        self.base_url: str = (base_url or self.BASE_URL).rstrip("/")
        self.rate_limit_safety_buffer: float = rate_limit_safety_buffer

        # Resolve token
        resolved_token = token if token is not None else os.getenv("GITHUB_TOKEN")
        if resolved_token and resolved_token.strip() and resolved_token.strip() != "your_token_here":
            self.token: Optional[str] = resolved_token.strip()
        else:
            self.token = None

        if rate_limit_threshold is not None:
            self.rate_limit_threshold: int = rate_limit_threshold
        else:
            self.rate_limit_threshold = 50 if self.token else 2

        if not self.token:
            logger.warning(
                "No valid GITHUB_TOKEN detected. Running in unauthenticated mode "
                "(rate limit: 60 requests/hr). Safety threshold set to %d. "
                "Set GITHUB_TOKEN in .env for 5,000 req/hr with 50-request safety buffer.",
                self.rate_limit_threshold,
            )

        # Configure session and standard headers
        self.session: requests.Session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": self.API_VERSION,
                "User-Agent": "GitPulse-ETL-Pipeline",
            }
        )
        if self.token:
            self.session.headers["Authorization"] = f"Bearer {self.token}"

    def _check_rate_limit(self, response: requests.Response) -> None:
        """Inspect X-RateLimit headers and sleep if remaining quota is below safety threshold.

        Rule: Check X-RateLimit-Remaining; if < 50, sleep until X-RateLimit-Reset + 5s.

        Args:
            response: Response received from GitHub API.
        """
        remaining_str = response.headers.get("X-RateLimit-Remaining")
        reset_str = response.headers.get("X-RateLimit-Reset")

        if remaining_str is None:
            return

        try:
            remaining = int(remaining_str)
            if remaining < self.rate_limit_threshold:
                now = time.time()
                reset_epoch = float(reset_str) if reset_str else (now + 60.0)
                sleep_seconds = max(0.0, reset_epoch - now) + self.rate_limit_safety_buffer
                reset_readable = datetime.fromtimestamp(reset_epoch, tz=timezone.utc).isoformat()
                logger.warning(
                    "Rate limit remaining (%d) is below safety threshold (%d). "
                    "Sleeping for %.1fs until reset at %s (+%.1fs safety buffer).",
                    remaining,
                    self.rate_limit_threshold,
                    sleep_seconds,
                    reset_readable,
                    self.rate_limit_safety_buffer,
                )
                time.sleep(sleep_seconds)
        except (ValueError, TypeError) as exc:
            logger.warning("Failed to parse rate limit headers (%s, %s): %s", remaining_str, reset_str, exc)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=120),
        retry=retry_if_exception(is_retryable_exception),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _execute_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> requests.Response:
        """Execute HTTP request with tenacity retry and rate-limit error classification.

        Args:
            method: HTTP method ('GET', 'POST', etc.).
            url: Full target URL.
            params: Query parameters.
            **kwargs: Extra parameters passed to requests.Session.request.

        Returns:
            Successful requests.Response.

        Raises:
            GitHubRateLimitError: If 403 (rate limited) or 429 response.
            requests.exceptions.HTTPError: For non-retryable 4xx/5xx responses.
            requests.exceptions.RequestException: For network/connection errors.
        """
        response = self.session.request(method=method, url=url, params=params, **kwargs)

        # Inspect headers for rate-limit quota safety after EVERY response
        self._check_rate_limit(response)

        # Check for rate-limiting status codes to trigger tenacity retry
        if response.status_code == 429:
            raise GitHubRateLimitError(
                f"GitHub rate limit exceeded (429 Too Many Requests): {response.text}",
                response=response,
            )

        if response.status_code == 403 and _is_rate_limit_403(response):
            raise GitHubRateLimitError(
                f"GitHub 403 Rate Limit / Secondary Rate Limit hit: {response.text}",
                response=response,
            )

        # Raise HTTPError for any remaining 4xx/5xx errors
        response.raise_for_status()
        return response

    def get(
        self,
        endpoint_or_url: str,
        params: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> requests.Response:
        """Send a GET request to the GitHub API.

        Args:
            endpoint_or_url: Relative endpoint path (e.g., '/repos/owner/repo') or absolute URL.
            params: Query parameters.
            **kwargs: Extra keyword arguments for requests.

        Returns:
            The HTTP response object.
        """
        if endpoint_or_url.startswith("http://") or endpoint_or_url.startswith("https://"):
            url = endpoint_or_url
        else:
            url = f"{self.base_url}/{endpoint_or_url.lstrip('/')}"
        return self._execute_request(method="GET", url=url, params=params, **kwargs)

    def paginate(
        self,
        endpoint_or_url: str,
        params: Optional[Dict[str, Any]] = None,
        max_pages: int = 10,
        per_page: int = 100,
    ) -> List[Dict[str, Any]]:
        """Paginate through GitHub list endpoints following Link headers.

        Follows RFC 5988 Link headers (rel="next") up to max_pages.

        Args:
            endpoint_or_url: Initial endpoint path or absolute URL.
            params: Initial query parameters.
            max_pages: Maximum number of pages to fetch (default: 10).
            per_page: Items per page (default: 100).

        Returns:
            Flattened list of all dictionary records fetched across pages.
        """
        items: List[Dict[str, Any]] = []
        page_count = 0

        # Prepare initial query parameters
        current_params: Optional[Dict[str, Any]] = dict(params) if params else {}
        current_params.setdefault("per_page", per_page)

        current_url: Optional[str] = endpoint_or_url

        while current_url and page_count < max_pages:
            # First request uses initial endpoint + params; subsequent requests follow Link url
            if page_count == 0:
                resp = self.get(current_url, params=current_params)
            else:
                resp = self.get(current_url)

            data = resp.json()
            if isinstance(data, list):
                items.extend(data)
                logger.debug(
                    "Fetched page %d with %d items from %s",
                    page_count + 1,
                    len(data),
                    current_url,
                )
                if len(data) == 0:
                    break
            elif isinstance(data, dict):
                # Rare endpoint returning object with list attribute
                items.append(data)
                break
            else:
                break

            page_count += 1

            # Follow Link header rel="next"
            next_link = resp.links.get("next")
            if next_link and "url" in next_link:
                current_url = next_link["url"]
            else:
                current_url = None

        logger.info(
            "Pagination complete for %s: fetched %d records across %d page(s).",
            endpoint_or_url,
            len(items),
            page_count,
        )
        return items

    def get_repo_info(self, owner: str, repo: str) -> Dict[str, Any]:
        """Fetch repository metadata.

        Args:
            owner: Repository owner (organization or user).
            repo: Repository name.

        Returns:
            Dictionary with repository metadata.
        """
        endpoint = f"/repos/{owner}/{repo}"
        logger.info("Fetching repository info for %s/%s", owner, repo)
        response = self.get(endpoint)
        data: Dict[str, Any] = response.json()
        return data

    def get_commits(
        self,
        owner: str,
        repo: str,
        since: Optional[str] = None,
        max_pages: int = 10,
    ) -> List[Dict[str, Any]]:
        """Fetch commits for a repository with optional incremental filter.

        Args:
            owner: Repository owner.
            repo: Repository name.
            since: Optional ISO 8601 timestamp string (e.g., '2024-01-01T00:00:00Z').
            max_pages: Maximum pages to paginate (default 10).

        Returns:
            List of commit dictionaries.
        """
        endpoint = f"/repos/{owner}/{repo}/commits"
        params: Dict[str, Any] = {}
        if since:
            params["since"] = since

        logger.info(
            "Fetching commits for %s/%s (since=%s, max_pages=%d)",
            owner,
            repo,
            since,
            max_pages,
        )
        return self.paginate(endpoint, params=params, max_pages=max_pages)

    def get_issues(
        self,
        owner: str,
        repo: str,
        since: Optional[str] = None,
        state: str = "all",
        max_pages: int = 10,
    ) -> List[Dict[str, Any]]:
        """Fetch issues for a repository with optional incremental filter.

        Args:
            owner: Repository owner.
            repo: Repository name.
            since: Optional ISO 8601 timestamp string.
            state: Issue state ('all', 'open', 'closed'). Defaults to 'all'.
            max_pages: Maximum pages to paginate (default 10).

        Returns:
            List of issue dictionaries.
        """
        endpoint = f"/repos/{owner}/{repo}/issues"
        params: Dict[str, Any] = {
            "state": state,
            "sort": "updated",
            "direction": "desc",
        }
        if since:
            params["since"] = since

        logger.info(
            "Fetching issues for %s/%s (since=%s, state=%s, max_pages=%d)",
            owner,
            repo,
            since,
            state,
            max_pages,
        )
        return self.paginate(endpoint, params=params, max_pages=max_pages)

    def get_pulls(
        self,
        owner: str,
        repo: str,
        since: Optional[str] = None,
        state: str = "all",
        max_pages: int = 10,
    ) -> List[Dict[str, Any]]:
        """Fetch pull requests for a repository with optional incremental filter.

        GitHub's /pulls endpoint does not support a native 'since' parameter,
        so results sorted by 'updated' desc are filtered client-side.

        Args:
            owner: Repository owner.
            repo: Repository name.
            since: Optional ISO 8601 timestamp string.
            state: Pull request state ('all', 'open', 'closed'). Defaults to 'all'.
            max_pages: Maximum pages to paginate (default 10).

        Returns:
            List of pull request dictionaries.
        """
        endpoint = f"/repos/{owner}/{repo}/pulls"
        params: Dict[str, Any] = {
            "state": state,
            "sort": "updated",
            "direction": "desc",
        }

        logger.info(
            "Fetching pull requests for %s/%s (since=%s, state=%s, max_pages=%d)",
            owner,
            repo,
            since,
            state,
            max_pages,
        )
        raw_pulls = self.paginate(endpoint, params=params, max_pages=max_pages)

        if not since:
            return raw_pulls

        # Client-side filtering when since is provided
        filtered_pulls: List[Dict[str, Any]] = []
        for pr in raw_pulls:
            updated_at = pr.get("updated_at")
            if updated_at and updated_at >= since:
                filtered_pulls.append(pr)
            elif not updated_at:
                filtered_pulls.append(pr)

        logger.info(
            "Filtered pull requests for %s/%s: retained %d of %d based on since=%s",
            owner,
            repo,
            len(filtered_pulls),
            len(raw_pulls),
            since,
        )
        return filtered_pulls


# ---------------------------------------------------------------------------
# State Management Functions
# ---------------------------------------------------------------------------

def load_state(filepath: Optional[Union[str, Path]] = None) -> Dict[str, Any]:
    """Load extraction state tracking last successful sync timestamps per entity.

    Args:
        filepath: Optional path to state JSON file. Defaults to data/state.json.

    Returns:
        Dictionary containing state mapping, e.g. {'owner/repo': {'commits': '...'}}.
    """
    path = Path(filepath) if filepath else DEFAULT_STATE_FILE
    if not path.is_file():
        logger.debug("State file %s does not exist; returning empty state.", path)
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return {}
            data = json.loads(content)
            if isinstance(data, dict):
                return data
            logger.warning("State file %s does not contain a JSON object; returning empty state.", path)
            return {}
    except Exception as exc:
        logger.warning("Error reading state file %s: %s. Returning empty state.", path, exc)
        return {}


def save_state(state: Dict[str, Any], filepath: Optional[Union[str, Path]] = None) -> None:
    """Save extraction state tracking last successful sync timestamps per entity.

    Args:
        state: State dictionary to persist.
        filepath: Optional path to state JSON file. Defaults to data/state.json.
    """
    path = Path(filepath) if filepath else DEFAULT_STATE_FILE
    path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = path.with_suffix(".tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    temp_path.replace(path)
    logger.info("Persisted sync state to %s", path)


def get_last_sync(
    owner: str,
    repo: str,
    entity: str,
    filepath: Optional[Union[str, Path]] = None,
) -> Optional[str]:
    """Retrieve the last recorded sync timestamp for a given repository and entity.

    Args:
        owner: Repository owner.
        repo: Repository name.
        entity: Entity name ('commits', 'issues', 'pulls').
        filepath: Optional state file path.

    Returns:
        ISO 8601 timestamp string or None if not previously synced.
    """
    state = load_state(filepath=filepath)
    repo_key = f"{owner}/{repo}"
    return state.get(repo_key, {}).get(entity)


def update_sync_state(
    owner: str,
    repo: str,
    entity: str,
    timestamp: Optional[str] = None,
    filepath: Optional[Union[str, Path]] = None,
) -> Dict[str, Any]:
    """Update and persist the last recorded sync timestamp for a repository and entity.

    Args:
        owner: Repository owner.
        repo: Repository name.
        entity: Entity name ('commits', 'issues', 'pulls').
        timestamp: ISO 8601 timestamp. If None, current UTC timestamp is used.
        filepath: Optional state file path.

    Returns:
        Updated state dictionary.
    """
    state = load_state(filepath=filepath)
    repo_key = f"{owner}/{repo}"
    if repo_key not in state:
        state[repo_key] = {}

    sync_ts = timestamp or datetime.now(timezone.utc).isoformat()
    state[repo_key][entity] = sync_ts
    save_state(state, filepath=filepath)
    return state


# ---------------------------------------------------------------------------
# Module-level convenience functions
# ---------------------------------------------------------------------------

_default_client: Optional[GitHubClient] = None


def get_default_client() -> GitHubClient:
    """Retrieve or initialize the singleton GitHubClient instance."""
    global _default_client
    if _default_client is None:
        _default_client = GitHubClient()
    return _default_client


def get_repo_info(owner: str, repo: str, client: Optional[GitHubClient] = None) -> Dict[str, Any]:
    """Module-level function to fetch repository metadata.

    Args:
        owner: Repository owner.
        repo: Repository name.
        client: Optional GitHubClient instance.

    Returns:
        Dictionary with repository metadata.
    """
    c = client or get_default_client()
    return c.get_repo_info(owner=owner, repo=repo)


def get_commits(
    owner: str,
    repo: str,
    since: Optional[str] = None,
    client: Optional[GitHubClient] = None,
    max_pages: int = 10,
) -> List[Dict[str, Any]]:
    """Module-level function to fetch repository commits.

    Args:
        owner: Repository owner.
        repo: Repository name.
        since: Optional ISO 8601 timestamp string.
        client: Optional GitHubClient instance.
        max_pages: Maximum pages to paginate.

    Returns:
        List of commit records.
    """
    c = client or get_default_client()
    return c.get_commits(owner=owner, repo=repo, since=since, max_pages=max_pages)


def get_issues(
    owner: str,
    repo: str,
    since: Optional[str] = None,
    state: str = "all",
    client: Optional[GitHubClient] = None,
    max_pages: int = 10,
) -> List[Dict[str, Any]]:
    """Module-level function to fetch repository issues.

    Args:
        owner: Repository owner.
        repo: Repository name.
        since: Optional ISO 8601 timestamp string.
        state: Issue state ('all', 'open', 'closed').
        client: Optional GitHubClient instance.
        max_pages: Maximum pages to paginate.

    Returns:
        List of issue records.
    """
    c = client or get_default_client()
    return c.get_issues(owner=owner, repo=repo, since=since, state=state, max_pages=max_pages)


def get_pulls(
    owner: str,
    repo: str,
    since: Optional[str] = None,
    state: str = "all",
    client: Optional[GitHubClient] = None,
    max_pages: int = 10,
) -> List[Dict[str, Any]]:
    """Module-level function to fetch repository pull requests.

    Args:
        owner: Repository owner.
        repo: Repository name.
        since: Optional ISO 8601 timestamp string.
        state: Pull request state ('all', 'open', 'closed').
        client: Optional GitHubClient instance.
        max_pages: Maximum pages to paginate.

    Returns:
        List of pull request records.
    """
    c = client or get_default_client()
    return c.get_pulls(owner=owner, repo=repo, since=since, state=state, max_pages=max_pages)


# ---------------------------------------------------------------------------
# Test Execution Block
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    test_owner = "apache"
    test_repo = "airflow"

    print("=" * 70)
    print(f"GitPulse: Testing GitHub REST API Client with '{test_owner}/{test_repo}'")
    print("=" * 70)

    client = GitHubClient()

    # 1. Test get_repo_info
    print("\n[1/5] Fetching repository metadata...")
    repo_info = client.get_repo_info(test_owner, test_repo)
    print(f"  - Full Name:    {repo_info.get('full_name')}")
    print(f"  - Description:  {repo_info.get('description')}")
    print(f"  - Stars:        {repo_info.get('stargazers_count'):,}")
    print(f"  - Forks:        {repo_info.get('forks_count'):,}")
    print(f"  - Default Branch: {repo_info.get('default_branch')}")

    # 2. Test get_commits (test pagination with max_pages=1 to stay within rate limits)
    print("\n[2/5] Fetching recent commits (max_pages=1)...")
    commits = client.get_commits(test_owner, test_repo, max_pages=1)
    print(f"  - Retrieved:    {len(commits)} commits")
    if commits:
        first_commit = commits[0]
        sha = first_commit.get("sha", "")[:7]
        msg = first_commit.get("commit", {}).get("message", "").split("\n")[0]
        author = first_commit.get("commit", {}).get("author", {}).get("name")
        date = first_commit.get("commit", {}).get("author", {}).get("date")
        print(f"  - Latest Commit: [{sha}] {msg} by {author} ({date})")

    # 3. Test get_issues (test pagination with max_pages=1)
    print("\n[3/5] Fetching recent issues (max_pages=1)...")
    issues = client.get_issues(test_owner, test_repo, max_pages=1)
    print(f"  - Retrieved:    {len(issues)} issues")
    if issues:
        first_issue = issues[0]
        issue_num = first_issue.get("number")
        title = first_issue.get("title")
        state = first_issue.get("state")
        updated_at = first_issue.get("updated_at")
        print(f"  - Sample Issue: #{issue_num} [{state}] \"{title}\" (updated {updated_at})")

    # 4. Test get_pulls (test pagination with max_pages=1)
    print("\n[4/5] Fetching recent pull requests (max_pages=1)...")
    pulls = client.get_pulls(test_owner, test_repo, max_pages=1)
    print(f"  - Retrieved:    {len(pulls)} pull requests")
    if pulls:
        first_pr = pulls[0]
        pr_num = first_pr.get("number")
        pr_title = first_pr.get("title")
        pr_state = first_pr.get("state")
        pr_updated = first_pr.get("updated_at")
        print(f"  - Sample PR:    #{pr_num} [{pr_state}] \"{pr_title}\" (updated {pr_updated})")

    # 5. Test state management
    print("\n[5/5] Testing state persistence (data/state.json)...")
    now_iso = datetime.now(timezone.utc).isoformat()
    update_sync_state(test_owner, test_repo, "repo_info", now_iso)
    update_sync_state(test_owner, test_repo, "commits", now_iso)
    update_sync_state(test_owner, test_repo, "issues", now_iso)
    update_sync_state(test_owner, test_repo, "pulls", now_iso)

    loaded = load_state()
    repo_state = loaded.get(f"{test_owner}/{test_repo}", {})
    print(f"  - Persisted state for {test_owner}/{test_repo}:")
    for entity, ts in repo_state.items():
        print(f"      * {entity:10s} -> {ts}")

    print("\n" + "=" * 70)
    print("ALL TESTS PASSED SUCCESSFULLY!")
    print("=" * 70)

"""Extract package for GitPulse data pipeline."""

from .github_client import GitHubClient, load_state, save_state

__all__ = ["GitHubClient", "load_state", "save_state"]

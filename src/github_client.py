"""GitHub API client with rate limiting and retry logic."""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Generator, Optional

import requests


class GitHubClient:
    """Handles GitHub API interactions with rate limiting and pagination."""

    BASE_URL = "https://api.github.com"
    GREPTILE_BOT_NAMES = [
        "greptile-apps[bot]",
        "greptileai",
        "greptile[bot]"
    ]

    def __init__(self, token: Optional[str] = None):
        self.token = token or os.environ.get("GITHUB_TOKEN")
        if not self.token:
            raise ValueError("GITHUB_TOKEN environment variable required")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"
        })
        self.rate_limit_remaining = 5000
        self.rate_limit_reset: Optional[datetime] = None
        self.logger = logging.getLogger(__name__)

    def _handle_rate_limit(self, response: requests.Response) -> None:
        """Update rate limit tracking from response headers."""
        self.rate_limit_remaining = int(
            response.headers.get("X-RateLimit-Remaining", 5000)
        )
        reset_timestamp = response.headers.get("X-RateLimit-Reset")
        if reset_timestamp:
            self.rate_limit_reset = datetime.fromtimestamp(
                int(reset_timestamp), tz=timezone.utc
            )

        if self.rate_limit_remaining < 100:
            if self.rate_limit_reset:
                sleep_time = (
                    self.rate_limit_reset - datetime.now(timezone.utc)
                ).total_seconds()
                if sleep_time > 0:
                    self.logger.warning(
                        f"Rate limit low ({self.rate_limit_remaining}). "
                        f"Sleeping {sleep_time:.0f}s"
                    )
                    time.sleep(min(sleep_time + 5, 3600))

    def _paginate(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """Generator that handles pagination automatically."""
        params = params or {}
        params.setdefault("per_page", 100)

        while url:
            response = self._request("GET", url, params=params)
            if response is None:
                break

            data = response.json()
            if isinstance(data, list):
                yield from data
            else:
                yield data
                break

            url = None
            link_header = response.headers.get("Link", "")
            for link in link_header.split(","):
                if 'rel="next"' in link:
                    url = link.split(";")[0].strip("<> ")
                    params = {}
                    break

    def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any
    ) -> Optional[requests.Response]:
        """Make a request with retry logic."""
        if not url.startswith("http"):
            url = f"{self.BASE_URL}{url}"

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.request(method, url, **kwargs)
                self._handle_rate_limit(response)

                if response.status_code == 200:
                    return response
                elif response.status_code == 404:
                    self.logger.debug(f"Resource not found: {url}")
                    return None
                elif response.status_code == 403:
                    if "rate limit" in response.text.lower():
                        self._handle_rate_limit(response)
                        continue
                    self.logger.warning(f"Access denied: {url}")
                    return None
                elif response.status_code >= 500:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    self.logger.warning(
                        f"Request failed ({response.status_code}): {url}"
                    )
                    return None

            except requests.RequestException as e:
                self.logger.warning(f"Request exception: {e}")
                time.sleep(2 ** attempt)

        return None

    def get_pull_requests(
        self,
        owner: str,
        repo: str,
        since: Optional[datetime] = None,
        state: str = "all",
        sort_by: str = "updated"
    ) -> Generator[Dict[str, Any], None, None]:
        """Get PRs for a repo, filtered by date.

        Args:
            sort_by: "created" or "updated" - determines which field to filter on
        """
        url = f"/repos/{owner}/{repo}/pulls"
        params: Dict[str, Any] = {
            "state": state,
            "sort": sort_by,
            "direction": "desc"
        }

        date_field = "created_at" if sort_by == "created" else "updated_at"

        for pr in self._paginate(url, params):
            pr_date = datetime.fromisoformat(
                pr[date_field].replace("Z", "+00:00")
            )
            if since and pr_date < since:
                # PRs are sorted desc, so we can stop here
                break
            yield pr

    def get_pr_review_comments(
        self,
        owner: str,
        repo: str,
        pr_number: int
    ) -> Generator[Dict[str, Any], None, None]:
        """Get review comments (inline comments) for a PR."""
        url = f"/repos/{owner}/{repo}/pulls/{pr_number}/comments"
        yield from self._paginate(url)

    def get_pr_issue_comments(
        self,
        owner: str,
        repo: str,
        pr_number: int
    ) -> Generator[Dict[str, Any], None, None]:
        """Get issue comments (general comments) for a PR."""
        url = f"/repos/{owner}/{repo}/issues/{pr_number}/comments"
        yield from self._paginate(url)

    def get_pr_reviews(
        self,
        owner: str,
        repo: str,
        pr_number: int
    ) -> Generator[Dict[str, Any], None, None]:
        """Get review submissions for a PR."""
        url = f"/repos/{owner}/{repo}/pulls/{pr_number}/reviews"
        yield from self._paginate(url)

    def is_greptile_user(self, user: Optional[Dict[str, Any]]) -> bool:
        """Check if a user is the Greptile bot."""
        if not user:
            return False
        login = user.get("login", "").lower()
        return any(
            bot_name.lower() in login
            for bot_name in self.GREPTILE_BOT_NAMES
        )

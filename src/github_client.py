"""GitHub API client with rate limiting and retry logic."""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Generator, Optional

import requests

from .constants import GREPTILE_BOT_NAMES


class GitHubClient:
    """Handles GitHub API interactions with rate limiting and pagination."""

    BASE_URL = "https://api.github.com"

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
            for bot_name in GREPTILE_BOT_NAMES
        )

    def get_pr_files(
        self,
        owner: str,
        repo: str,
        pr_number: int
    ) -> Dict[str, str]:
        """Get file patches for a PR.

        Returns dict mapping file path to patch content.
        """
        url = f"/repos/{owner}/{repo}/pulls/{pr_number}/files"
        file_patches: Dict[str, str] = {}

        for file_data in self._paginate(url):
            filename = file_data.get("filename", "")
            patch = file_data.get("patch", "")
            if filename and patch:
                file_patches[filename] = patch

        return file_patches

    def get_pr_details(
        self,
        owner: str,
        repo: str,
        pr_number: int
    ) -> Optional[Dict[str, Any]]:
        """Get PR metadata."""
        url = f"/repos/{owner}/{repo}/pulls/{pr_number}"
        response = self._request("GET", url)
        if response:
            return response.json()
        return None

    def enrich_comment_with_context(
        self,
        comment: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Enrich an addressed comment with GitHub context.

        Phase 2 of the two-phase approach:
        - Fetch full PR diff for the relevant file
        - Find any replies to the Greptile comment
        - Add PR metadata

        Args:
            comment: Comment dict from fetch_new_addressed_comments()

        Returns:
            Enriched comment dict with diff context and replies
        """
        repo = comment.get("repo", "")
        pr_number = comment.get("pr_number")
        file_path = comment.get("file_path")

        if "/" not in repo:
            self.logger.warning(f"Invalid repo format: {repo}")
            return comment

        owner, repo_name = repo.split("/", 1)

        # Get PR details
        pr_details = self.get_pr_details(owner, repo_name, pr_number)
        if pr_details:
            comment["pr_title"] = pr_details.get("title", comment.get("pr_title"))
            comment["pr_state"] = pr_details.get("state", comment.get("pr_state"))
            comment["pr_url"] = pr_details.get("html_url", comment.get("pr_url"))

        # Get file diff if we have a file path
        if file_path:
            file_patches = self.get_pr_files(owner, repo_name, pr_number)
            comment["file_patch"] = file_patches.get(file_path, "")

        # Get replies to this comment
        comment["reply_body"] = self._find_replies_to_comment(
            owner, repo_name, pr_number, comment.get("comment_body", "")
        )

        return comment

    def _find_replies_to_comment(
        self,
        owner: str,
        repo: str,
        pr_number: int,
        greptile_comment_body: str
    ) -> Optional[str]:
        """Find developer replies to a Greptile comment.

        Since we match by body content, we look for comments that:
        1. Are not from Greptile
        2. Are in reply to a comment with matching body
        """
        if not greptile_comment_body:
            return None

        # Get all review comments
        greptile_comment_id = None
        replies = []

        for comment in self.get_pr_review_comments(owner, repo, pr_number):
            user = comment.get("user", {})
            body = comment.get("body", "")

            # Find the Greptile comment by matching body prefix
            if self.is_greptile_user(user):
                if greptile_comment_body[:100] in body or body[:100] in greptile_comment_body:
                    greptile_comment_id = comment.get("id")
            # Check if this is a reply to the Greptile comment
            elif greptile_comment_id and comment.get("in_reply_to_id") == greptile_comment_id:
                replies.append(body)

        if replies:
            return "\n---\n".join(replies)

        return None

    def enrich_comments_batch(
        self,
        comments: list
    ) -> list:
        """Enrich multiple comments with GitHub context.

        Groups comments by PR to minimize API calls.
        """
        # Group by repo/pr_number
        from collections import defaultdict
        pr_comments: Dict[str, list] = defaultdict(list)

        for comment in comments:
            key = f"{comment.get('repo')}/{comment.get('pr_number')}"
            pr_comments[key].append(comment)

        enriched = []
        total_prs = len(pr_comments)

        for i, (pr_key, pr_comment_list) in enumerate(pr_comments.items()):
            self.logger.info(f"Enriching PR {i+1}/{total_prs}: {pr_key}")

            # Fetch context once per PR
            first_comment = pr_comment_list[0]
            repo = first_comment.get("repo", "")
            pr_number = first_comment.get("pr_number")

            if "/" not in repo:
                enriched.extend(pr_comment_list)
                continue

            owner, repo_name = repo.split("/", 1)

            # Get PR details and files once
            pr_details = self.get_pr_details(owner, repo_name, pr_number)
            file_patches = self.get_pr_files(owner, repo_name, pr_number)

            # Get all review comments for reply detection
            all_review_comments = list(self.get_pr_review_comments(owner, repo_name, pr_number))

            for comment in pr_comment_list:
                # Add PR details
                if pr_details:
                    comment["pr_title"] = pr_details.get("title", comment.get("pr_title"))
                    comment["pr_state"] = pr_details.get("state", comment.get("pr_state"))
                    comment["pr_url"] = pr_details.get("html_url", comment.get("pr_url"))

                # Add file patch
                file_path = comment.get("file_path")
                if file_path:
                    comment["file_patch"] = file_patches.get(file_path, "")

                # Find replies and actual comment URL
                reply_body, actual_url = self._find_reply_and_url(
                    all_review_comments,
                    comment.get("comment_body", "")
                )
                comment["reply_body"] = reply_body
                if actual_url:
                    comment["comment_url"] = actual_url

                enriched.append(comment)

        return enriched

    def _find_reply_and_url(
        self,
        all_comments: list,
        greptile_body: str
    ) -> tuple:
        """Find replies and URL for a Greptile comment from pre-fetched comments.

        Returns:
            (reply_body, comment_url) tuple
        """
        if not greptile_body:
            return None, None

        greptile_comment_id = None
        greptile_comment_url = None
        replies = []

        for comment in all_comments:
            user = comment.get("user", {})
            body = comment.get("body", "")

            if self.is_greptile_user(user):
                if greptile_body[:100] in body or body[:100] in greptile_body:
                    greptile_comment_id = comment.get("id")
                    greptile_comment_url = comment.get("html_url")
            elif greptile_comment_id and comment.get("in_reply_to_id") == greptile_comment_id:
                replies.append(body)

        reply_body = "\n---\n".join(replies) if replies else None
        return reply_body, greptile_comment_url

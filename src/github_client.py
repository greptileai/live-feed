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

    def enrich_comments_batch(
        self,
        comments: list
    ) -> list:
        """Enrich multiple comments with GitHub context.

        Groups comments by PR to minimize API calls.
        Uses the same pattern as comment_fetcher.py: iterate through all
        GitHub review comments once, build a complete index of Greptile
        comments with their html_url and replies, then match DB comments.
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

            # Build index of all Greptile comments from GitHub API
            # Same pattern as comment_fetcher.py: single pass, extract URLs and replies
            all_review_comments = list(self.get_pr_review_comments(owner, repo_name, pr_number))
            greptile_index = self._build_greptile_comment_index(all_review_comments)

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

                # Match this DB comment to a GitHub comment by body
                db_body = comment.get("comment_body", "")
                matched = self._match_db_comment_to_github(db_body, greptile_index)
                if matched:
                    comment["comment_url"] = matched["html_url"]
                    comment["reply_body"] = matched.get("reply_body")
                else:
                    comment["reply_body"] = None

                enriched.append(comment)

        return enriched

    def _build_greptile_comment_index(
        self,
        all_review_comments: list
    ) -> list:
        """Build an index of all Greptile comments with their URLs and replies.

        Single pass through review comments (same pattern as comment_fetcher.py):
        1. Identify all Greptile comments, store their id, body, html_url
        2. Find replies to each Greptile comment via in_reply_to_id

        Returns:
            List of dicts with: body, html_url, reply_body, github_id
        """
        # First pass: identify Greptile comments
        greptile_comments = {}  # {github_id: {body, html_url, reply_body}}
        greptile_ids = set()

        for comment in all_review_comments:
            if self.is_greptile_user(comment.get("user")):
                cid = comment.get("id")
                greptile_ids.add(cid)
                greptile_comments[cid] = {
                    "github_id": cid,
                    "body": comment.get("body", ""),
                    "html_url": comment.get("html_url"),
                    "replies": []
                }

        # Second pass: find replies to Greptile comments
        for comment in all_review_comments:
            if not self.is_greptile_user(comment.get("user")):
                reply_to = comment.get("in_reply_to_id")
                if reply_to and reply_to in greptile_ids:
                    greptile_comments[reply_to]["replies"].append(
                        comment.get("body", "")
                    )

        # Build final index with joined reply_body
        index = []
        for cid, data in greptile_comments.items():
            reply_body = "\n---\n".join(data["replies"]) if data["replies"] else None
            index.append({
                "github_id": data["github_id"],
                "body": data["body"],
                "html_url": data["html_url"],
                "reply_body": reply_body
            })

        return index

    def _match_db_comment_to_github(
        self,
        db_body: str,
        greptile_index: list
    ) -> Optional[Dict[str, Any]]:
        """Match a DB comment to a GitHub comment by body content.

        Uses progressively looser matching:
        1. Exact body match
        2. DB body contained in GitHub body (DB may truncate)
        3. GitHub body contained in DB body
        4. First 100 char prefix match

        Returns the matched GitHub comment dict or None.
        """
        if not db_body:
            return None

        # Try exact match first
        for gc in greptile_index:
            if gc["body"] == db_body:
                return gc

        # Try containment (DB body in GitHub, or vice versa)
        for gc in greptile_index:
            gh_body = gc["body"]
            if not gh_body:
                continue
            if db_body in gh_body or gh_body in db_body:
                return gc

        # Fallback: prefix match (first 100 chars)
        db_prefix = db_body[:100]
        for gc in greptile_index:
            gh_body = gc["body"]
            if gh_body and db_prefix in gh_body:
                return gc

        return None

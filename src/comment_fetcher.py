"""Core logic for fetching and filtering Greptile comments."""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .github_client import GitHubClient
from .models import GreptileComment, PRWithGreptileComments, RepoConfig, RepoState


def extract_score(comment_body: str) -> Optional[int]:
    """Extract confidence score from Greptile comment body.

    Looks for patterns like:
    - "Confidence: 3/5"
    - "Confidence score: 3/5"
    - "Score: 3/5"
    - "3/5" at start of line or after colon
    """
    if not comment_body:
        return None

    # Common patterns for Greptile scores
    patterns = [
        r'[Cc]onfidence(?:\s+score)?[:\s]+(\d)/5',
        r'[Ss]core[:\s]+(\d)/5',
        r'(?:^|[\s:])(\d)/5',
    ]

    for pattern in patterns:
        match = re.search(pattern, comment_body)
        if match:
            return int(match.group(1))

    return None


class CommentFetcher:
    """Fetches Greptile comments from PRs with incremental processing."""

    def __init__(self, github_client: GitHubClient):
        self.client = github_client
        self.logger = logging.getLogger(__name__)

    def fetch_greptile_comments_for_repo(
        self,
        repo_config: RepoConfig,
        state: Optional[RepoState] = None
    ) -> Tuple[List[PRWithGreptileComments], RepoState]:
        """Fetch Greptile comments for new PRs and PRs with new commits."""
        owner, repo_name = repo_config.repo.split("/", 1)
        since = state.last_checked if state else None
        old_pr_shas = state.pr_shas if state and state.pr_shas else {}

        self.logger.info(f"Processing {repo_config.repo} (since: {since})")

        results: List[PRWithGreptileComments] = []
        latest_pr_number = state.last_pr_number if state else None
        new_pr_shas: Dict[int, str] = {}

        try:
            # Fetch PRs updated since last check (includes new PRs and PRs with new commits)
            for pr in self.client.get_pull_requests(
                owner, repo_name, since=since, sort_by="updated"
            ):
                pr_number = pr["number"]
                head_sha = pr["head"]["sha"]
                pr_created = datetime.fromisoformat(
                    pr["created_at"].replace("Z", "+00:00")
                )

                # Determine if this PR needs processing:
                # 1. New PR (created since last check)
                # 2. Existing PR with new commits (SHA changed)
                is_new_pr = since is None or pr_created >= since
                old_sha = old_pr_shas.get(pr_number)
                has_new_commits = old_sha is not None and old_sha != head_sha

                if is_new_pr or has_new_commits:
                    trigger_type = "new_pr" if is_new_pr else "new_commits"
                    self.logger.debug(f"  PR #{pr_number}: {trigger_type}")

                    pr_comments = self._fetch_greptile_comments_for_pr(
                        owner, repo_name, pr
                    )

                    pr_updated = datetime.fromisoformat(
                        pr["updated_at"].replace("Z", "+00:00")
                    )

                    if pr_comments:
                        results.append(PRWithGreptileComments(
                            repo=repo_config.repo,
                            org=repo_config.org,
                            pr_number=pr_number,
                            pr_title=pr["title"],
                            pr_author=pr["user"]["login"],
                            pr_url=pr["html_url"],
                            pr_created_at=pr_created,
                            pr_updated_at=pr_updated,
                            pr_state=pr["state"],
                            greptile_comments=pr_comments,
                            fetched_at=datetime.now(timezone.utc),
                            head_sha=head_sha,
                            trigger_type=trigger_type
                        ))

                # Track SHA for next run
                new_pr_shas[pr_number] = head_sha

                if latest_pr_number is None or pr_number > latest_pr_number:
                    latest_pr_number = pr_number

            new_state = RepoState(
                repo=repo_config.repo,
                last_checked=datetime.now(timezone.utc),
                last_pr_number=latest_pr_number,
                error_count=0,
                pr_shas=new_pr_shas
            )

        except Exception as e:
            self.logger.error(f"Error processing {repo_config.repo}: {e}")
            new_state = RepoState(
                repo=repo_config.repo,
                last_checked=state.last_checked if state else datetime.now(timezone.utc),
                last_pr_number=state.last_pr_number if state else None,
                error_count=(state.error_count + 1) if state else 1,
                pr_shas=old_pr_shas
            )

        return results, new_state

    def _fetch_greptile_comments_for_pr(
        self,
        owner: str,
        repo: str,
        pr: Dict[str, Any]
    ) -> List[GreptileComment]:
        """Fetch all Greptile comments for a single PR."""
        comments: List[GreptileComment] = []
        pr_number = pr["number"]

        # Review comments (inline comments on diff)
        for comment in self.client.get_pr_review_comments(owner, repo, pr_number):
            if self.client.is_greptile_user(comment.get("user")):
                body = comment.get("body", "")
                comments.append(GreptileComment(
                    comment_id=comment["id"],
                    comment_body=body,
                    comment_url=comment["html_url"],
                    created_at=datetime.fromisoformat(
                        comment["created_at"].replace("Z", "+00:00")
                    ),
                    updated_at=datetime.fromisoformat(
                        comment["updated_at"].replace("Z", "+00:00")
                    ),
                    file_path=comment.get("path"),
                    line_number=comment.get("line") or comment.get("original_line"),
                    diff_hunk=comment.get("diff_hunk"),
                    comment_type="review_comment",
                    score=extract_score(body)
                ))

        # Issue comments (general comments on PR)
        for comment in self.client.get_pr_issue_comments(owner, repo, pr_number):
            if self.client.is_greptile_user(comment.get("user")):
                body = comment.get("body", "")
                comments.append(GreptileComment(
                    comment_id=comment["id"],
                    comment_body=body,
                    comment_url=comment["html_url"],
                    created_at=datetime.fromisoformat(
                        comment["created_at"].replace("Z", "+00:00")
                    ),
                    updated_at=datetime.fromisoformat(
                        comment["updated_at"].replace("Z", "+00:00")
                    ),
                    file_path=None,
                    line_number=None,
                    diff_hunk=None,
                    comment_type="issue_comment",
                    score=extract_score(body)
                ))

        # Review bodies (from review submissions)
        for review in self.client.get_pr_reviews(owner, repo, pr_number):
            if self.client.is_greptile_user(review.get("user")) and review.get("body"):
                body = review.get("body", "")
                comments.append(GreptileComment(
                    comment_id=review["id"],
                    comment_body=body,
                    comment_url=review["html_url"],
                    created_at=datetime.fromisoformat(
                        review["submitted_at"].replace("Z", "+00:00")
                    ),
                    updated_at=datetime.fromisoformat(
                        review["submitted_at"].replace("Z", "+00:00")
                    ),
                    file_path=None,
                    line_number=None,
                    diff_hunk=None,
                    comment_type="review_body",
                    score=extract_score(body)
                ))

        return comments

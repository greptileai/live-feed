"""Data models for Greptile comment monitor."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List


@dataclass
class RepoConfig:
    """Parsed from oss_master.csv."""
    repo: str
    link: str
    org: str
    total_reviews: int
    reviews_30d: int


@dataclass
class RepoState:
    """Per-repo tracking state."""
    repo: str
    last_checked: datetime
    last_pr_number: Optional[int]
    error_count: int
    # Track HEAD SHA per PR to detect new commits
    pr_shas: Optional[dict] = None  # {pr_number: head_sha}


@dataclass
class GreptileComment:
    """Single Greptile comment."""
    comment_id: int
    comment_body: str
    comment_url: str
    created_at: datetime
    updated_at: datetime
    file_path: Optional[str]
    line_number: Optional[int]
    diff_hunk: Optional[str]
    comment_type: str  # "review_comment" | "issue_comment" | "review_body"
    score: Optional[int] = None  # Extracted confidence score (0-5)


@dataclass
class PRWithGreptileComments:
    """PR metadata with associated Greptile comments."""
    repo: str
    org: str
    pr_number: int
    pr_title: str
    pr_author: str
    pr_url: str
    pr_created_at: datetime
    pr_state: str
    greptile_comments: List[GreptileComment]
    fetched_at: datetime
    head_sha: Optional[str] = None  # Current HEAD SHA of the PR

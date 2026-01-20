"""CSV output for Greptile comments."""

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List
from zoneinfo import ZoneInfo

from .models import PRWithGreptileComments


PST = ZoneInfo("America/Los_Angeles")


def write_comments_csv(
    results: List[PRWithGreptileComments],
    output_file: str = "output/new_comments.csv"
) -> int:
    """Write Greptile comments to CSV file.

    Returns number of comments written.
    """
    logger = logging.getLogger(__name__)
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "repo",
        "pr_number",
        "pr_title",
        "pr_author",
        "pr_url",
        "pr_state",
        "comment_id",
        "comment_type",
        "score",
        "file_path",
        "line_number",
        "comment_body",
        "comment_url",
        "created_at",
        "fetched_at"
    ]

    comment_count = 0

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for pr in results:
            for comment in pr.greptile_comments:
                writer.writerow({
                    "repo": pr.repo,
                    "pr_number": pr.pr_number,
                    "pr_title": pr.pr_title,
                    "pr_author": pr.pr_author,
                    "pr_url": pr.pr_url,
                    "pr_state": pr.pr_state,
                    "comment_id": comment.comment_id,
                    "comment_type": comment.comment_type,
                    "score": comment.score,
                    "file_path": comment.file_path or "",
                    "line_number": comment.line_number or "",
                    "comment_body": comment.comment_body,
                    "comment_url": comment.comment_url,
                    "created_at": comment.created_at.isoformat(),
                    "fetched_at": pr.fetched_at.isoformat()
                })
                comment_count += 1

    logger.info(f"Wrote {comment_count} comments to {output_file}")
    return comment_count


def append_evaluated_comments_csv(
    evaluated_comments: List[dict],
    output_file: str = "output/quality_catches.csv"
) -> int:
    """Append evaluated quality comments to CSV.

    Args:
        evaluated_comments: List of dicts with comment data + LLM evaluation

    Returns number of comments written.
    """
    logger = logging.getLogger(__name__)
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "repo",
        "pr_number",
        "pr_title",
        "pr_url",
        "score",
        "comment_body",
        "comment_url",
        "reply_body",
        "created_at",
        "bug_category",
        "severity",
        "llm_reasoning",
        "evaluated_at"
    ]

    # Check if file exists to determine if we need header
    file_exists = output_path.exists()

    with open(output_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        for comment in evaluated_comments:
            writer.writerow({
                "repo": comment.get("repo", ""),
                "pr_number": comment.get("pr_number", ""),
                "pr_title": comment.get("pr_title", ""),
                "pr_url": comment.get("pr_url", ""),
                "score": comment.get("score", ""),
                "comment_body": comment.get("comment_body", ""),
                "comment_url": comment.get("comment_url", ""),
                "reply_body": comment.get("reply_body", ""),
                "created_at": comment.get("created_at", ""),
                "bug_category": comment.get("bug_category", ""),
                "severity": comment.get("severity", ""),
                "llm_reasoning": comment.get("llm_reasoning", ""),
                "evaluated_at": datetime.now(timezone.utc).isoformat()
            })

    logger.info(f"Appended {len(evaluated_comments)} quality catches to {output_file}")
    return len(evaluated_comments)

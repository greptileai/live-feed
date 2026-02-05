"""Enrich comment data with addressed status from Greptile database."""

import csv
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

import psycopg2
from psycopg2.extras import RealDictCursor


logger = logging.getLogger(__name__)


class EvaluatedCommentsState:
    """Track which comments have been evaluated to avoid re-processing."""

    def __init__(self, state_file: str = "state/evaluated_comments.json"):
        self.state_file = Path(state_file)
        self.evaluated_ids: Set[str] = set()
        self.last_check: Optional[datetime] = None

    def load(self) -> None:
        """Load state from file."""
        if self.state_file.exists():
            with open(self.state_file) as f:
                data = json.load(f)
                self.evaluated_ids = set(data.get("evaluated_ids", []))
                last_check = data.get("last_check")
                if last_check:
                    self.last_check = datetime.fromisoformat(last_check)
            logger.info(f"Loaded {len(self.evaluated_ids)} evaluated comment IDs")
        else:
            logger.info("No evaluated comments state found, starting fresh")

    def save(self) -> None:
        """Save state to file."""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "evaluated_ids": list(self.evaluated_ids),
            "last_check": self.last_check.isoformat() if self.last_check else None
        }
        with open(self.state_file, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved {len(self.evaluated_ids)} evaluated comment IDs")

    def is_evaluated(self, comment_id: str) -> bool:
        """Check if a comment has already been evaluated."""
        return comment_id in self.evaluated_ids

    def mark_evaluated(self, comment_id: str) -> None:
        """Mark a comment as evaluated."""
        self.evaluated_ids.add(comment_id)

    def mark_evaluated_batch(self, comment_ids: List[str]) -> None:
        """Mark multiple comments as evaluated."""
        self.evaluated_ids.update(comment_ids)

    def update_last_check(self) -> None:
        """Update last check timestamp to now."""
        self.last_check = datetime.now(timezone.utc)


def get_db_connection():
    """Get database connection from environment variable."""
    db_url = os.environ.get("GREPTILE_DB_URL")
    if not db_url:
        raise ValueError("GREPTILE_DB_URL environment variable required")

    # Remove pgbouncer parameter if present (not supported by psycopg2)
    db_url = re.sub(r'\?pgbouncer=true', '', db_url)

    return psycopg2.connect(db_url)


def extract_comment_id_from_url(comment_url: str) -> Optional[str]:
    """Extract GitHub comment ID from comment URL.

    Comment URLs look like:
    - https://github.com/owner/repo/pull/123#discussion_r1234567890
    - https://github.com/owner/repo/pull/123#issuecomment-1234567890

    The DB stores node IDs like PRRC_kwDON7PBmc6Qlbr9, so we need to match
    via repo + pr_number + position or fetch the mapping.
    """
    # For now, we'll match via repo + pr_number and body content
    # since the URL ID format differs from DB node ID format
    return None


def get_addressed_status_bulk(
    comments: List[Dict],
    conn
) -> Dict[str, bool]:
    """Query DB for addressed status of multiple comments.

    Args:
        comments: List of comment dicts with repo, pr_number, comment_body
        conn: Database connection

    Returns:
        Dict mapping comment_url to addressed status
    """
    results = {}

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for comment in comments:
            repo = comment.get("repo", "")
            pr_number = comment.get("pr_number")
            comment_url = comment.get("comment_url", "")

            if not repo or not pr_number:
                continue

            # Query for matching comment
            cur.execute("""
                SELECT
                    c.addressed,
                    c.comment_id,
                    c.body
                FROM "MergeRequestComment" c
                JOIN "MergeRequest" mr ON c.merge_request_id = mr.id
                JOIN "Repository" r ON mr.repo_id = r.id
                WHERE r.name = %s
                  AND mr.pr_number = %s
                  AND c.greptile_generated = true
                ORDER BY c.created_at DESC
            """, (repo, pr_number))

            rows = cur.fetchall()

            # Match by checking if comment body is contained
            comment_body = comment.get("comment_body", "")[:200]  # First 200 chars

            for row in rows:
                db_body = row["body"][:200] if row["body"] else ""
                # Fuzzy match - check if significant overlap
                if comment_body and db_body and (
                    comment_body[:100] in db_body or db_body[:100] in comment_body
                ):
                    results[comment_url] = row["addressed"]
                    break

    return results


def enrich_csv_with_addressed(
    input_csv: str,
    output_csv: str,
    db_url: Optional[str] = None
) -> int:
    """Read CSV, add addressed column from DB, write to new CSV.

    Args:
        input_csv: Path to input CSV with comment data
        output_csv: Path to output CSV with addressed column added
        db_url: Optional DB URL (uses env var if not provided)

    Returns:
        Number of comments enriched with addressed status
    """
    if db_url:
        os.environ["GREPTILE_DB_URL"] = db_url

    conn = get_db_connection()

    try:
        # Read input CSV
        with open(input_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            comments = list(reader)

        # Get addressed status for all comments
        addressed_map = get_addressed_status_bulk(comments, conn)

        # Add addressed column if not present
        if "addressed" not in fieldnames:
            # Insert after severity/quality_score or at end
            if "quality_score" in fieldnames:
                idx = fieldnames.index("quality_score") + 1
                fieldnames.insert(idx, "addressed")
            elif "severity" in fieldnames:
                idx = fieldnames.index("severity") + 1
                fieldnames.insert(idx, "addressed")
            else:
                fieldnames.append("addressed")

        # Write output CSV
        enriched_count = 0
        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for comment in comments:
                comment_url = comment.get("comment_url", "")
                if comment_url in addressed_map:
                    comment["addressed"] = addressed_map[comment_url]
                    enriched_count += 1
                else:
                    comment["addressed"] = ""
                writer.writerow(comment)

        logger.info(f"Enriched {enriched_count}/{len(comments)} comments with addressed status")
        return enriched_count

    finally:
        conn.close()


def fetch_new_addressed_comments(
    evaluated_state: EvaluatedCommentsState,
    since: Optional[datetime] = None,
    limit: int = 500,
    repos: Optional[List[str]] = None
) -> List[Dict]:
    """Fetch addressed comments that haven't been evaluated yet.

    Phase 1 of the two-phase approach:
    - Query DB for addressed=true comments
    - Filter out already-evaluated comment IDs
    - Optionally filter to specific repos
    - Return comments needing evaluation

    Args:
        evaluated_state: State tracker for evaluated comments
        since: Only fetch comments updated after this time (defaults to last_check)
        limit: Maximum number of comments to return
        repos: Optional list of repo names to filter (e.g., ["owner/repo"])

    Returns:
        List of comment dicts ready for GitHub context enrichment
    """
    conn = get_db_connection()

    # Use last check time if no since provided
    if since is None:
        since = evaluated_state.last_check

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT
                    r.name as repo,
                    r.remote as remote,
                    mr.pr_number,
                    mr.title as pr_title,
                    mr.state as pr_state,
                    mr.source_repo_url,
                    c.comment_id,
                    c.body as comment_body,
                    c.file_path,
                    c.line_start as line_number,
                    c.addressed,
                    c.upvotes,
                    c.downvotes,
                    c.created_at,
                    c.updated_at
                FROM "MergeRequestComment" c
                JOIN "MergeRequest" mr ON c.merge_request_id = mr.id
                JOIN "Repository" r ON mr.repo_id = r.id
                WHERE c.greptile_generated = true
                  AND c.addressed = true
            """
            params: List = []

            if repos:
                query += " AND r.name = ANY(%s)"
                params.append(repos)

            if since:
                query += " AND c.updated_at >= %s"
                params.append(since)

            query += " ORDER BY c.updated_at DESC LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            rows = cur.fetchall()

            # Filter out already evaluated comments
            new_comments = []
            for row in rows:
                comment_id = row["comment_id"]
                if evaluated_state.is_evaluated(comment_id):
                    continue

                # Build PR URL
                remote = row["remote"]
                repo = row["repo"]
                pr_number = row["pr_number"]

                if remote == "github":
                    pr_url = f"https://github.com/{repo}/pull/{pr_number}"
                elif remote == "gitlab":
                    pr_url = f"https://gitlab.com/{repo}/-/merge_requests/{pr_number}"
                else:
                    pr_url = row.get("source_repo_url") or ""

                new_comments.append({
                    "repo": repo,
                    "remote": remote,
                    "pr_number": pr_number,
                    "pr_title": row["pr_title"],
                    "pr_state": row["pr_state"],
                    "pr_url": pr_url,
                    "comment_id": comment_id,
                    "comment_body": row["comment_body"],
                    "file_path": row["file_path"],
                    "line_number": row["line_number"],
                    "addressed": True,
                    "upvotes": row["upvotes"],
                    "downvotes": row["downvotes"],
                    "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                    "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None
                })

            logger.info(
                f"Found {len(rows)} addressed comments, "
                f"{len(new_comments)} new (not yet evaluated)"
            )
            return new_comments

    finally:
        conn.close()


def fetch_addressed_comments(
    repos: Optional[List[str]] = None,
    since_days: int = 30,
    limit: int = 1000
) -> List[Dict]:
    """Fetch addressed Greptile comments from the database.

    Args:
        repos: Optional list of repo names to filter (e.g., ["owner/repo"])
        since_days: Only fetch comments from the last N days
        limit: Maximum number of comments to return

    Returns:
        List of comment dicts with all relevant fields
    """
    conn = get_db_connection()

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT
                    r.name as repo,
                    r.remote as remote,
                    mr.pr_number,
                    mr.title as pr_title,
                    mr.state as pr_state,
                    mr.source_repo_url as pr_url,
                    c.comment_id,
                    c.body as comment_body,
                    c.file_path,
                    c.line_start as line_number,
                    c.addressed,
                    c.upvotes,
                    c.downvotes,
                    c.helpfulness,
                    c.created_at
                FROM "MergeRequestComment" c
                JOIN "MergeRequest" mr ON c.merge_request_id = mr.id
                JOIN "Repository" r ON mr.repo_id = r.id
                WHERE c.greptile_generated = true
                  AND c.addressed = true
                  AND c.created_at >= NOW() - INTERVAL '%s days'
            """
            params: List = [since_days]

            if repos:
                query += " AND r.name = ANY(%s)"
                params.append(repos)

            query += " ORDER BY c.created_at DESC LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            rows = cur.fetchall()

            comments = []
            for row in rows:
                # Build comment URL from parts
                remote = row["remote"]
                repo = row["repo"]
                pr_number = row["pr_number"]

                if remote == "github":
                    base_url = f"https://github.com/{repo}/pull/{pr_number}"
                elif remote == "gitlab":
                    base_url = f"https://gitlab.com/{repo}/-/merge_requests/{pr_number}"
                else:
                    base_url = row["pr_url"] or ""

                comments.append({
                    "repo": row["repo"],
                    "pr_number": row["pr_number"],
                    "pr_title": row["pr_title"],
                    "pr_state": row["pr_state"],
                    "pr_url": base_url,
                    "comment_id": row["comment_id"],
                    "comment_body": row["comment_body"],
                    "comment_url": base_url,  # Will be enriched later if needed
                    "file_path": row["file_path"],
                    "line_number": row["line_number"],
                    "addressed": row["addressed"],
                    "upvotes": row["upvotes"],
                    "downvotes": row["downvotes"],
                    "helpfulness": row["helpfulness"],
                    "created_at": row["created_at"].isoformat() if row["created_at"] else None
                })

            logger.info(f"Fetched {len(comments)} addressed comments from DB")
            return comments

    finally:
        conn.close()


def get_addressed_comments_for_prs(
    pr_list: List[Dict]
) -> Dict[str, List[Dict]]:
    """Get addressed comments for a list of PRs.

    Args:
        pr_list: List of dicts with 'repo' and 'pr_number' keys

    Returns:
        Dict mapping "repo/pr_number" to list of addressed comments
    """
    if not pr_list:
        return {}

    conn = get_db_connection()

    try:
        results = {}

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for pr in pr_list:
                repo = pr.get("repo")
                pr_number = pr.get("pr_number")

                if not repo or not pr_number:
                    continue

                cur.execute("""
                    SELECT
                        c.comment_id,
                        c.body as comment_body,
                        c.file_path,
                        c.line_start as line_number,
                        c.addressed,
                        c.created_at
                    FROM "MergeRequestComment" c
                    JOIN "MergeRequest" mr ON c.merge_request_id = mr.id
                    JOIN "Repository" r ON mr.repo_id = r.id
                    WHERE r.name = %s
                      AND mr.pr_number = %s
                      AND c.greptile_generated = true
                      AND c.addressed = true
                    ORDER BY c.created_at DESC
                """, (repo, pr_number))

                rows = cur.fetchall()
                key = f"{repo}/{pr_number}"
                results[key] = [dict(row) for row in rows]

        return results

    finally:
        conn.close()


def query_addressed_stats(db_url: Optional[str] = None) -> Dict:
    """Get statistics on addressed vs not addressed Greptile comments.

    Returns dict with counts and percentages.
    """
    if db_url:
        os.environ["GREPTILE_DB_URL"] = db_url

    conn = get_db_connection()

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    addressed,
                    COUNT(*) as count
                FROM "MergeRequestComment"
                WHERE greptile_generated = true
                GROUP BY addressed
            """)

            rows = cur.fetchall()

            stats = {"addressed": 0, "not_addressed": 0, "total": 0}
            for row in rows:
                if row["addressed"]:
                    stats["addressed"] = row["count"]
                else:
                    stats["not_addressed"] = row["count"]

            stats["total"] = stats["addressed"] + stats["not_addressed"]
            if stats["total"] > 0:
                stats["addressed_pct"] = round(100 * stats["addressed"] / stats["total"], 1)
            else:
                stats["addressed_pct"] = 0

            return stats

    finally:
        conn.close()


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) >= 3:
        input_csv = sys.argv[1]
        output_csv = sys.argv[2]
        enrich_csv_with_addressed(input_csv, output_csv)
    else:
        # Just print stats
        stats = query_addressed_stats()
        print(f"Greptile comment stats:")
        print(f"  Addressed: {stats['addressed']} ({stats['addressed_pct']}%)")
        print(f"  Not addressed: {stats['not_addressed']}")
        print(f"  Total: {stats['total']}")

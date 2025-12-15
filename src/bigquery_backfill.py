"""BigQuery backfill script to fetch historical Greptile comments from GH Archive."""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from google.cloud import bigquery

from .csv_parser import parse_repos_csv
from .state_manager import StateManager
from .models import RepoState


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )


def build_query(repo_names: List[str], days_back: int = 365) -> str:
    """Build BigQuery SQL to fetch Greptile comments from GH Archive."""
    # Format repo names for SQL IN clause
    repos_sql = ", ".join(f"'{repo}'" for repo in repo_names)

    query = f"""
    WITH greptile_events AS (
      SELECT
        repo.name AS repo,
        type AS event_type,
        created_at,
        actor.login AS actor,
        payload
      FROM `githubarchive.day.20*`
      WHERE
        _TABLE_SUFFIX >= FORMAT_DATE('%y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY))
        AND type IN (
          'PullRequestReviewCommentEvent',
          'PullRequestReviewEvent',
          'IssueCommentEvent'
        )
        AND LOWER(actor.login) LIKE '%greptile%'
        AND repo.name IN ({repos_sql})
    )
    SELECT
      repo,
      event_type,
      created_at,
      actor,
      -- PR info
      CAST(JSON_EXTRACT_SCALAR(payload, '$.pull_request.number') AS INT64) AS pr_number,
      JSON_EXTRACT_SCALAR(payload, '$.pull_request.title') AS pr_title,
      JSON_EXTRACT_SCALAR(payload, '$.pull_request.user.login') AS pr_author,
      JSON_EXTRACT_SCALAR(payload, '$.pull_request.html_url') AS pr_url,
      JSON_EXTRACT_SCALAR(payload, '$.pull_request.created_at') AS pr_created_at,
      JSON_EXTRACT_SCALAR(payload, '$.pull_request.state') AS pr_state,
      -- Comment info (for review comments and issue comments)
      CAST(JSON_EXTRACT_SCALAR(payload, '$.comment.id') AS INT64) AS comment_id,
      JSON_EXTRACT_SCALAR(payload, '$.comment.body') AS comment_body,
      JSON_EXTRACT_SCALAR(payload, '$.comment.html_url') AS comment_url,
      JSON_EXTRACT_SCALAR(payload, '$.comment.created_at') AS comment_created_at,
      JSON_EXTRACT_SCALAR(payload, '$.comment.path') AS file_path,
      CAST(JSON_EXTRACT_SCALAR(payload, '$.comment.line') AS INT64) AS line_number,
      JSON_EXTRACT_SCALAR(payload, '$.comment.diff_hunk') AS diff_hunk,
      -- Review info (for review events)
      CAST(JSON_EXTRACT_SCALAR(payload, '$.review.id') AS INT64) AS review_id,
      JSON_EXTRACT_SCALAR(payload, '$.review.body') AS review_body,
      JSON_EXTRACT_SCALAR(payload, '$.review.html_url') AS review_url,
      JSON_EXTRACT_SCALAR(payload, '$.review.submitted_at') AS review_submitted_at,
      -- Issue number for IssueCommentEvent (PR comments show as issues)
      CAST(JSON_EXTRACT_SCALAR(payload, '$.issue.number') AS INT64) AS issue_number,
      JSON_EXTRACT_SCALAR(payload, '$.issue.pull_request.html_url') AS issue_pr_url
    FROM greptile_events
    ORDER BY repo, pr_number, created_at
    """
    return query


def transform_results(rows) -> dict:
    """Transform BigQuery results to match the GitHub API output format."""
    # Group by repo -> PR
    prs_by_key = {}

    for row in rows:
        repo = row.repo

        # Determine PR number (different field for IssueCommentEvent)
        pr_number = row.pr_number or row.issue_number
        if not pr_number:
            continue

        key = (repo, pr_number)

        if key not in prs_by_key:
            # Determine PR URL
            pr_url = row.pr_url or row.issue_pr_url
            if not pr_url and repo and pr_number:
                pr_url = f"https://github.com/{repo}/pull/{pr_number}"

            prs_by_key[key] = {
                "repo": repo,
                "org": "",  # Will be filled from CSV
                "pr_number": pr_number,
                "pr_title": row.pr_title or "",
                "pr_author": row.pr_author or "",
                "pr_url": pr_url or "",
                "pr_created_at": row.pr_created_at or "",
                "pr_state": row.pr_state or "unknown",
                "greptile_comments": [],
                "fetched_at": datetime.now(timezone.utc).isoformat()
            }

        # Add comment based on event type
        if row.event_type == "PullRequestReviewCommentEvent" and row.comment_id:
            prs_by_key[key]["greptile_comments"].append({
                "comment_id": row.comment_id,
                "comment_body": row.comment_body or "",
                "comment_url": row.comment_url or "",
                "created_at": row.comment_created_at or row.created_at.isoformat(),
                "updated_at": row.comment_created_at or row.created_at.isoformat(),
                "file_path": row.file_path,
                "line_number": row.line_number,
                "diff_hunk": row.diff_hunk,
                "comment_type": "review_comment"
            })
        elif row.event_type == "PullRequestReviewEvent" and row.review_id and row.review_body:
            prs_by_key[key]["greptile_comments"].append({
                "comment_id": row.review_id,
                "comment_body": row.review_body,
                "comment_url": row.review_url or "",
                "created_at": row.review_submitted_at or row.created_at.isoformat(),
                "updated_at": row.review_submitted_at or row.created_at.isoformat(),
                "file_path": None,
                "line_number": None,
                "diff_hunk": None,
                "comment_type": "review_body"
            })
        elif row.event_type == "IssueCommentEvent" and row.comment_id:
            # Only include if it's a PR comment (has PR URL)
            if row.issue_pr_url:
                prs_by_key[key]["greptile_comments"].append({
                    "comment_id": row.comment_id,
                    "comment_body": row.comment_body or "",
                    "comment_url": row.comment_url or "",
                    "created_at": row.comment_created_at or row.created_at.isoformat(),
                    "updated_at": row.comment_created_at or row.created_at.isoformat(),
                    "file_path": None,
                    "line_number": None,
                    "diff_hunk": None,
                    "comment_type": "issue_comment"
                })

    # Filter out PRs with no comments and deduplicate comments
    results = []
    for pr_data in prs_by_key.values():
        if pr_data["greptile_comments"]:
            # Deduplicate comments by comment_id
            seen_ids = set()
            unique_comments = []
            for comment in pr_data["greptile_comments"]:
                if comment["comment_id"] not in seen_ids:
                    seen_ids.add(comment["comment_id"])
                    unique_comments.append(comment)
            pr_data["greptile_comments"] = unique_comments
            results.append(pr_data)

    return results


def add_org_info(results: list, csv_path: str) -> list:
    """Add org info from CSV to results."""
    repos = parse_repos_csv(csv_path)
    org_map = {r.repo: r.org for r in repos}

    for pr in results:
        pr["org"] = org_map.get(pr["repo"], "")

    return results


def write_output(results: list, output_file: str) -> None:
    """Write results to JSON file."""
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    total_comments = sum(len(pr["greptile_comments"]) for pr in results)

    output = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": "bigquery_backfill",
            "total_prs_with_comments": len(results),
            "total_greptile_comments": total_comments
        },
        "comments": results
    }

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    logging.info(f"Wrote {len(results)} PRs with {total_comments} comments to {output_file}")


def initialize_state(repos: list, state_file: str) -> None:
    """Initialize state with current timestamp so API script continues from here."""
    state_manager = StateManager(state_file)

    now = datetime.now(timezone.utc)
    for repo in repos:
        state_manager.update_state(RepoState(
            repo=repo.repo,
            last_checked=now,
            last_pr_number=None,
            error_count=0
        ))

    state_manager.save()
    logging.info(f"Initialized state for {len(repos)} repos at {now.isoformat()}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill historical Greptile comments from BigQuery"
    )
    parser.add_argument(
        "--csv",
        default="oss_master.csv",
        help="Path to CSV file with repo list"
    )
    parser.add_argument(
        "--output",
        default="output/greptile_comments_backfill.json",
        help="Output JSON file path"
    )
    parser.add_argument(
        "--state-file",
        default="state/last_checked.json",
        help="State file to initialize for API script"
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=365,
        help="How many days of history to fetch"
    )
    parser.add_argument(
        "--project",
        default=None,
        help="GCP project ID (uses default if not specified)"
    )
    parser.add_argument(
        "--repos",
        nargs="+",
        help="Specific repos to backfill (e.g., owner/repo). If not specified, uses all from CSV"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print query without executing"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    args = parser.parse_args()

    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    # Load repos from CSV
    all_repos = parse_repos_csv(args.csv)

    # Filter to specific repos if provided
    if args.repos:
        repos = [r for r in all_repos if r.repo in args.repos]
        repo_names = args.repos
        logger.info(f"Backfilling {len(repo_names)} specific repos: {repo_names}")
    else:
        repos = all_repos
        repo_names = [r.repo for r in repos]
        logger.info(f"Loaded {len(repo_names)} repos from {args.csv}")

    # Build query
    query = build_query(repo_names, args.days_back)

    if args.dry_run:
        print("\n=== BigQuery SQL ===\n")
        print(query)
        print(f"\n=== Would query {len(repo_names)} repos, {args.days_back} days back ===\n")
        return

    # Execute query
    logger.info(f"Executing BigQuery query ({args.days_back} days back)...")
    client = bigquery.Client(project=args.project)

    query_job = client.query(query)
    rows = list(query_job.result())
    logger.info(f"Query returned {len(rows)} events")

    # Transform results
    results = transform_results(rows)
    results = add_org_info(results, args.csv)

    # Write output
    write_output(results, args.output)

    # Initialize state for API script
    initialize_state(repos, args.state_file)

    logger.info("Backfill complete! API script will now fetch new PRs from this point forward.")


if __name__ == "__main__":
    main()

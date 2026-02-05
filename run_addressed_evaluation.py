#!/usr/bin/env python3
"""Evaluate ADDRESSED Greptile comments using the two-phase approach.

Phase 1: Query DB for addressed comments we haven't evaluated yet
Phase 2: Fetch GitHub context (diff, replies) and run LLM evaluation

This ensures we only evaluate comments that developers actually acted on,
giving us high-signal data for improving the judge.

Usage:
    python run_addressed_evaluation.py [--limit 100] [--min-score 8] [--sync-sheets]

Environment variables required:
    GREPTILE_DB_URL - PostgreSQL connection string for Greptile DB
    GITHUB_TOKEN - GitHub personal access token
    ANTHROPIC_API_KEY - Anthropic API key for Claude
"""

import argparse
import csv
import logging
import os
import sys
from datetime import datetime, timezone
from typing import List

from src.db_enrichment import EvaluatedCommentsState, fetch_new_addressed_comments
from src.github_client import GitHubClient
from src.llm_evaluator import LLMEvaluator


def load_oss_repos(csv_path: str = "oss_master.csv") -> List[str]:
    """Load list of OSS repo names from CSV."""
    repos = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                repo = row.get("repo", "").strip()
                if repo:
                    repos.append(repo)
    except FileNotFoundError:
        logging.warning(f"OSS repos file not found: {csv_path}")
    return repos


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )


def write_results_csv(
    catches: list,
    output_file: str = "output/quality_catches.csv"
) -> int:
    """Append quality catches to CSV."""
    if not catches:
        logging.info("No catches to write")
        return 0

    fieldnames = [
        "repo",
        "pr_number",
        "pr_title",
        "pr_url",
        "comment_body",
        "comment_url",
        "reply_body",
        "created_at",
        "bug_category",
        "severity",
        "quality_score",
        "llm_reasoning",
        "evaluated_at"
    ]

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Check if file exists to determine if we need header
    file_exists = os.path.exists(output_file)

    with open(output_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        for catch in catches:
            row = {k: catch.get(k, "") for k in fieldnames}
            row["evaluated_at"] = datetime.now(timezone.utc).isoformat()
            writer.writerow(row)

    logging.info(f"Appended {len(catches)} catches to {output_file}")
    return len(catches)


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate addressed Greptile comments (two-phase approach)"
    )
    parser.add_argument(
        "--limit", type=int, default=100,
        help="Maximum new comments to process per run (default: 100)"
    )
    parser.add_argument(
        "--min-score", type=int, default=8,
        help="Minimum quality score to include (default: 8)"
    )
    parser.add_argument(
        "--output", type=str, default="output/quality_catches.csv",
        help="Output CSV path"
    )
    parser.add_argument(
        "--state-file", type=str, default="state/evaluated_comments.json",
        help="State file for tracking evaluated comments"
    )
    parser.add_argument(
        "--sync-sheets", action="store_true",
        help="Sync results to Google Sheets"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose logging"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch and enrich but don't evaluate (for testing)"
    )
    parser.add_argument(
        "--repos-csv", type=str, default="oss_master.csv",
        help="CSV file with list of OSS repos to monitor"
    )
    parser.add_argument(
        "--all-repos", action="store_true",
        help="Evaluate all repos, not just OSS repos from CSV"
    )
    args = parser.parse_args()

    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    # Check required env vars
    missing_vars = []
    if not os.environ.get("GREPTILE_DB_URL"):
        missing_vars.append("GREPTILE_DB_URL")
    if not os.environ.get("GITHUB_TOKEN"):
        missing_vars.append("GITHUB_TOKEN")
    if not os.environ.get("ANTHROPIC_API_KEY") and not args.dry_run:
        missing_vars.append("ANTHROPIC_API_KEY")

    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

    # Load state
    state = EvaluatedCommentsState(args.state_file)
    state.load()

    logger.info(f"Last check: {state.last_check or 'never'}")
    logger.info(f"Already evaluated: {len(state.evaluated_ids)} comments")

    # Load OSS repos to filter
    oss_repos = None
    if not args.all_repos:
        oss_repos = load_oss_repos(args.repos_csv)
        if oss_repos:
            logger.info(f"Filtering to {len(oss_repos)} OSS repos from {args.repos_csv}")
        else:
            logger.warning(f"No repos loaded from {args.repos_csv}, will fetch all repos")

    # Phase 1: Fetch new addressed comments from DB
    logger.info("Phase 1: Fetching new addressed comments from DB...")
    new_comments = fetch_new_addressed_comments(
        evaluated_state=state,
        limit=args.limit,
        repos=oss_repos
    )

    if not new_comments:
        logger.info("No new addressed comments to evaluate")
        state.update_last_check()
        state.save()
        return

    logger.info(f"Found {len(new_comments)} new addressed comments")

    # Phase 2: Enrich with GitHub context
    logger.info("Phase 2: Enriching with GitHub context...")
    github = GitHubClient()
    enriched_comments = github.enrich_comments_batch(new_comments)

    logger.info(f"Enriched {len(enriched_comments)} comments with GitHub context")

    if args.dry_run:
        logger.info("Dry run - skipping LLM evaluation")
        for comment in enriched_comments[:3]:
            print(f"\n--- {comment.get('repo')} PR#{comment.get('pr_number')} ---")
            print(f"File: {comment.get('file_path')}")
            print(f"Body: {comment.get('comment_body', '')[:200]}...")
            print(f"Has patch: {bool(comment.get('file_patch'))}")
            print(f"Has reply: {bool(comment.get('reply_body'))}")
        return

    # Phase 3: LLM Evaluation
    logger.info(f"Phase 3: Evaluating with LLM (min_score={args.min_score})...")
    evaluator = LLMEvaluator(min_quality_score=args.min_score)
    quality_catches = evaluator.evaluate_addressed_comments(enriched_comments)

    # Mark all processed comments as evaluated (even if they didn't pass the threshold)
    comment_ids = [c.get("comment_id") for c in enriched_comments if c.get("comment_id")]
    state.mark_evaluated_batch(comment_ids)
    state.update_last_check()
    state.save()

    # Write results
    if quality_catches:
        write_results_csv(quality_catches, args.output)

    # Sync to sheets if requested
    if args.sync_sheets and quality_catches:
        try:
            from src.sheets_sync import SheetsSync
            sync = SheetsSync()
            sync.sync_quality_catches(quality_catches)
            logger.info("Synced to Google Sheets")
        except Exception as e:
            logger.warning(f"Failed to sync to sheets: {e}")

    # Print summary
    print(f"\n{'='*60}")
    print("EVALUATION SUMMARY")
    print(f"{'='*60}")
    print(f"New addressed comments found: {len(new_comments)}")
    print(f"Enriched with GitHub context: {len(enriched_comments)}")
    print(f"Showcase-worthy catches (score >= {args.min_score}): {len(quality_catches)}")
    print(f"Total evaluated (all time): {len(state.evaluated_ids)}")
    print(f"Output: {args.output}")

    if quality_catches:
        print(f"\nTop catches:")
        for catch in sorted(quality_catches, key=lambda x: x.get('quality_score', 0), reverse=True)[:5]:
            print(f"  [{catch.get('quality_score')}/10] {catch.get('repo')} PR#{catch.get('pr_number')}")
            print(f"    {catch.get('bug_category')} ({catch.get('severity')})")
            print(f"    {catch.get('llm_reasoning', '')[:80]}...")


if __name__ == "__main__":
    main()

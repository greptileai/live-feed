"""Main entry point for Greptile comment monitor."""

import argparse
import json
import logging
import os
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from .comment_fetcher import CommentFetcher
from .csv_output import append_evaluated_comments_csv, write_comments_csv
from .csv_parser import filter_active_repos, parse_repos_csv
from .github_client import GitHubClient
from .models import PRWithGreptileComments
from .state_manager import StateManager


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )


def serialize_pr_comments(pr: PRWithGreptileComments) -> dict:
    """Convert dataclass to JSON-serializable dict."""
    data = asdict(pr)
    data["pr_created_at"] = pr.pr_created_at.isoformat()
    data["fetched_at"] = pr.fetched_at.isoformat()
    for comment in data["greptile_comments"]:
        comment["created_at"] = comment["created_at"].isoformat()
        comment["updated_at"] = comment["updated_at"].isoformat()
    return data


def write_output(
    results: List[PRWithGreptileComments],
    output_file: str,
    repos_processed: int
) -> None:
    """Write results to JSON file."""
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    total_comments = sum(len(pr.greptile_comments) for pr in results)

    output = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_repos_processed": repos_processed,
            "total_prs_with_comments": len(results),
            "total_greptile_comments": total_comments
        },
        "comments": [serialize_pr_comments(pr) for pr in results]
    }

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, default=str)

    logging.info(
        f"Wrote {len(results)} PRs with {total_comments} comments to {output_file}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Monitor GitHub repos for Greptile comments"
    )
    parser.add_argument(
        "--csv",
        default="oss_master.csv",
        help="Path to CSV file with repo list"
    )
    parser.add_argument(
        "--output",
        default="output/greptile_comments.json",
        help="Output JSON file path"
    )
    parser.add_argument(
        "--state-file",
        default="state/last_checked.json",
        help="State file for tracking progress"
    )
    parser.add_argument(
        "--min-reviews",
        type=int,
        default=0,
        help="Minimum reviews_30d to process a repo"
    )
    parser.add_argument(
        "--max-repos",
        type=int,
        default=None,
        help="Maximum repos to process (for testing)"
    )
    parser.add_argument(
        "--evaluate",
        action="store_true",
        help="Run LLM evaluation on low-confidence comments"
    )
    parser.add_argument(
        "--max-score",
        type=int,
        default=3,
        help="Only evaluate comments with score <= this value"
    )
    parser.add_argument(
        "--sync-sheets",
        action="store_true",
        help="Sync quality catches to Google Sheets"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    args = parser.parse_args()

    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    try:
        client = GitHubClient()
    except ValueError as e:
        logger.error(f"GitHub client initialization failed: {e}")
        sys.exit(1)

    fetcher = CommentFetcher(client)
    state_manager = StateManager(args.state_file)
    state_manager.load()

    repos = parse_repos_csv(args.csv)
    repos = filter_active_repos(repos, args.min_reviews)

    if args.max_repos:
        repos = repos[:args.max_repos]
        logger.info(f"Limited to {args.max_repos} repos for testing")

    all_results: List[PRWithGreptileComments] = []
    repos_processed = 0

    for i, repo_config in enumerate(repos):
        logger.info(f"Processing {i+1}/{len(repos)}: {repo_config.repo}")

        if state_manager.should_skip_repo(repo_config.repo):
            continue

        repo_state = state_manager.get_state(repo_config.repo)

        results, new_state = fetcher.fetch_greptile_comments_for_repo(
            repo_config, repo_state
        )

        state_manager.update_state(new_state)
        all_results.extend(results)
        repos_processed += 1

        if repos_processed % 50 == 0:
            state_manager.save()
            logger.info(f"Checkpoint: saved state after {repos_processed} repos")

    state_manager.save()
    write_output(all_results, args.output, repos_processed)

    # Write comments to CSV
    write_comments_csv(all_results, "output/new_comments.csv")

    logger.info(
        f"Done! Processed {repos_processed} repos, "
        f"found {len(all_results)} PRs with Greptile comments"
    )

    # LLM Evaluation step
    if args.evaluate and all_results:
        logger.info("Running LLM evaluation on comments...")
        try:
            from .llm_evaluator import LLMEvaluator
            evaluator = LLMEvaluator()
            quality_catches = evaluator.evaluate_comments(
                all_results, max_score=args.max_score
            )

            if quality_catches:
                append_evaluated_comments_csv(
                    quality_catches, "output/quality_catches.csv"
                )
                logger.info(f"Found {len(quality_catches)} quality bug catches")
        except ValueError as e:
            logger.error(f"LLM evaluator failed: {e}")
            logger.info("Set ANTHROPIC_API_KEY to enable LLM evaluation")

    # Google Sheets sync step
    if args.sync_sheets:
        logger.info("Syncing to Google Sheets...")
        try:
            from .sheets_sync import SheetsSync
            syncer = SheetsSync()
            syncer.sync_quality_catches()
            logger.info("Sheets sync complete")
        except Exception as e:
            logger.error(f"Sheets sync failed: {e}")


if __name__ == "__main__":
    main()

"""Run LLM evaluation on existing greptile_comments.json output."""

import json
import logging
from datetime import datetime
from pathlib import Path

from src.models import GreptileComment, PRWithGreptileComments
from src.llm_evaluator import LLMEvaluator
from src.csv_output import append_quality_prs_csv


def load_results(json_file: str) -> list[PRWithGreptileComments]:
    """Load PR results from JSON file."""
    with open(json_file, "r") as f:
        data = json.load(f)

    results = []
    for pr_data in data["comments"]:
        comments = []
        for c in pr_data["greptile_comments"]:
            comments.append(GreptileComment(
                comment_id=c["comment_id"],
                comment_body=c["comment_body"],
                comment_url=c["comment_url"],
                created_at=datetime.fromisoformat(c["created_at"]),
                updated_at=datetime.fromisoformat(c["updated_at"]),
                file_path=c.get("file_path"),
                line_number=c.get("line_number"),
                diff_hunk=c.get("diff_hunk"),
                comment_type=c["comment_type"],
                score=c.get("score")
            ))

        # Handle pr_updated_at - use pr_created_at as fallback for older data
        pr_updated_at = pr_data.get("pr_updated_at") or pr_data["pr_created_at"]

        results.append(PRWithGreptileComments(
            repo=pr_data["repo"],
            org=pr_data.get("org", ""),
            pr_number=pr_data["pr_number"],
            pr_title=pr_data["pr_title"],
            pr_author=pr_data["pr_author"],
            pr_url=pr_data["pr_url"],
            pr_created_at=datetime.fromisoformat(pr_data["pr_created_at"]),
            pr_updated_at=datetime.fromisoformat(pr_updated_at),
            pr_state=pr_data["pr_state"],
            greptile_comments=comments,
            fetched_at=datetime.fromisoformat(pr_data["fetched_at"]),
            head_sha=pr_data.get("head_sha"),
            trigger_type=pr_data.get("trigger_type", "new_pr")
        ))

    return results


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)

    # Load existing results
    json_file = "output/greptile_comments.json"
    logger.info(f"Loading results from {json_file}")
    results = load_results(json_file)
    logger.info(f"Loaded {len(results)} PRs")

    # Run evaluation at PR level
    evaluator = LLMEvaluator()
    quality_prs = evaluator.evaluate_prs(results, max_score=3)

    if quality_prs:
        append_quality_prs_csv(quality_prs, "output/quality_prs.csv")
        logger.info(f"Found {len(quality_prs)} PRs with meaningful catches")

        # Print results
        for pr in quality_prs:
            print(f"\n{'='*60}")
            print(f"Repo: {pr['repo']} PR#{pr['pr_number']}")
            print(f"Title: {pr['pr_title']}")
            print(f"URL: {pr['pr_url']}")
            print(f"Summary: {pr['summary']}")
            for catch in pr['meaningful_catches']:
                print(f"  - [{catch['bug_category']}] ({catch['severity']})")

        # Sync to Google Sheets
        try:
            from src.sheets_sync import SheetsSync
            syncer = SheetsSync()
            synced = syncer.sync_quality_prs()
            logger.info(f"Synced {synced} PRs to Google Sheets")
        except Exception as e:
            logger.warning(f"Sheets sync failed: {e}")
    else:
        logger.info("No PRs with meaningful catches found")


if __name__ == "__main__":
    main()

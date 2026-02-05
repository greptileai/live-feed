#!/usr/bin/env python3
"""Re-evaluate existing sheet data with the new calibrated evaluator.

This script:
1. Loads existing data from output/google_sheet_data.csv
2. Re-evaluates with the new calibrated LLM judge (min_score=8)
3. Groups by PR and deduplicates
4. Outputs only quality catches that score 8+

Usage:
    python reevaluate_sheet.py [--sync-sheets]
"""

import argparse
import csv
import logging
import os
import sys
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_existing_data(csv_path: str = "output/google_sheet_data.csv") -> list:
    """Load existing evaluated comments from CSV."""
    comments = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            comments.append({
                "repo": row.get("repo", "").strip(),
                "pr_number": row.get("pr_number", "").strip(),
                "pr_title": row.get("pr_title", "").strip(),
                "pr_url": row.get("pr_url", "").strip(),
                "comment_body": row.get("comment_body", "").strip(),
                "comment_url": row.get("comment_url", "").strip(),
                "reply_body": row.get("reply_body", "").strip(),
                "created_at": row.get("created_at", "").strip(),
                "file_path": row.get("file_path", "").strip() or None,
                "line_number": row.get("line_number", "").strip() or None,
            })
    return comments


def write_results_csv(catches: list, output_file: str) -> int:
    """Write quality catches to CSV."""
    if not catches:
        logger.info("No catches to write")
        return 0

    fieldnames = [
        "repo", "pr_number", "pr_title", "pr_url",
        "comment_body", "comment_url", "reply_body", "created_at",
        "bug_category", "severity", "quality_score", "llm_reasoning",
        "evaluated_at"
    ]

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for catch in catches:
            row = {k: catch.get(k, "") for k in fieldnames}
            row["evaluated_at"] = datetime.now(timezone.utc).isoformat()
            writer.writerow(row)

    logger.info(f"Wrote {len(catches)} catches to {output_file}")
    return len(catches)


def main():
    parser = argparse.ArgumentParser(
        description="Re-evaluate existing sheet data with new calibrated evaluator"
    )
    parser.add_argument(
        "--input", type=str, default="output/google_sheet_data.csv",
        help="Input CSV with existing data"
    )
    parser.add_argument(
        "--output", type=str, default="output/quality_catches_new.csv",
        help="Output CSV for quality catches"
    )
    parser.add_argument(
        "--min-score", type=int, default=8,
        help="Minimum quality score (default: 8)"
    )
    parser.add_argument(
        "--sync-sheets", action="store_true",
        help="Sync results to Google Sheets (after clearing)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be done without making changes"
    )
    args = parser.parse_args()

    # Check API key
    if not os.environ.get("ANTHROPIC_API_KEY"):
        logger.error("ANTHROPIC_API_KEY required")
        sys.exit(1)

    # Load existing data
    logger.info(f"Loading existing data from {args.input}...")
    comments = load_existing_data(args.input)
    logger.info(f"Loaded {len(comments)} comments")

    if args.dry_run:
        # Group by PR to show what would be evaluated
        pr_groups = {}
        for c in comments:
            key = f"{c['repo']}#{c['pr_number']}"
            pr_groups[key] = pr_groups.get(key, 0) + 1

        print(f"\nWould evaluate {len(comments)} comments from {len(pr_groups)} PRs")
        print(f"PRs with multiple comments (will be deduplicated):")
        for key, count in sorted(pr_groups.items(), key=lambda x: -x[1]):
            if count > 1:
                print(f"  {key}: {count} comments")
        return

    # Re-evaluate with new calibrated evaluator
    from src.llm_evaluator import LLMEvaluator
    evaluator = LLMEvaluator(min_quality_score=args.min_score)

    logger.info(f"Re-evaluating with min_score={args.min_score}...")
    quality_catches = evaluator.evaluate_addressed_comments(comments)

    # Write results
    write_results_csv(quality_catches, args.output)

    # Summary
    print(f"\n{'='*60}")
    print("RE-EVALUATION SUMMARY")
    print(f"{'='*60}")
    print(f"Input comments: {len(comments)}")
    print(f"Quality catches (score >= {args.min_score}): {len(quality_catches)}")
    print(f"Output: {args.output}")

    if quality_catches:
        print(f"\nScore distribution:")
        score_counts = {}
        for c in quality_catches:
            score = c.get('quality_score', 0)
            score_counts[score] = score_counts.get(score, 0) + 1
        for score in sorted(score_counts.keys(), reverse=True):
            print(f"  {score}: {score_counts[score]}")

        print(f"\nCategory distribution:")
        cat_counts = {}
        for c in quality_catches:
            cat = c.get('bug_category', 'unknown')
            cat_counts[cat] = cat_counts.get(cat, 0) + 1
        for cat, count in sorted(cat_counts.items(), key=lambda x: -x[1]):
            print(f"  {cat}: {count}")

    # Sync to sheets if requested
    if args.sync_sheets and quality_catches:
        try:
            from src.sheets_sync import SheetsSync
            sync = SheetsSync()

            # Clear and re-sync
            logger.info("Syncing to Google Sheets...")
            sync.clear_and_sync(quality_catches)
            logger.info("Done!")
        except Exception as e:
            logger.error(f"Failed to sync to sheets: {e}")
            raise


if __name__ == "__main__":
    main()

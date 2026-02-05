#!/usr/bin/env python3
"""Add titles to existing quality catches without re-evaluating scores.

Reads quality_catches.csv, generates a short title for each catch via LLM,
and updates the CSV + Google Sheet.

Usage:
    python add_titles.py [--sync-sheets]
"""

import csv
import json
import logging
import os
import sys
from datetime import datetime, timezone

import anthropic

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

INPUT_CSV = "output/quality_catches.csv"
OUTPUT_CSV = "output/quality_catches.csv"


def generate_title(client: anthropic.Anthropic, comment_body: str, bug_category: str) -> str:
    """Generate a short title for a catch based on the comment body."""
    prompt = f"""Given this code review comment that caught a bug, generate a short title (5-10 words) that describes the bug caught. The title should be specific and technical, not generic.

Bug category: {bug_category}

Comment:
{comment_body[:1500]}

Respond with ONLY the title text, nothing else. No quotes, no prefix."""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=60,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.content[0].text.strip().strip('"')


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--sync-sheets", action="store_true")
    args = parser.parse_args()

    if not os.environ.get("ANTHROPIC_API_KEY"):
        logger.error("ANTHROPIC_API_KEY required")
        sys.exit(1)

    client = anthropic.Anthropic()

    # Read existing catches
    with open(INPUT_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        catches = list(reader)

    logger.info(f"Loaded {len(catches)} catches")

    # Generate titles
    for i, catch in enumerate(catches):
        logger.info(f"Generating title {i+1}/{len(catches)}: {catch['repo']} PR#{catch['pr_number']}")
        title = generate_title(client, catch.get("comment_body", ""), catch.get("bug_category", ""))
        catch["title"] = title
        logger.info(f"  -> {title}")

    # Write updated CSV
    fieldnames = [
        "repo", "pr_number", "pr_title", "pr_url",
        "comment_body", "comment_url", "reply_body", "created_at",
        "title", "bug_category", "severity", "quality_score", "llm_reasoning",
        "evaluated_at"
    ]

    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for catch in catches:
            row = {k: catch.get(k, "") for k in fieldnames}
            writer.writerow(row)

    logger.info(f"Wrote {len(catches)} catches with titles to {OUTPUT_CSV}")

    # Sync to sheets
    if args.sync_sheets:
        from src.sheets_sync import SheetsSync
        sync = SheetsSync()
        sync.clear_and_sync(catches)
        logger.info("Synced to Google Sheets")

    # Print sample
    print(f"\nSample titles:")
    for catch in catches[:5]:
        print(f"  [{catch['quality_score']}] {catch['title']}")


if __name__ == "__main__":
    main()

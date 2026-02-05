#!/usr/bin/env python3
"""Test the LLM evaluator against the golden set of manually-scored comments.

Compares LLM quality_score vs human score to identify:
- Calibration issues (LLM consistently higher/lower)
- Disagreements (where to investigate prompt improvements)
- Agreement rate at the 8+ threshold
"""

import csv
import logging
import os
import sys
from typing import Dict, List, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_golden_set(golden_csv: str = "output/comment_scores.csv") -> List[Dict]:
    """Load manually-scored comments."""
    comments = []
    with open(golden_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            score_str = row.get("score", "").strip()
            if score_str:
                try:
                    score = int(score_str)
                    comments.append({
                        "comment_url": row.get("comment_url", "").strip(),
                        "human_score": score,
                        "justification": row.get("justification", row.get(" justificationhu", "")).strip()
                    })
                except ValueError:
                    pass
    return comments


def load_full_comments(sheet_csv: str = "output/google_sheet_data.csv") -> Dict[str, Dict]:
    """Load full comment data from sheet export, indexed by comment_url."""
    comments = {}
    with open(sheet_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("comment_url", "").strip()
            if url:
                comments[url] = row
    return comments


def evaluate_comment(comment_data: Dict, evaluator) -> Optional[Dict]:
    """Run LLM evaluation on a single comment."""
    result = evaluator.evaluate_addressed_comment({
        "repo": comment_data.get("repo", ""),
        "pr_number": comment_data.get("pr_number", ""),
        "pr_title": comment_data.get("pr_title", ""),
        "pr_url": comment_data.get("pr_url", ""),
        "comment_body": comment_data.get("comment_body", ""),
        "comment_url": comment_data.get("comment_url", ""),
        "file_path": None,
        "line_number": None,
        "created_at": comment_data.get("created_at", "")
    })
    return result


def main():
    # Check for API key
    if not os.environ.get("ANTHROPIC_API_KEY"):
        logger.error("ANTHROPIC_API_KEY required")
        sys.exit(1)

    # Load data
    logger.info("Loading golden set...")
    golden_set = load_golden_set()
    logger.info(f"Loaded {len(golden_set)} scored comments")

    logger.info("Loading full comment data...")
    full_comments = load_full_comments()
    logger.info(f"Loaded {len(full_comments)} comments from sheet")

    # Match golden set with full data
    matched = []
    for item in golden_set:
        url = item["comment_url"]
        if url in full_comments:
            matched.append({
                **item,
                **full_comments[url]
            })
        else:
            logger.warning(f"No match for: {url[:80]}...")

    logger.info(f"Matched {len(matched)} comments")

    if not matched:
        logger.error("No comments to evaluate")
        sys.exit(1)

    # Initialize evaluator (with min_score=1 to get all scores, not just 8+)
    from src.llm_evaluator import LLMEvaluator
    evaluator = LLMEvaluator(min_quality_score=1)

    # Evaluate each comment
    results = []
    for i, comment in enumerate(matched):
        logger.info(f"Evaluating {i+1}/{len(matched)}: {comment['comment_url'][:60]}...")

        result = evaluate_comment(comment, evaluator)

        if result:
            llm_score = result.get("quality_score", 0)
        else:
            llm_score = None

        results.append({
            "comment_url": comment["comment_url"],
            "human_score": comment["human_score"],
            "llm_score": llm_score,
            "human_justification": comment.get("justification", ""),
            "llm_reasoning": result.get("llm_reasoning", "") if result else "",
            "bug_category": result.get("bug_category", "") if result else "",
            "severity": result.get("severity", "") if result else ""
        })

        # Print comparison
        diff = (llm_score - comment["human_score"]) if llm_score else None
        status = "✓" if diff is not None and abs(diff) <= 1 else "✗" if diff else "?"
        logger.info(f"  Human: {comment['human_score']}, LLM: {llm_score}, Diff: {diff} {status}")

    # Write results
    output_file = "output/evaluator_comparison.csv"
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "comment_url", "human_score", "llm_score", "diff",
            "human_justification", "llm_reasoning", "bug_category", "severity"
        ])
        writer.writeheader()
        for r in results:
            r["diff"] = (r["llm_score"] - r["human_score"]) if r["llm_score"] else None
            writer.writerow(r)

    logger.info(f"Results written to {output_file}")

    # Print summary statistics
    print("\n" + "="*60)
    print("EVALUATION COMPARISON SUMMARY")
    print("="*60)

    valid_results = [r for r in results if r["llm_score"] is not None]
    if valid_results:
        diffs = [r["llm_score"] - r["human_score"] for r in valid_results]
        avg_diff = sum(diffs) / len(diffs)

        exact_match = sum(1 for d in diffs if d == 0)
        within_1 = sum(1 for d in diffs if abs(d) <= 1)
        within_2 = sum(1 for d in diffs if abs(d) <= 2)

        # Threshold agreement (both >= 8 or both < 8)
        threshold_agree = sum(
            1 for r in valid_results
            if (r["human_score"] >= 8) == (r["llm_score"] >= 8)
        )

        print(f"Total evaluated: {len(valid_results)}")
        print(f"Average diff (LLM - Human): {avg_diff:+.2f}")
        print(f"Exact match: {exact_match} ({100*exact_match/len(valid_results):.1f}%)")
        print(f"Within ±1: {within_1} ({100*within_1/len(valid_results):.1f}%)")
        print(f"Within ±2: {within_2} ({100*within_2/len(valid_results):.1f}%)")
        print(f"Threshold agreement (8+): {threshold_agree} ({100*threshold_agree/len(valid_results):.1f}%)")

        print("\n--- Score Distribution ---")
        for score in range(1, 11):
            human_count = sum(1 for r in valid_results if r["human_score"] == score)
            llm_count = sum(1 for r in valid_results if r["llm_score"] == score)
            print(f"  {score}: Human={human_count}, LLM={llm_count}")

        print("\n--- Biggest Disagreements ---")
        sorted_by_diff = sorted(valid_results, key=lambda x: abs(x["llm_score"] - x["human_score"]), reverse=True)
        for r in sorted_by_diff[:5]:
            diff = r["llm_score"] - r["human_score"]
            print(f"  Human={r['human_score']}, LLM={r['llm_score']} (diff={diff:+d})")
            print(f"    {r['comment_url'][:70]}...")
            print(f"    Human: {r['human_justification'][:60]}...")
            print(f"    LLM: {r['llm_reasoning'][:60]}...")
            print()


if __name__ == "__main__":
    main()

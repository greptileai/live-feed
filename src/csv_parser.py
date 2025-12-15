"""Parse the oss_master.csv input file."""

import csv
import logging
from typing import List

from .models import RepoConfig


def parse_repos_csv(csv_path: str = "oss_master.csv") -> List[RepoConfig]:
    """Parse oss_master.csv and return list of RepoConfig objects."""
    logger = logging.getLogger(__name__)
    repos: List[RepoConfig] = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header

        for row in reader:
            if len(row) < 2:
                continue
            try:
                # Only need repo and link - other columns may have comma issues
                repo = row[0].strip()
                link = row[1].strip()
                if repo and link:
                    repos.append(RepoConfig(
                        repo=repo,
                        link=link,
                        org="",  # Skip - has comma issues in CSV
                        total_reviews=0,
                        reviews_30d=0
                    ))
            except (IndexError, ValueError) as e:
                logger.warning(f"Skipping invalid row: {row} - {e}")

    logger.info(f"Loaded {len(repos)} repos from {csv_path}")
    return repos


def filter_active_repos(
    repos: List[RepoConfig],
    min_reviews_30d: int = 0
) -> List[RepoConfig]:
    """Filter repos to only those with recent activity."""
    active = [r for r in repos if r.reviews_30d >= min_reviews_30d]
    logging.getLogger(__name__).info(
        f"Filtered to {len(active)} active repos (>= {min_reviews_30d} reviews/30d)"
    )
    return active

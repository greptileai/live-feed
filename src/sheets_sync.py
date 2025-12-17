"""Sync quality catches to Google Sheets using service account."""

import csv
import logging
import os
import time
import requests
from pathlib import Path
from typing import List, Optional, Dict

import gspread
from google.oauth2.service_account import Credentials

from .constants import GREPTILE_BOT_NAMES


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

DEFAULT_SPREADSHEET_ID = "15SoQrckKQoD_BPJkkwB0D3NwjRlIyn0W5O9UQtWVmM0"


class SheetsSync:
    """Syncs quality bug catches to Google Sheets."""

    def __init__(
        self,
        credentials_file: Optional[str] = None,
        spreadsheet_id: Optional[str] = None
    ):
        """Initialize Google Sheets client.

        Args:
            credentials_file: Path to service account JSON file.
                             Defaults to GOOGLE_CREDENTIALS_FILE env var.
            spreadsheet_id: Google Sheets spreadsheet ID.
                           Defaults to GOOGLE_SPREADSHEET_ID env var.
        """
        self.credentials_file = credentials_file or os.environ.get(
            "GOOGLE_CREDENTIALS_FILE", "credentials.json"
        )
        self.spreadsheet_id = spreadsheet_id or os.environ.get(
            "GOOGLE_SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID
        )

        self.logger = logging.getLogger(__name__)
        self._client: Optional[gspread.Client] = None
        self._spreadsheet: Optional[gspread.Spreadsheet] = None

    def _get_client(self) -> gspread.Client:
        """Get or create authenticated gspread client."""
        if self._client is None:
            creds = Credentials.from_service_account_file(
                self.credentials_file, scopes=SCOPES
            )
            self._client = gspread.authorize(creds)
        return self._client

    def _get_spreadsheet(self) -> gspread.Spreadsheet:
        """Get or open the spreadsheet."""
        if self._spreadsheet is None:
            client = self._get_client()
            self._spreadsheet = client.open_by_key(self.spreadsheet_id)
        return self._spreadsheet

    def sync_quality_catches(
        self,
        csv_file: str = "output/quality_catches.csv",
        worksheet_name: str = "Quality Catches"
    ) -> int:
        """Sync quality catches CSV to Google Sheets.

        Appends new rows to the worksheet, avoiding duplicates by comment_id.

        Returns number of new rows added.
        """
        csv_path = Path(csv_file)
        if not csv_path.exists():
            self.logger.warning(f"CSV file not found: {csv_file}")
            return 0

        # Read CSV data
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            self.logger.info("No rows to sync")
            return 0

        spreadsheet = self._get_spreadsheet()

        # Get or create worksheet
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=worksheet_name, rows=1000, cols=20
            )
            # Add header row
            headers = list(rows[0].keys())
            worksheet.append_row(headers)
            self.logger.info(f"Created new worksheet: {worksheet_name}")

        # Get existing comment IDs to avoid duplicates
        existing_data = worksheet.get_all_values()
        if existing_data:
            headers = existing_data[0]
            comment_id_idx = headers.index("comment_id") if "comment_id" in headers else None
            existing_ids = set()
            if comment_id_idx is not None:
                for row in existing_data[1:]:
                    if len(row) > comment_id_idx:
                        existing_ids.add(row[comment_id_idx])
        else:
            existing_ids = set()
            # Add headers if worksheet is empty
            headers = list(rows[0].keys())
            worksheet.append_row(headers)

        # Filter to new rows only
        new_rows = [
            row for row in rows
            if str(row.get("comment_id", "")) not in existing_ids
        ]

        if not new_rows:
            self.logger.info("No new rows to sync (all already exist)")
            return 0

        # Append new rows
        headers = list(new_rows[0].keys())
        values = [[row.get(h, "") for h in headers] for row in new_rows]
        worksheet.append_rows(values)

        self.logger.info(
            f"Synced {len(new_rows)} new rows to '{worksheet_name}'"
        )
        return len(new_rows)

    def sync_all_comments(
        self,
        csv_file: str = "output/new_comments.csv",
        worksheet_name: str = "All Comments"
    ) -> int:
        """Sync all new comments CSV to Google Sheets.

        Returns number of new rows added.
        """
        return self.sync_quality_catches(csv_file, worksheet_name)

    def sync_quality_prs(
        self,
        csv_file: str = "output/quality_prs.csv",
        worksheet_name: str = "Quality PRs"
    ) -> int:
        """Sync quality PRs CSV to Google Sheets.

        Appends new rows to the worksheet, avoiding duplicates by pr_url.

        Returns number of new rows added.
        """
        csv_path = Path(csv_file)
        if not csv_path.exists():
            self.logger.warning(f"CSV file not found: {csv_file}")
            return 0

        # Read CSV data
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            self.logger.info("No rows to sync")
            return 0

        spreadsheet = self._get_spreadsheet()

        # Get or create worksheet
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=worksheet_name, rows=1000, cols=20
            )
            # Add header row
            headers = list(rows[0].keys())
            worksheet.append_row(headers)
            self.logger.info(f"Created new worksheet: {worksheet_name}")

        # Get existing PR URLs to avoid duplicates
        existing_data = worksheet.get_all_values()
        if existing_data:
            headers = existing_data[0]
            pr_url_idx = headers.index("pr_url") if "pr_url" in headers else None
            existing_urls = set()
            if pr_url_idx is not None:
                for row in existing_data[1:]:
                    if len(row) > pr_url_idx:
                        existing_urls.add(row[pr_url_idx])
        else:
            existing_urls = set()
            # Add headers if worksheet is empty
            headers = list(rows[0].keys())
            worksheet.append_row(headers)

        # Filter to new rows only
        new_rows = [
            row for row in rows
            if str(row.get("pr_url", "")) not in existing_urls
        ]

        if not new_rows:
            self.logger.info("No new PRs to sync (all already exist)")
            return 0

        # Append new rows
        headers = list(new_rows[0].keys())
        values = [[row.get(h, "") for h in headers] for row in new_rows]
        worksheet.append_rows(values)

        self.logger.info(
            f"Synced {len(new_rows)} new PRs to '{worksheet_name}'"
        )
        return len(new_rows)

    def replace_quality_prs(
        self,
        csv_file: str = "output/quality_prs.csv",
        worksheet_name: str = "Quality PRs"
    ) -> int:
        """Replace all data in Quality PRs worksheet with CSV contents.

        Clears existing data and uploads fresh from CSV.

        Returns number of rows written.
        """
        csv_path = Path(csv_file)
        if not csv_path.exists():
            self.logger.warning(f"CSV file not found: {csv_file}")
            return 0

        # Read CSV data
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            rows = list(reader)

        if not rows:
            self.logger.info("No rows to sync")
            return 0

        spreadsheet = self._get_spreadsheet()

        # Get or create worksheet
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            # Clear existing data
            worksheet.clear()
            self.logger.info(f"Cleared existing data in '{worksheet_name}'")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=worksheet_name, rows=1000, cols=20
            )
            self.logger.info(f"Created new worksheet: {worksheet_name}")

        # Write header row
        worksheet.append_row(fieldnames)

        # Write all data rows
        values = [[row.get(h, "") for h in fieldnames] for row in rows]
        worksheet.append_rows(values)

        self.logger.info(
            f"Replaced '{worksheet_name}' with {len(rows)} rows from CSV"
        )
        return len(rows)

    def _check_rate_limit(self, github_token: str, min_remaining: int = 100) -> None:
        """Check GitHub rate limit and sleep if necessary."""
        try:
            headers = {
                "Authorization": f"Bearer {github_token}",
                "Accept": "application/vnd.github+json"
            }
            response = requests.get(
                "https://api.github.com/rate_limit",
                headers=headers,
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                remaining = data["resources"]["core"]["remaining"]
                if remaining < min_remaining:
                    reset_time = data["resources"]["core"]["reset"]
                    sleep_seconds = max(0, reset_time - time.time()) + 5
                    self.logger.warning(
                        f"Rate limit low ({remaining} remaining). "
                        f"Sleeping {sleep_seconds:.0f}s until reset."
                    )
                    time.sleep(sleep_seconds)
        except Exception as e:
            self.logger.warning(f"Failed to check rate limit: {e}")

    def _parse_pr_url(self, pr_url: str) -> Optional[tuple]:
        """Parse owner, repo, number from PR URL."""
        try:
            parts = pr_url.rstrip('/').split('/')
            number = parts[-1]
            repo = parts[-3]
            owner = parts[-4]
            return owner, repo, number
        except:
            return None

    def _get_pr_state_from_github(self, pr_url: str, github_token: str) -> Optional[str]:
        """Check current PR state from GitHub API."""
        parsed = self._parse_pr_url(pr_url)
        if not parsed:
            return None
        owner, repo, number = parsed

        try:
            api_url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{number}"
            headers = {
                "Authorization": f"Bearer {github_token}",
                "Accept": "application/vnd.github+json"
            }
            response = requests.get(api_url, headers=headers, timeout=10)

            if response.status_code == 200:
                return response.json().get("state")
            else:
                self.logger.warning(f"Failed to get PR state for {pr_url}: {response.status_code}")
                return None
        except Exception as e:
            self.logger.warning(f"Error getting PR state for {pr_url}: {e}")
            return None

    def _extract_score_from_body(self, body: str) -> Optional[int]:
        """Extract confidence score from Greptile comment body."""
        import re
        if not body:
            return None

        patterns = [
            r'[Cc]onfidence(?:\s+score)?[:\s]+(\d)/5',
            r'[Ss]core[:\s]+(\d)/5',
            r'(?:^|[\s:])(\d)/5',
        ]

        for pattern in patterns:
            match = re.search(pattern, body)
            if match:
                return int(match.group(1))
        return None

    def _get_latest_greptile_score(self, pr_url: str, github_token: str) -> Optional[int]:
        """Fetch latest Greptile score for a PR from GitHub API.

        Uses pagination to ensure we check ALL comments/reviews, not just the first page.
        """
        parsed = self._parse_pr_url(pr_url)
        if not parsed:
            return None
        owner, repo, number = parsed

        headers = {
            "Authorization": f"Bearer {github_token}",
            "Accept": "application/vnd.github+json"
        }

        greptile_logins = [u.lower() for u in GREPTILE_BOT_NAMES]
        latest_score = None
        latest_time = None

        try:
            # Check issue comments (with pagination)
            url = f"https://api.github.com/repos/{owner}/{repo}/issues/{number}/comments"
            for comment in self._fetch_paginated(url, headers):
                user_login = comment.get("user", {}).get("login", "").lower()
                if user_login in greptile_logins:
                    score = self._extract_score_from_body(comment.get("body", ""))
                    if score is not None:
                        created = comment.get("created_at", "")
                        if latest_time is None or created > latest_time:
                            latest_score = score
                            latest_time = created

            # Check reviews (with pagination)
            url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{number}/reviews"
            for review in self._fetch_paginated(url, headers):
                user_login = review.get("user", {}).get("login", "").lower()
                if user_login in greptile_logins:
                    score = self._extract_score_from_body(review.get("body", ""))
                    if score is not None:
                        submitted = review.get("submitted_at", "")
                        if latest_time is None or submitted > latest_time:
                            latest_score = score
                            latest_time = submitted

            return latest_score
        except Exception as e:
            self.logger.warning(f"Error getting Greptile score for {pr_url}: {e}")
            return None

    def _fetch_paginated(self, url: str, headers: dict) -> List[dict]:
        """Fetch all pages from a GitHub API endpoint."""
        all_items = []
        page = 1
        per_page = 100  # Max allowed by GitHub

        while True:
            paginated_url = f"{url}?per_page={per_page}&page={page}"
            try:
                response = requests.get(paginated_url, headers=headers, timeout=10)
                if response.status_code != 200:
                    break

                items = response.json()
                if not items:
                    break

                all_items.extend(items)

                # Check if there are more pages
                if len(items) < per_page:
                    break

                page += 1

                # Safety limit to prevent infinite loops
                if page > 50:
                    self.logger.warning(f"Pagination limit reached for {url}")
                    break

            except Exception as e:
                self.logger.warning(f"Pagination error for {url}: {e}")
                break

        return all_items

    def _fetch_all_greptile_comments(self, pr_url: str, github_token: str) -> List[dict]:
        """Fetch ALL Greptile comments for a PR (for re-evaluation).

        Uses pagination to ensure all comments are fetched.
        """
        parsed = self._parse_pr_url(pr_url)
        if not parsed:
            return []
        owner, repo, number = parsed

        headers = {
            "Authorization": f"Bearer {github_token}",
            "Accept": "application/vnd.github+json"
        }

        comments = []
        greptile_logins = [u.lower() for u in GREPTILE_BOT_NAMES]

        try:
            # Fetch issue comments (with pagination)
            url = f"https://api.github.com/repos/{owner}/{repo}/issues/{number}/comments"
            for comment in self._fetch_paginated(url, headers):
                user_login = comment.get("user", {}).get("login", "").lower()
                if user_login in greptile_logins:
                    comments.append({
                        "body": comment.get("body", ""),
                        "created_at": comment.get("created_at", ""),
                        "type": "issue_comment"
                    })

            # Fetch review comments - inline (with pagination)
            url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{number}/comments"
            for comment in self._fetch_paginated(url, headers):
                user_login = comment.get("user", {}).get("login", "").lower()
                if user_login in greptile_logins:
                    comments.append({
                        "body": comment.get("body", ""),
                        "created_at": comment.get("created_at", ""),
                        "file_path": comment.get("path"),
                        "diff_hunk": comment.get("diff_hunk"),
                        "type": "review_comment"
                    })

            # Fetch reviews (with pagination)
            url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{number}/reviews"
            for review in self._fetch_paginated(url, headers):
                user_login = review.get("user", {}).get("login", "").lower()
                if user_login in greptile_logins:
                    body = review.get("body", "")
                    if body:  # Only include reviews with body text
                        comments.append({
                            "body": body,
                            "created_at": review.get("submitted_at", ""),
                            "type": "review_body"
                        })

        except Exception as e:
            self.logger.warning(f"Error fetching comments for {pr_url}: {e}")

        return comments

    def _reevaluate_prs(
        self,
        pr_urls: List[str],
        pr_map: Dict[str, dict],
        github_token: str,
        fieldnames: List[str]
    ) -> None:
        """Re-evaluate PRs with score changes using LLM.

        Fetches all Greptile comments and runs LLM evaluation.
        Updates pr_map in place with new evaluation results.
        PRs that no longer qualify as great catches are removed from pr_map.
        """
        from datetime import datetime
        from zoneinfo import ZoneInfo
        PST = ZoneInfo("America/Los_Angeles")

        # Import LLM evaluator
        try:
            from .llm_evaluator import LLMEvaluator
            evaluator = LLMEvaluator()
        except Exception as e:
            self.logger.error(f"Failed to import LLMEvaluator: {e}")
            return

        prs_to_remove = []

        for pr_url in pr_urls:
            self._check_rate_limit(github_token)

            row = pr_map.get(pr_url)
            if not row:
                continue

            # Fetch all Greptile comments
            comments = self._fetch_all_greptile_comments(pr_url, github_token)
            if not comments:
                self.logger.info(f"No Greptile comments found for {pr_url}, removing from results")
                prs_to_remove.append(pr_url)
                continue

            # Build combined comment text for evaluation (include diff context)
            comment_parts = []
            for c in comments:
                part = f"[{c['type']}] {c.get('file_path', 'general')}"
                if c.get('diff_hunk'):
                    part += f"\n\nCode context:\n```\n{c['diff_hunk']}\n```"
                part += f"\n\nGreptile's comment:\n{c['body']}"
                comment_parts.append(part)
            comment_text = "\n\n---\n\n".join(comment_parts)

            # Run LLM evaluation
            try:
                result = evaluator.evaluate_single_pr_text(
                    repo=row.get("repo", ""),
                    pr_title=row.get("pr_title", ""),
                    pr_url=pr_url,
                    comment_text=comment_text
                )

                if result and result.get("is_great_catch"):
                    # Update row with new evaluation
                    catches = result.get("great_catches", [])
                    categories = list(set(c.get("bug_category", "") for c in catches if c.get("bug_category")))

                    row["summary"] = result.get("summary", "")
                    row["catch_categories"] = ", ".join(categories)
                    row["evaluated_at"] = datetime.now(PST).strftime("%Y-%m-%d %H:%M:%S")
                    self.logger.info(f"Re-evaluated {pr_url}: still a great catch")
                else:
                    # No longer a great catch
                    self.logger.info(f"Re-evaluated {pr_url}: no longer a great catch, removing")
                    prs_to_remove.append(pr_url)

            except Exception as e:
                self.logger.warning(f"Failed to re-evaluate {pr_url}: {e}")

        # Remove PRs that are no longer great catches
        for pr_url in prs_to_remove:
            del pr_map[pr_url]

        self.logger.info(f"Re-evaluation complete: {len(pr_urls) - len(prs_to_remove)} still great catches, {len(prs_to_remove)} removed")

    def refresh_and_sync_open_prs(
        self,
        csv_file: str = "output/quality_prs.csv",
        worksheet_name: str = "Quality PRs",
        github_token: Optional[str] = None,
        prs_with_new_activity: Optional[List[str]] = None
    ) -> int:
        """Refresh PR states from GitHub, re-evaluate as needed, sync open PRs.

        1. Reads CSV and deduplicates by pr_url (keeps latest by evaluated_at)
        2. Refreshes pr_state and pr_score from GitHub API
        3. Re-evaluates PRs that:
           - Had new comments this run (passed via prs_with_new_activity)
           - Have score changes detected during refresh
        4. Removes PRs that are no longer great catches
        5. Updates CSV with current states and evaluations
        6. Syncs only open PRs to Google Sheets

        Args:
            prs_with_new_activity: List of PR URLs that had new Greptile comments
                                   this run. These will be re-evaluated even if
                                   score is unchanged.

        Returns number of rows written.
        """
        github_token = github_token or os.environ.get("GITHUB_TOKEN")
        if not github_token:
            self.logger.warning("No GITHUB_TOKEN, falling back to sync_open_prs_only")
            return self.sync_open_prs_only(csv_file, worksheet_name)

        csv_path = Path(csv_file)
        if not csv_path.exists():
            self.logger.warning(f"CSV file not found: {csv_file}")
            return 0

        # Read all CSV data
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            all_rows = list(reader)

        self.logger.info(f"Loaded {len(all_rows)} total rows from CSV")

        # Deduplicate by pr_url, keeping latest (by evaluated_at)
        pr_map: Dict[str, dict] = {}
        for row in all_rows:
            pr_url = row.get("pr_url", "")
            if not pr_url:
                continue
            existing = pr_map.get(pr_url)
            if existing is None:
                pr_map[pr_url] = row
            else:
                # Keep the one with later evaluated_at
                if row.get("evaluated_at", "") > existing.get("evaluated_at", ""):
                    pr_map[pr_url] = row

        self.logger.info(f"Deduplicated to {len(pr_map)} unique PRs")

        # Refresh PR states and scores from GitHub, track score changes
        state_updated_count = 0
        score_changed_prs: List[str] = []  # PRs that need re-evaluation

        for i, (pr_url, row) in enumerate(pr_map.items()):
            # Check rate limit every 20 PRs
            if i > 0 and i % 20 == 0:
                self._check_rate_limit(github_token)

            # Refresh PR state
            current_state = self._get_pr_state_from_github(pr_url, github_token)
            if current_state and current_state != row.get("pr_state"):
                self.logger.info(f"PR state changed: {pr_url} {row.get('pr_state')} -> {current_state}")
                row["pr_state"] = current_state
                state_updated_count += 1

            # Refresh Greptile score
            current_score = self._get_latest_greptile_score(pr_url, github_token)
            if current_score is not None:
                old_score = row.get("pr_score", "")
                # Parse old score - handle both "3" and "3/5" formats
                try:
                    old_score_str = old_score.split("/")[0] if "/" in old_score else old_score
                    old_score_int = int(old_score_str) if old_score_str else None
                except (ValueError, TypeError):
                    old_score_int = None

                if old_score_int != current_score:
                    self.logger.info(f"PR score changed: {pr_url} {old_score} -> {current_score}/5")
                    row["pr_score"] = f"{current_score}/5"
                    # Only re-evaluate open PRs with score changes
                    if row.get("pr_state") == "open":
                        score_changed_prs.append(pr_url)

        self.logger.info(f"Updated {state_updated_count} PR states from GitHub")
        self.logger.info(f"Found {len(score_changed_prs)} open PRs with score changes")

        # Combine PRs that need re-evaluation:
        # 1. PRs with score changes (detected above)
        # 2. PRs with new activity this run (if they exist in CSV)
        prs_to_reevaluate = set(score_changed_prs)

        if prs_with_new_activity:
            # Only re-evaluate PRs that are in our CSV (existing great catches)
            existing_prs_with_new_activity = [
                url for url in prs_with_new_activity
                if url in pr_map and pr_map[url].get("pr_state") == "open"
            ]
            prs_to_reevaluate.update(existing_prs_with_new_activity)
            self.logger.info(
                f"Found {len(existing_prs_with_new_activity)} existing great catches with new activity"
            )

        self.logger.info(f"Total PRs to re-evaluate: {len(prs_to_reevaluate)}")

        # Re-evaluate PRs
        if prs_to_reevaluate:
            self._reevaluate_prs(list(prs_to_reevaluate), pr_map, github_token, fieldnames)

        # Write updated CSV back
        rows = list(pr_map.values())
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        self.logger.info(f"Wrote {len(rows)} rows back to CSV")

        # Filter to open PRs only (LLM evaluation is the quality filter, not Greptile score)
        open_rows = [r for r in rows if r.get("pr_state") == "open"]
        closed_count = len(rows) - len(open_rows)
        self.logger.info(f"Found {len(open_rows)} open PRs to sync")
        if closed_count > 0:
            self.logger.info(f"Excluded {closed_count} closed/merged PRs")

        if not open_rows:
            self.logger.info("No open PRs to sync")
            return 0

        spreadsheet = self._get_spreadsheet()

        # Get or create worksheet
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            worksheet.clear()
            self.logger.info(f"Cleared existing data in '{worksheet_name}'")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=worksheet_name, rows=1000, cols=20
            )
            self.logger.info(f"Created new worksheet: {worksheet_name}")

        # Write header row
        worksheet.append_row(fieldnames)

        # Write all data rows
        values = [[row.get(h, "") for h in fieldnames] for row in open_rows]
        worksheet.append_rows(values)

        self.logger.info(
            f"Synced {len(open_rows)} open PRs to '{worksheet_name}'"
        )
        return len(open_rows)

    def sync_open_prs_only(
        self,
        csv_file: str = "output/quality_prs.csv",
        worksheet_name: str = "Quality PRs"
    ) -> int:
        """Sync only open PRs to Google Sheets, removing closed ones.

        Clears existing data and uploads only PRs with pr_state='open'.
        LLM evaluation is the quality filter, not Greptile score.
        Note: Does not refresh states from GitHub. Use refresh_and_sync_open_prs for that.

        Returns number of rows written.
        """
        csv_path = Path(csv_file)
        if not csv_path.exists():
            self.logger.warning(f"CSV file not found: {csv_file}")
            return 0

        # Read CSV data and filter to open PRs only
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            rows = [r for r in reader if r.get("pr_state") == "open"]

        self.logger.info(f"Found {len(rows)} open PRs to sync")

        if not rows:
            self.logger.info("No open PRs to sync")
            return 0

        spreadsheet = self._get_spreadsheet()

        # Get or create worksheet
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            worksheet.clear()
            self.logger.info(f"Cleared existing data in '{worksheet_name}'")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=worksheet_name, rows=1000, cols=20
            )
            self.logger.info(f"Created new worksheet: {worksheet_name}")

        # Write header row
        worksheet.append_row(fieldnames)

        # Write all data rows
        values = [[row.get(h, "") for h in fieldnames] for row in rows]
        worksheet.append_rows(values)

        self.logger.info(
            f"Synced {len(rows)} open PRs to '{worksheet_name}'"
        )
        return len(rows)

    def sync_all_great_catches(
        self,
        csv_file: str = "output/quality_prs.csv",
        worksheet_name: str = "Quality PRs",
        github_token: Optional[str] = None
    ) -> int:
        """Sync ALL PRs with great catches to Google Sheets.

        Unlike refresh_and_sync_open_prs, this includes ALL PRs where the LLM
        identified meaningful bugs - regardless of PR state (open/closed) or
        Greptile score. The LLM's judgment is the filter.

        1. Reads CSV and deduplicates by pr_url (keeps latest by evaluated_at)
        2. Optionally refreshes pr_state from GitHub API (for display only)
        3. Syncs ALL rows to Google Sheets (no filtering)

        Returns number of rows written.
        """
        github_token = github_token or os.environ.get("GITHUB_TOKEN")

        csv_path = Path(csv_file)
        if not csv_path.exists():
            self.logger.warning(f"CSV file not found: {csv_file}")
            return 0

        # Read all CSV data
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            all_rows = list(reader)

        self.logger.info(f"Loaded {len(all_rows)} total rows from CSV")

        # Deduplicate by pr_url, keeping latest (by evaluated_at)
        pr_map: Dict[str, dict] = {}
        for row in all_rows:
            pr_url = row.get("pr_url", "")
            if not pr_url:
                continue
            existing = pr_map.get(pr_url)
            if existing is None:
                pr_map[pr_url] = row
            else:
                # Keep the one with later evaluated_at
                if row.get("evaluated_at", "") > existing.get("evaluated_at", ""):
                    pr_map[pr_url] = row

        self.logger.info(f"Deduplicated to {len(pr_map)} unique PRs")

        # Optionally refresh PR states from GitHub (for display purposes only)
        if github_token:
            state_updated_count = 0
            for i, (pr_url, row) in enumerate(pr_map.items()):
                # Check rate limit every 20 PRs
                if i > 0 and i % 20 == 0:
                    self._check_rate_limit(github_token)

                current_state = self._get_pr_state_from_github(pr_url, github_token)
                if current_state and current_state != row.get("pr_state"):
                    self.logger.debug(f"PR state changed: {pr_url} {row.get('pr_state')} -> {current_state}")
                    row["pr_state"] = current_state
                    state_updated_count += 1

            if state_updated_count > 0:
                self.logger.info(f"Updated {state_updated_count} PR states from GitHub")

        rows = list(pr_map.values())

        # Write updated CSV back (with refreshed states)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        self.logger.info(f"Wrote {len(rows)} rows back to CSV")

        if not rows:
            self.logger.info("No PRs to sync")
            return 0

        spreadsheet = self._get_spreadsheet()

        # Get or create worksheet
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            worksheet.clear()
            self.logger.info(f"Cleared existing data in '{worksheet_name}'")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=worksheet_name, rows=1000, cols=20
            )
            self.logger.info(f"Created new worksheet: {worksheet_name}")

        # Write header row
        worksheet.append_row(fieldnames)

        # Write all data rows
        values = [[row.get(h, "") for h in fieldnames] for row in rows]
        worksheet.append_rows(values)

        self.logger.info(
            f"Synced {len(rows)} PRs with great catches to '{worksheet_name}'"
        )
        return len(rows)

    def sync_current_run(
        self,
        quality_prs: List[dict],
        worksheet_name: str = "Quality PRs"
    ) -> int:
        """Sync only the current run's great catches to Google Sheets.

        This replaces the Sheet with ONLY the PRs from this run that were
        determined to be great catches by the LLM evaluator. Does not read
        from CSV - takes the evaluator output directly.

        Args:
            quality_prs: List of PR dicts from LLM evaluator (great catches only)
            worksheet_name: Name of the worksheet to sync to

        Returns number of rows written.
        """
        if not quality_prs:
            self.logger.info("No great catches to sync")
            return 0

        fieldnames = [
            "repo",
            "pr_url",
            "pr_number",
            "pr_title",
            "pr_author",
            "pr_created_at",
            "pr_state",
            "trigger_type",
            "pr_score",
            "catch_categories",
            "summary",
            "evaluated_at"
        ]

        # Format rows for sheet
        from datetime import datetime
        from zoneinfo import ZoneInfo
        PST = ZoneInfo("America/Los_Angeles")

        def to_pst(dt_str: str) -> str:
            if not dt_str:
                return ""
            dt = datetime.fromisoformat(dt_str)
            return dt.astimezone(PST).strftime("%Y-%m-%d %H:%M:%S")

        rows = []
        for pr in quality_prs:
            catches = pr.get("great_catches", [])
            categories = list(set(c.get("bug_category", "") for c in catches if c.get("bug_category")))
            score = pr.get("pr_score")
            score_formatted = f"{score}/5" if score is not None else ""

            rows.append({
                "repo": pr.get("repo", ""),
                "pr_url": pr.get("pr_url", ""),
                "pr_number": pr.get("pr_number", ""),
                "pr_title": pr.get("pr_title", ""),
                "pr_author": pr.get("pr_author", ""),
                "pr_created_at": to_pst(pr.get("pr_created_at", "")),
                "pr_state": pr.get("pr_state", ""),
                "trigger_type": pr.get("trigger_type", "new_pr"),
                "pr_score": score_formatted,
                "catch_categories": ", ".join(categories),
                "summary": pr.get("summary", ""),
                "evaluated_at": datetime.now(PST).strftime("%Y-%m-%d %H:%M:%S")
            })

        spreadsheet = self._get_spreadsheet()

        # Get or create worksheet
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            worksheet.clear()
            self.logger.info(f"Cleared existing data in '{worksheet_name}'")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=worksheet_name, rows=1000, cols=20
            )
            self.logger.info(f"Created new worksheet: {worksheet_name}")

        # Write header row
        worksheet.append_row(fieldnames)

        # Write data rows
        values = [[row.get(h, "") for h in fieldnames] for row in rows]
        worksheet.append_rows(values)

        self.logger.info(
            f"Synced {len(rows)} great catches from current run to '{worksheet_name}'"
        )
        return len(rows)

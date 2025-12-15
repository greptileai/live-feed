"""Sync quality catches to Google Sheets using service account."""

import csv
import logging
import os
from pathlib import Path
from typing import List, Optional

import gspread
from google.oauth2.service_account import Credentials


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

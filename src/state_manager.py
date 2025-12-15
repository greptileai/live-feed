"""Manages persistent state between runs."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from .models import RepoState


class StateManager:
    """Handles state persistence for incremental processing."""

    def __init__(self, state_file: str = "state/last_checked.json"):
        self.state_file = Path(state_file)
        self.states: Dict[str, RepoState] = {}
        self.logger = logging.getLogger(__name__)

    def load(self) -> None:
        """Load state from file."""
        if self.state_file.exists():
            with open(self.state_file) as f:
                self._load_from_json(f.read())
        else:
            self.logger.info("No existing state found, starting fresh")

    def _load_from_json(self, json_str: str) -> None:
        """Parse JSON state string."""
        try:
            data = json.loads(json_str)
            for repo, state_data in data.items():
                last_checked = state_data["last_checked"]
                if isinstance(last_checked, str):
                    last_checked = datetime.fromisoformat(last_checked)
                # Convert pr_shas keys back to int (JSON only supports string keys)
                pr_shas = state_data.get("pr_shas")
                if pr_shas:
                    pr_shas = {int(k): v for k, v in pr_shas.items()}
                self.states[repo] = RepoState(
                    repo=repo,
                    last_checked=last_checked,
                    last_pr_number=state_data.get("last_pr_number"),
                    error_count=state_data.get("error_count", 0),
                    pr_shas=pr_shas
                )
            self.logger.info(f"Loaded state for {len(self.states)} repos")
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse state JSON: {e}")

    def save(self) -> None:
        """Save state to file."""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)

        data = {}
        for repo, state in self.states.items():
            state_data = {
                "last_checked": state.last_checked.isoformat(),
                "last_pr_number": state.last_pr_number,
                "error_count": state.error_count
            }
            # Include pr_shas if present (convert int keys to strings for JSON)
            if state.pr_shas:
                state_data["pr_shas"] = {str(k): v for k, v in state.pr_shas.items()}
            data[repo] = state_data

        with open(self.state_file, "w") as f:
            json.dump(data, f, indent=2)

        self.logger.info(f"Saved state for {len(self.states)} repos")

    def get_state(self, repo: str) -> Optional[RepoState]:
        """Get state for a specific repo."""
        return self.states.get(repo)

    def update_state(self, state: RepoState) -> None:
        """Update state for a repo."""
        self.states[state.repo] = state

    def should_skip_repo(self, repo: str, max_errors: int = 5) -> bool:
        """Check if a repo should be skipped due to repeated errors."""
        state = self.states.get(repo)
        if not state:
            return False

        if state.error_count >= max_errors:
            self.logger.warning(
                f"Skipping {repo} due to {state.error_count} consecutive errors"
            )
            return True

        return False

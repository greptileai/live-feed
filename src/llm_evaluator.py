"""LLM-based evaluation of Greptile comments for meaningful bugs."""

import json
import logging
import os
from typing import Any, Dict, List, Optional

import anthropic

from .models import GreptileComment, PRWithGreptileComments


EVALUATION_PROMPT = """You are evaluating code review comments from an AI code reviewer (Greptile).
Your task is to determine if the comment identifies a MEANINGFUL BUG - something that could cause:
- Runtime errors or crashes
- Security vulnerabilities
- Data corruption or loss
- Logic errors that produce wrong results
- Performance issues that significantly impact users
- Race conditions or concurrency bugs

NOT meaningful bugs (ignore these):
- Style/formatting suggestions
- Minor refactoring suggestions
- Documentation improvements
- Naming conventions
- Code organization preferences
- "Nice to have" improvements

Analyze this code review comment:

Repository: {repo}
PR Title: {pr_title}
File: {file_path}
Line: {line_number}

Code context (diff hunk):
```
{diff_hunk}
```

Greptile's comment:
```
{comment_body}
```

Greptile's confidence score: {score}/5

Respond with JSON only:
{{
  "is_meaningful_bug": true/false,
  "bug_category": "security|logic|runtime|performance|concurrency|data_integrity|null",
  "severity": "critical|high|medium|low|null",
  "reasoning": "1-2 sentence explanation"
}}"""


SUMMARY_PROMPT = """Summarize what Greptile caught in this PR review. Be concise (1-2 sentences).

PR: {repo} #{pr_number} - {pr_title}

Greptile's catches:
{catches}

Write a brief summary of the bug(s) Greptile identified. Focus on the actual issue and its impact."""


class LLMEvaluator:
    """Evaluates Greptile comments using Claude API."""

    def __init__(self, api_key: Optional[str] = None, model: str = "claude-opus-4-5-20251101"):
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable required")

        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.model = model
        self.summary_model = "claude-sonnet-4-20250514"
        self.logger = logging.getLogger(__name__)

    def evaluate_comment(
        self,
        comment: GreptileComment,
        pr: PRWithGreptileComments
    ) -> Dict[str, Any]:
        """Evaluate a single comment for meaningful bug detection.

        Returns dict with original comment data + evaluation results.
        """
        prompt = EVALUATION_PROMPT.format(
            repo=pr.repo,
            pr_title=pr.pr_title,
            file_path=comment.file_path or "N/A",
            line_number=comment.line_number or "N/A",
            diff_hunk=comment.diff_hunk or "N/A",
            comment_body=comment.comment_body,
            score=comment.score if comment.score is not None else "N/A"
        )

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=256,
                messages=[{"role": "user", "content": prompt}]
            )

            # Parse JSON response
            response_text = response.content[0].text.strip()
            # Handle potential markdown code blocks
            if response_text.startswith("```"):
                response_text = response_text.split("```")[1]
                if response_text.startswith("json"):
                    response_text = response_text[4:]
                response_text = response_text.strip()

            evaluation = json.loads(response_text)

        except json.JSONDecodeError as e:
            self.logger.warning(f"Failed to parse LLM response: {e}")
            evaluation = {
                "is_meaningful_bug": False,
                "bug_category": None,
                "severity": None,
                "reasoning": f"Parse error: {str(e)}"
            }
        except Exception as e:
            self.logger.error(f"LLM API error: {e}")
            evaluation = {
                "is_meaningful_bug": False,
                "bug_category": None,
                "severity": None,
                "reasoning": f"API error: {str(e)}"
            }

        # Combine original data with evaluation
        return {
            "repo": pr.repo,
            "pr_number": pr.pr_number,
            "pr_title": pr.pr_title,
            "pr_url": pr.pr_url,
            "comment_id": comment.comment_id,
            "comment_type": comment.comment_type,
            "score": comment.score,
            "file_path": comment.file_path,
            "line_number": comment.line_number,
            "comment_body": comment.comment_body,
            "comment_url": comment.comment_url,
            "created_at": comment.created_at.isoformat(),
            "is_meaningful_bug": evaluation.get("is_meaningful_bug", False),
            "bug_category": evaluation.get("bug_category"),
            "severity": evaluation.get("severity"),
            "llm_reasoning": evaluation.get("reasoning", "")
        }

    def summarize_catches(
        self,
        catches: List[Dict[str, Any]],
        repo: str,
        pr_number: int,
        pr_title: str
    ) -> str:
        """Summarize multiple catches into a single cohesive explanation."""
        if not catches:
            return ""

        if len(catches) == 1:
            return catches[0].get("llm_reasoning", "")

        # Format catches for the prompt
        catches_text = "\n".join(
            f"- [{c.get('bug_category', 'unknown')}] ({c.get('severity', 'unknown')}): {c.get('llm_reasoning', '')}"
            for c in catches
        )

        prompt = SUMMARY_PROMPT.format(
            repo=repo,
            pr_number=pr_number,
            pr_title=pr_title,
            catches=catches_text
        )

        try:
            response = self.client.messages.create(
                model=self.summary_model,
                max_tokens=150,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text.strip()
        except Exception as e:
            self.logger.warning(f"Summary failed: {e}")
            # Fallback to concatenation
            return " | ".join(c.get("llm_reasoning", "") for c in catches)

    def _get_pr_score(self, pr: PRWithGreptileComments) -> Optional[int]:
        """Get the PR's confidence score from the overview comment."""
        for comment in pr.greptile_comments:
            if comment.score is not None:
                return comment.score
        return None

    def _is_skipped_comment(self, comment: GreptileComment) -> bool:
        """Check if comment is a 'Skipped' placeholder."""
        return "<!-- greptile-status -->" in comment.comment_body and "Skipped:" in comment.comment_body

    def _is_review_summary(self, comment: GreptileComment) -> bool:
        """Check if comment is just a review summary (no actionable content)."""
        body = comment.comment_body.strip()
        # Match patterns like "<sub>1 file reviewed, 2 comments</sub>"
        return body.startswith("<sub>") and "file reviewed" in body and len(body) < 300

    def evaluate_comments(
        self,
        results: List[PRWithGreptileComments],
        max_score: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Evaluate all comments, filtering by PR-level score.

        Args:
            results: List of PRs with comments
            max_score: Only evaluate PRs with confidence score <= this value (e.g., 3)

        Returns list of evaluated comments that ARE meaningful bugs.
        """
        quality_catches = []
        total_evaluated = 0
        prs_evaluated = 0

        for pr in results:
            # Get PR-level score from overview comment
            pr_score = self._get_pr_score(pr)

            # Filter by PR score if specified
            if max_score is not None:
                if pr_score is None or pr_score > max_score:
                    self.logger.debug(
                        f"Skipping {pr.repo} PR#{pr.pr_number} - score {pr_score} > {max_score}"
                    )
                    continue

            prs_evaluated += 1
            self.logger.info(
                f"Evaluating {pr.repo} PR#{pr.pr_number} (score: {pr_score}/5)"
            )

            for comment in pr.greptile_comments:
                # Skip noise comments
                if self._is_skipped_comment(comment):
                    self.logger.debug(f"Skipping 'Skipped' comment {comment.comment_id}")
                    continue

                if self._is_review_summary(comment):
                    self.logger.debug(f"Skipping review summary {comment.comment_id}")
                    continue

                total_evaluated += 1
                self.logger.debug(
                    f"Evaluating comment {comment.comment_id} from {pr.repo}"
                )

                evaluation = self.evaluate_comment(comment, pr)

                if evaluation["is_meaningful_bug"]:
                    quality_catches.append(evaluation)
                    self.logger.info(
                        f"Found meaningful bug in {pr.repo} PR#{pr.pr_number}: "
                        f"{evaluation['bug_category']} ({evaluation['severity']})"
                    )

        self.logger.info(
            f"Evaluated {prs_evaluated} PRs, {total_evaluated} comments, "
            f"found {len(quality_catches)} meaningful bugs"
        )
        return quality_catches

    def evaluate_prs(
        self,
        results: List[PRWithGreptileComments],
        max_score: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Evaluate PRs and return those with at least one meaningful catch.

        Args:
            results: List of PRs with comments
            max_score: Only evaluate PRs with confidence score <= this value (e.g., 3)

        Returns list of PRs that contain at least one meaningful bug catch.
        """
        quality_prs = []
        total_evaluated = 0
        prs_evaluated = 0

        for pr in results:
            # Get PR-level score from overview comment
            pr_score = self._get_pr_score(pr)

            # Filter by PR score if specified
            if max_score is not None:
                if pr_score is None or pr_score > max_score:
                    self.logger.debug(
                        f"Skipping {pr.repo} PR#{pr.pr_number} - score {pr_score} > {max_score}"
                    )
                    continue

            prs_evaluated += 1
            self.logger.info(
                f"Evaluating {pr.repo} PR#{pr.pr_number} (score: {pr_score}/5)"
            )

            pr_has_meaningful_catch = False
            meaningful_comments = []

            for comment in pr.greptile_comments:
                # Skip noise comments
                if self._is_skipped_comment(comment):
                    self.logger.debug(f"Skipping 'Skipped' comment {comment.comment_id}")
                    continue

                if self._is_review_summary(comment):
                    self.logger.debug(f"Skipping review summary {comment.comment_id}")
                    continue

                total_evaluated += 1
                evaluation = self.evaluate_comment(comment, pr)

                if evaluation["is_meaningful_bug"]:
                    pr_has_meaningful_catch = True
                    meaningful_comments.append(evaluation)
                    self.logger.info(
                        f"Found meaningful bug in {pr.repo} PR#{pr.pr_number}: "
                        f"{evaluation['bug_category']} ({evaluation['severity']})"
                    )

            # Only include PRs with critical or high severity bugs
            high_severity_catches = [
                c for c in meaningful_comments
                if c.get("severity") in ("critical", "high")
            ]

            if high_severity_catches:
                # Generate summary of what Greptile caught
                summary = self.summarize_catches(
                    high_severity_catches,
                    pr.repo,
                    pr.pr_number,
                    pr.pr_title
                )

                quality_prs.append({
                    "repo": pr.repo,
                    "org": pr.org,
                    "pr_number": pr.pr_number,
                    "pr_title": pr.pr_title,
                    "pr_author": pr.pr_author,
                    "pr_url": pr.pr_url,
                    "pr_created_at": pr.pr_created_at.isoformat(),
                    "pr_updated_at": pr.pr_updated_at.isoformat(),
                    "pr_state": pr.pr_state,
                    "pr_score": pr_score,
                    "trigger_type": pr.trigger_type,
                    "meaningful_catches": high_severity_catches,
                    "summary": summary
                })

        self.logger.info(
            f"Evaluated {prs_evaluated} PRs, {total_evaluated} comments, "
            f"found {len(quality_prs)} PRs with meaningful catches"
        )
        return quality_prs

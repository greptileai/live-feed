"""LLM-based evaluation of Greptile comments for meaningful bugs."""

import json
import logging
import os
from typing import Any, Dict, List, Optional

import anthropic

from .models import GreptileComment, PRWithGreptileComments


EVALUATION_PROMPT = """Evaluate if this AI code review comment (by Greptile) is a GREAT CATCH worth showcasing.

YOUR TASK:
1. Read the code diff carefully
2. Read Greptile's comment
3. Verify if Greptile's claim is actually correct by examining the code
4. If a developer replied, treat their feedback as ground truth

GREAT CATCH criteria (must meet ALL):
- Real bug that causes incorrect behavior, crashes, security issues, or data loss
- Non-obvious: a typical reviewer would likely miss it
- Greptile's analysis is CORRECT (you must verify against the code)
- Specific and actionable

REJECT these (when in doubt, reject):
- Style/formatting/naming suggestions
- Vague advice ("consider adding error handling")
- Documentation suggestions
- Refactoring that doesn't fix a bug
- Theoretical concerns without concrete evidence
- Config/build/CI file issues
- Test file feedback
- Obvious issues anyone would catch
- FALSE POSITIVES where Greptile misread the code

DEVELOPER REPLIES:
- If developer says Greptile is WRONG → reject (false positive)
- If developer says "good catch", "fixed", "thanks" → validates the catch
- If NO reply → evaluate based on code analysis alone

---

Repository: {repo}
PR Title: {pr_title}
File: {file_path}
Line: {line_number}

{code_context_section}
Greptile's comment:
```
{comment_body}
```

Greptile's confidence score: {score}/5
{developer_reply_section}
---

Examine the code. Is Greptile's claim correct? Is this a great catch?

Respond with JSON only:
{{
  "is_great_catch": true/false,
  "bug_category": "security|logic|runtime|performance|concurrency|data_integrity|type_error|resource_leak|null",
  "severity": "critical|high|medium|low|null",
  "reasoning": "1-2 sentences: what you verified in the code and why this is/isn't a great catch"
}}"""


BATCH_EVALUATION_PROMPT = """You are evaluating ALL comments from an AI code reviewer (Greptile) on a single PR. Your job is to find the SINGLE BEST catch worth showcasing.

STRICT SEVERITY CRITERIA:
- critical: Security vulnerability (auth bypass, injection, data exposure) OR guaranteed data loss/corruption in production
- high: Bug that WILL cause incorrect behavior affecting users in normal usage (not edge cases)
- medium: Bug in edge cases or error paths that could cause issues under specific conditions
- low: Minor issues, unlikely edge cases, or "nice to fix" items

EVALUATION RULES:
1. DE-DUPLICATE: Multiple comments about the same underlying issue = pick the best-written one
2. VERIFY: Check that Greptile's analysis is actually correct against the code
3. PRIORITIZE: Developer-confirmed catches ("good catch", "fixed") are more valuable
4. BE STRICT: Only return a catch if it's truly impressive and showcase-worthy
5. ONE WINNER: Return only the single best catch, or none if nothing qualifies

REJECT (when in doubt, reject):
- Style/formatting/naming suggestions
- Generic advice ("add error handling", "consider validation")
- Documentation/comment suggestions
- Refactoring that doesn't fix a real bug
- Theoretical concerns without concrete evidence
- Config/build/CI/test file issues
- Obvious issues any developer would catch
- FALSE POSITIVES where Greptile misunderstood the code

---

Repository: {repo}
PR Title: {pr_title}
PR URL: {pr_url}

COMMENTS TO EVALUATE:
{comments_section}

---

Analyze all comments. Find duplicates. Pick the SINGLE BEST catch (if any qualifies).

Respond with JSON only:
{{
  "has_great_catch": true/false,
  "selected_comment_index": <0-based index of best comment, or null if none>,
  "bug_category": "security|logic|runtime|performance|concurrency|data_integrity|type_error|resource_leak|null",
  "severity": "critical|high|medium|low|null",
  "reasoning": "2-3 sentences: why this is the best catch and what makes it showcase-worthy (or why none qualify)",
  "duplicates_found": ["brief description of any duplicate/similar issues that were consolidated"]
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
        # Build code context section - prefer full file patch over diff hunk
        if comment.file_patch:
            code_context_section = f"""Full file diff:
```diff
{comment.file_patch}
```

"""
        elif comment.diff_hunk:
            code_context_section = f"""Code context (diff hunk):
```
{comment.diff_hunk}
```

"""
        else:
            code_context_section = ""

        # Build developer reply section if there's a reply
        if comment.reply_body:
            developer_reply_section = f"""
Developer's reply to Greptile:
```
{comment.reply_body}
```

"""
        else:
            developer_reply_section = ""

        prompt = EVALUATION_PROMPT.format(
            repo=pr.repo,
            pr_title=pr.pr_title,
            file_path=comment.file_path or "N/A",
            line_number=comment.line_number or "N/A",
            code_context_section=code_context_section,
            comment_body=comment.comment_body,
            score=comment.score if comment.score is not None else "N/A",
            developer_reply_section=developer_reply_section
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
                "is_great_catch": False,
                "bug_category": None,
                "severity": None,
                "reasoning": f"Parse error: {str(e)}"
            }
        except Exception as e:
            self.logger.error(f"LLM API error: {e}")
            evaluation = {
                "is_great_catch": False,
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
            "comment_body": comment.comment_body,
            "comment_url": comment.comment_url,
            "reply_body": comment.reply_body,
            "created_at": comment.created_at.isoformat(),
            "is_great_catch": evaluation.get("is_great_catch", False),
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

    def _has_positive_reply(self, reply_body: str) -> bool:
        """Check if a developer reply indicates positive confirmation."""
        if not reply_body:
            return False

        reply_lower = reply_body.lower()

        positive_indicators = [
            "good catch",
            "great catch",
            "nice catch",
            "thanks",
            "thank you",
            "fixed",
            "will fix",
            "you're right",
            "you are right",
            "correct",
            "agreed",
            "makes sense",
            "good point",
            "valid point",
            "addressed",
            "done",
            "updated",
            "resolved",
            "👍",
            "legit",
            "this looks legit",
        ]

        return any(indicator in reply_lower for indicator in positive_indicators)

    def _format_comment_for_batch(self, comment: GreptileComment, index: int) -> str:
        """Format a single comment for batch evaluation."""
        parts = [f"[Comment {index}]"]
        parts.append(f"File: {comment.file_path or 'N/A'}")
        parts.append(f"Line: {comment.line_number or 'N/A'}")

        if comment.file_patch:
            parts.append(f"Diff:\n```diff\n{comment.file_patch[:2000]}\n```")
        elif comment.diff_hunk:
            parts.append(f"Diff:\n```\n{comment.diff_hunk[:1000]}\n```")

        parts.append(f"Greptile's comment:\n```\n{comment.comment_body}\n```")

        if comment.reply_body:
            parts.append(f"Developer reply:\n```\n{comment.reply_body}\n```")

        return "\n".join(parts)

    def evaluate_pr_batch(self, pr: PRWithGreptileComments) -> Optional[Dict[str, Any]]:
        """Evaluate all comments in a PR together, return the single best catch.

        This method:
        1. Sends all comments to LLM together
        2. De-duplicates similar issues
        3. Applies strict severity criteria
        4. Returns only the single best catch (or None)
        """
        # Filter out noise comments
        valid_comments = [
            c for c in pr.greptile_comments
            if not self._is_skipped_comment(c) and not self._is_review_summary(c)
        ]

        if not valid_comments:
            return None

        # Format all comments for batch evaluation
        comments_section = "\n\n---\n\n".join(
            self._format_comment_for_batch(c, i)
            for i, c in enumerate(valid_comments)
        )

        prompt = BATCH_EVALUATION_PROMPT.format(
            repo=pr.repo,
            pr_title=pr.pr_title,
            pr_url=pr.pr_url,
            comments_section=comments_section
        )

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=512,
                messages=[{"role": "user", "content": prompt}]
            )

            response_text = response.content[0].text.strip()
            if response_text.startswith("```"):
                response_text = response_text.split("```")[1]
                if response_text.startswith("json"):
                    response_text = response_text[4:]
                response_text = response_text.strip()

            evaluation = json.loads(response_text)

        except json.JSONDecodeError as e:
            self.logger.warning(f"Failed to parse batch LLM response: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Batch LLM API error: {e}")
            return None

        if not evaluation.get("has_great_catch"):
            self.logger.info(
                f"No great catch in {pr.repo} PR#{pr.pr_number}: {evaluation.get('reasoning', '')}"
            )
            if evaluation.get("duplicates_found"):
                self.logger.debug(f"Duplicates found: {evaluation['duplicates_found']}")
            return None

        # Get the selected comment
        selected_idx = evaluation.get("selected_comment_index")
        if selected_idx is None or selected_idx >= len(valid_comments):
            self.logger.warning(f"Invalid selected_comment_index: {selected_idx}")
            return None

        selected_comment = valid_comments[selected_idx]

        self.logger.info(
            f"Found best catch in {pr.repo} PR#{pr.pr_number}: "
            f"{evaluation['bug_category']} ({evaluation['severity']})"
        )
        if evaluation.get("duplicates_found"):
            self.logger.info(f"Consolidated duplicates: {evaluation['duplicates_found']}")

        return {
            "repo": pr.repo,
            "pr_number": pr.pr_number,
            "pr_title": pr.pr_title,
            "pr_url": pr.pr_url,
            "comment_body": selected_comment.comment_body,
            "comment_url": selected_comment.comment_url,
            "reply_body": selected_comment.reply_body,
            "created_at": selected_comment.created_at.isoformat(),
            "bug_category": evaluation.get("bug_category"),
            "severity": evaluation.get("severity"),
            "llm_reasoning": evaluation.get("reasoning", "")
        }

    def evaluate_comments(
        self,
        results: List[PRWithGreptileComments]
    ) -> List[Dict[str, Any]]:
        """Evaluate all comments and return the best catch per PR.

        Uses batch evaluation to:
        1. De-duplicate similar issues within a PR
        2. Apply strict severity criteria
        3. Return only the single best catch per PR

        Args:
            results: List of PRs with comments

        Returns list of the best catches (max 1 per PR).
        """
        quality_catches = []
        prs_evaluated = 0
        prs_with_catches = 0

        for pr in results:
            prs_evaluated += 1
            comment_count = len([
                c for c in pr.greptile_comments
                if not self._is_skipped_comment(c) and not self._is_review_summary(c)
            ])

            if comment_count == 0:
                self.logger.debug(f"Skipping {pr.repo} PR#{pr.pr_number} - no valid comments")
                continue

            self.logger.info(
                f"Evaluating {pr.repo} PR#{pr.pr_number} ({comment_count} comments, batch mode)"
            )

            # Use batch evaluation to get the single best catch
            best_catch = self.evaluate_pr_batch(pr)

            if best_catch:
                severity = best_catch.get("severity", "").lower()
                reply_body = best_catch.get("reply_body") or ""

                # Low/medium severity requires positive developer confirmation
                if severity in ("low", "medium"):
                    if not self._has_positive_reply(reply_body):
                        self.logger.info(
                            f"Skipping {pr.repo} PR#{pr.pr_number} - {severity} severity "
                            f"without positive developer reply"
                        )
                        continue

                quality_catches.append(best_catch)
                prs_with_catches += 1

        self.logger.info(
            f"Evaluated {prs_evaluated} PRs, "
            f"found {prs_with_catches} PRs with showcase-worthy catches"
        )
        return quality_catches

    def evaluate_prs(
        self,
        results: List[PRWithGreptileComments]
    ) -> List[Dict[str, Any]]:
        """Evaluate PRs and return those with at least one meaningful catch.

        Args:
            results: List of PRs with comments

        Returns list of PRs that contain at least one meaningful bug catch.
        """
        quality_prs = []
        total_evaluated = 0
        prs_evaluated = 0
        prs_skipped_no_score = 0

        for pr in results:
            # Get PR-level score from overview comment
            pr_score = self._get_pr_score(pr)

            # Skip PRs without a confidence score - no overview comment to showcase
            if pr_score is None:
                self.logger.info(
                    f"Skipping {pr.repo} PR#{pr.pr_number} - no confidence score"
                )
                prs_skipped_no_score += 1
                continue

            prs_evaluated += 1
            self.logger.info(
                f"Evaluating {pr.repo} PR#{pr.pr_number} (score: {pr_score}/5)"
            )

            great_catches = []

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

                if evaluation["is_great_catch"]:
                    great_catches.append(evaluation)
                    self.logger.info(
                        f"Found meaningful bug in {pr.repo} PR#{pr.pr_number}: "
                        f"{evaluation['bug_category']} ({evaluation['severity']})"
                    )

            # Include PRs with any meaningful bug (LLM qualified it as meaningful)
            if great_catches:
                # Generate summary of what Greptile caught
                summary = self.summarize_catches(
                    great_catches,
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
                    "great_catches": great_catches,
                    "summary": summary
                })

        self.logger.info(
            f"Evaluated {prs_evaluated} PRs, {total_evaluated} comments, "
            f"found {len(quality_prs)} PRs with meaningful catches "
            f"(skipped {prs_skipped_no_score} PRs without confidence score)"
        )
        return quality_prs

    def evaluate_single_pr_text(
        self,
        repo: str,
        pr_title: str,
        pr_url: str,
        comment_text: str
    ) -> Optional[Dict[str, Any]]:
        """Evaluate a PR using raw comment text (for re-evaluation).

        This is used when re-evaluating PRs with score changes, where we
        fetch all comments and combine them into a single text block.

        Args:
            repo: Repository name (owner/repo)
            pr_title: PR title
            pr_url: PR URL
            comment_text: Combined text of all Greptile comments

        Returns dict with is_great_catch, great_catches, summary, or None on error.
        """
        prompt = f"""You are evaluating whether an AI code reviewer (Greptile) made any GREAT CATCHES in this PR review.

BE SKEPTICAL. Most comments are NOT great catches. The bar should be HIGH.

A GREAT CATCH must meet ALL of these criteria:
1. Is a REAL BUG in application/library code that would cause incorrect behavior, crashes, security issues, or data loss
2. Is non-obvious - a human reviewer would likely miss it without careful analysis
3. Greptile's analysis is CORRECT - verify the logic, don't just trust Greptile's claims
4. Is specific and actionable with a clear fix

NOT great catches (REJECT these - when in doubt, reject):
- Style, formatting, or naming suggestions
- Generic best practice reminders ("consider adding error handling")
- Documentation or comment suggestions
- Refactoring ideas that don't fix actual bugs
- Theoretical concerns that are unlikely in practice
- Issues already handled elsewhere in the code
- False positives where Greptile misunderstood the code
- Build/CI/config file issues (meta.yaml, Dockerfile, package.json, etc.)
- Environment variable or shell script suggestions
- Test file issues (unless it's hiding a real bug in production code)
- Dependency version suggestions
- "Could cause issues" or "may fail" without concrete evidence
- Obvious issues any developer would catch immediately

---

Repository: {repo}
PR Title: {pr_title}

Greptile's comments:
{comment_text}

---

Evaluate: Are there any GREAT CATCHES worth showcasing? Be strict - only truly impressive bug catches should qualify.

Respond with JSON only:
{{
  "is_great_catch": true/false,
  "great_catches": [
    {{
      "bug_category": "security|logic|runtime|performance|concurrency|data_integrity",
      "severity": "critical|high|medium|low",
      "reasoning": "1-2 sentence explanation"
    }}
  ],
  "summary": "1-2 sentence summary of what Greptile caught (empty if no great catches)"
}}"""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=512,
                messages=[{"role": "user", "content": prompt}]
            )

            response_text = response.content[0].text.strip()
            # Handle potential markdown code blocks
            if response_text.startswith("```"):
                response_text = response_text.split("```")[1]
                if response_text.startswith("json"):
                    response_text = response_text[4:]
                response_text = response_text.strip()

            result = json.loads(response_text)
            return result

        except json.JSONDecodeError as e:
            self.logger.warning(f"Failed to parse LLM response: {e}")
            return None
        except Exception as e:
            self.logger.error(f"LLM API error: {e}")
            return None

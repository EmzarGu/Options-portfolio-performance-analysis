from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Tuple


@dataclass(frozen=True)
class IssueClassification:
    category: str
    severity: str
    action: str | None
    actionable: bool


_EXPECTED_WHEEL_AUDIT_PATTERNS = (
    "because no prior put-assignment stock inventory was held.",
    "because all assignment-derived stock inventory was already covering other calls.",
    "because they close a non-wheel call lot.",
    "because no included wheel call lot was open.",
    "because the closed call lot was non-wheel.",
    "assigned-call sold shares",
    "manually sold shares",
)


def classify_backend_issue(message: str) -> IssueClassification:
    text = str(message or "")
    if _is_expected_wheel_audit(text):
        return IssueClassification(
            category="wheel_audit",
            severity="info",
            action=None,
            actionable=False,
        )
    if text.startswith("Prorated ") and " wheel-held shares out of " in text:
        return IssueClassification(
            category="wheel_warning",
            severity="warning",
            action="review_source_data",
            actionable=True,
        )
    if " had no open short to close." in text or text.startswith("Unmatched buy quantity for "):
        return IssueClassification(
            category="missing_basis",
            severity="warning",
            action="review_source_data",
            actionable=True,
        )
    return IssueClassification(
        category="parse",
        severity="warning",
        action="fix_workbook_row",
        actionable=True,
    )


def _is_expected_wheel_audit(message: str) -> bool:
    if message.startswith("Prorated ") and "wheel-held shares out of" in message:
        return True
    if "Excluded " not in message and "Ignored " not in message:
        return False
    return any(pattern in message for pattern in _EXPECTED_WHEEL_AUDIT_PATTERNS)


def split_actionable_and_audit_issues(messages: Iterable[str]) -> Tuple[List[str], List[str]]:
    actionable: List[str] = []
    audit: List[str] = []
    for message in messages or []:
        classification = classify_backend_issue(str(message))
        if classification.actionable:
            actionable.append(str(message))
        else:
            audit.append(str(message))
    return actionable, audit

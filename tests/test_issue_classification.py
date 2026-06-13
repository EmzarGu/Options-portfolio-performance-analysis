from portfolio_backend.issue_classification import classify_backend_issue, split_actionable_and_audit_issues


def test_expected_wheel_exclusions_are_non_actionable_audit_notes():
    messages = [
        "Excluded ABC call execution on 2026-01-20 because no prior put-assignment stock inventory was held.",
        "Ignored 100 assigned-call sold shares of XYZ on 2026-02-20 because no assignment-derived stock inventory was available.",
        "Excluded 10 SPY put spread contracts on 2022-12-28 because the short put was opened with a protective long put.",
    ]

    classifications = [classify_backend_issue(message) for message in messages]

    assert [classification.category for classification in classifications] == ["wheel_audit"] * 3
    assert [classification.severity for classification in classifications] == ["info"] * 3
    assert [classification.actionable for classification in classifications] == [False] * 3
    assert split_actionable_and_audit_issues(messages) == ([], messages)


def test_known_historical_source_quirks_remain_audit_notes():
    messages = [
        "Prorated ASAN call execution on 2022-11-23 to 100 wheel-held shares out of 400 required shares.",
        "Buy ASAN Put 25.0 on 2022-05-20 had no open short to close.",
        "Unmatched buy quantity for ASAN Put 25.0 on 2022-05-20: 1 remaining.",
    ]

    actionable, audit = split_actionable_and_audit_issues(messages)

    assert actionable == []
    assert audit == messages


def test_new_accounting_warnings_remain_actionable():
    messages = [
        "Prorated GOOGL call execution on 2022-10-05 to 100 wheel-held shares out of 200 required shares.",
        "Buy GOOGL Put 125.0 on 2026-01-20 had no open short to close.",
        "Unmatched buy quantity for GOOGL Put 125.0 on 2026-01-20: 1 remaining.",
        "Mixed-leg option row needs review",
    ]

    classifications = [classify_backend_issue(message) for message in messages]
    actionable, audit = split_actionable_and_audit_issues(messages)

    assert [classification.category for classification in classifications] == [
        "wheel_warning",
        "missing_basis",
        "missing_basis",
        "parse",
    ]
    assert [classification.action for classification in classifications] == [
        "review_source_data",
        "review_source_data",
        "review_source_data",
        "fix_workbook_row",
    ]
    assert actionable == messages
    assert audit == []

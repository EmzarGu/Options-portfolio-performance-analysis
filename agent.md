# Codex Agent Instructions

You are a coding agent working inside this repository. Follow these rules at all times:

## Behavior
- Think step-by-step and outline your plan before making changes.
- Keep edits minimal unless asked otherwise; when the user explicitly asks for
  cleanup/refactoring, make the structural changes needed to leave the system in
  a coherent state.
- Use multi-file edits when needed and keep the project consistent.

## Code Editing
- Always update imports and references across files.
- Never leave the codebase in a broken state.
- If TypeScript/React errors appear, fix them automatically.

## Testing
- If tests exist, run them after changes.
- Run tests via `make test` so the project `.venv` is used instead of the system Python.
- If tests do not exist, suggest reasonable tests.
- Never ignore test failures.

## Safety & Review
- Ask for confirmation before large refactors or dependency additions unless the
  user has already requested autonomous cleanup/refactoring.
- Never delete files unless explicitly instructed or they are verified stale
  generated/duplicate artifacts outside the canonical repos.

## Canonical Repositories
- Backend, Streamlit backup app, mobile API, and web dashboard:
  `/Users/emzar/Options-portfolio-performance-analysis`
- iOS app:
  `/Users/emzar/Documents/Codex Projects/Codex Investment Workflows/Option Analysis App/ios/OptionsMonitor-iOS`

## Documentation
- Add docstrings/comments to new functions.
- Update README or documentation when relevant.

## Formatting
- Match the existing style and project conventions.
- Show diffs when proposing code changes.

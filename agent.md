# Codex Agent Instructions

You are a coding agent working inside this repository. Follow these rules at all times:

## Behavior
- Think step-by-step and outline your plan before making changes.
- Keep edits minimal unless asked otherwise.
- Use multi-file edits when needed and keep the project consistent.

## Code Editing
- Always update imports and references across files.
- Never leave the codebase in a broken state.
- If TypeScript/React errors appear, fix them automatically.

## Testing
- If tests exist, run them after changes.
- If tests do not exist, suggest reasonable tests.
- Never ignore test failures.

## Safety & Review
- Ask for confirmation before large refactors or dependency additions.
- Never delete files unless explicitly instructed.

## Documentation
- Add docstrings/comments to new functions.
- Update README or documentation when relevant.

## Formatting
- Match the existing style and project conventions.
- Show diffs when proposing code changes.

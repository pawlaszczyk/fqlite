# PR title

Release 5.1: align NodeQL editor with upstream and execute queries in place

# PR body

## Summary

Update FQLite's JavaFX NodeQL integration to follow the current NodeQL workbench
and let users execute visual queries without switching to the SQL Analyzer.
Prepare the application as version 5.1.

## Changes

- Add a NodeQL graph icon to the main toolbar; remove the separate top-menu entry.
- Rework the dark editor with resizable panels, a category rail, searchable block
  cards, upstream-inspired puzzle geometry, oval reporters and selection outlines.
- Support reporter insertion, detachment, nesting and undo, plus content-aware
  block sizes and shape-aware snapping.
- Add structured conditions, sorting direction, DISTINCT, table aliases,
  LIMIT/OFFSET, and text, alias and SQLite-function reporters.
- Execute queries directly with inline results and an optional expanded result
  view. Keep the SQL Analyzer as an optional hand-off.
- Keep nodes session-only: no NodeQL workspace persistence in project files.
- Bundle Material Icons with its Apache 2.0 license.
- Update README, changelog, integration guide, screenshot and verification notes.
- Align Gradle and application version metadata to 5.1 (2026-10-11).

## Validation

- JDK 21: `./gradlew --no-daemon compileJava processResources nodeQlSmokeTest`.
- 25 JavaFX/SQLite checks: block sizes and alignment, reporter drop targets,
  insertion/detachment, nested undo, SQL generation, quoting, SQLite function
  execution, inline result shape/order and content bounds.
- Smoke test uses only a temporary in-memory database and closes its editor.
- Visual comparison against NodeQL commit
  `bfbb70c5a6e4035041af05809d2f0f28f484aabd` (upstream version 0.4.3+2).

## Scope and release notes

This is a JavaFX integration, not a replacement with the standalone Flutter app.
Native pointer automation, every legacy SQL template and other display densities
have not been exhaustively tested. The upstream/bundled FQLite PDF manual is
unchanged; the integration guide covers the updated NodeQL workflow.

Version metadata is prepared for 5.1. Signed installers and the GitHub release
remain a separate release workflow; this PR does not publish them.

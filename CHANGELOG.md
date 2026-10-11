# FQLite 5.1 — NodeQL integration update

Version date: 2026-10-11. Installer publication is a separate release workflow.

- Add a dedicated NodeQL graph icon to the main toolbar and remove the top-menu entry.
- Align the JavaFX editor with the current NodeQL workbench: dark resizable panels,
  category icons, searchable block cards, colored puzzle blocks and oval reporters.
- Support reporter insertion, detachment, nesting and undo, with content-aware
  block sizes and snapping based on each block's shape.
- Execute queries directly inside NodeQL, with inline results and an optional
  expanded result view. The SQL Analyzer remains an optional hand-off.
- Add structured conditions, sorting direction, SELECT DISTINCT, table aliases,
  LIMIT/OFFSET and text, alias and SQLite-function reporters.
- Keep NodeQL nodes session-only; no NodeQL project save/load persistence.
- Bundle Material Icons with its Apache 2.0 license.
- Update the README, integration guide and visual verification notes; add a
  reproducible JavaFX/SQLite integration smoke test.
- Set the application and Gradle version to 5.1.

## Previous release: 5.0

- Bug fixes for WAL archive and rollback journal analysis.
- Settings: timestamp conversion format customization (issue 22).
- Added support for SQLCipher databases (issue 24).

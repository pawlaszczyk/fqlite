# FQLite 5.1 — NodeQL integration update

Special Thanks to Paul Bodach (Kartoffelspalt) and overcuriousity for the pull-request :-)

- Add a dedicated NodeQL graph icon to the main toolbar and remove the top-menu entry.
- Align the JavaFX editor with the current NodeQL workbench: dark resizable panels,
  category icons, searchable block cards, colored puzzle blocks and oval reporters.
- Support reporter insertion, detachment, nesting and undo, with content-aware
  block sizes and snapping based on each block's shape.
- Execute queries directly inside NodeQL, with inline results and an optional
  expanded result view. The SQL Analyzer remains an optional hand-off.
- Share FQLite's session-wide Light/Dark theme and toolbar switch. Synchronize
  the main app, NodeQL builders, expanded results and errors in both directions;
  reuse the main palette without introducing separate NodeQL preferences.
- Prevent workspace jumps during node dragging: separate canvas panning from
  block drags, remove layout-triggered recentering and keep the zoom center stable.
- Suggest current tables, views and temporary tables in every table input, with
  case-insensitive filtering and SQL identifier quoting. Custom SQL input remains
  fully editable; schema suggestions refresh when the input/list is opened.
- Extend schema suggestions to column/expression inputs, respecting selected
  tables, views, aliases and JOIN ambiguity. Preserve SELECT list prefixes,
  custom expressions and nested reporters; quote qualified identifiers safely.
- Add real checkbox-based column multi-selection with search, preselected inputs,
  retained selections across filtering, explicit Apply/Cancel and single-step undo.
  Keep custom SQL selectable and support SELECT, COLUMN reporters, GROUP BY and
  function argument lists without the single-suggestion menu's 20-item limit.
- Add structured conditions, sorting direction, SELECT DISTINCT, table aliases,
  LIMIT/OFFSET and text, alias and SQLite-function reporters.
- Keep NodeQL nodes session-only; no NodeQL project save/load persistence.
- Bundle Material Icons with its Apache 2.0 license.
- Update the README, integration guide and visual verification notes; add a
  reproducible JavaFX/SQLite integration smoke test.
- Set the application and Gradle version to 5.1.

- Bug fixes for WAL archive and rollback journal analysis (23) due to a bug in overflow page parsing.


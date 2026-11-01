# NodeQL integration: reference and verification

Scope: the existing FQLite NodeQL editor, aligned with the current public NodeQL block presentation and core query workflow. This is a JavaFX integration, not a replacement with the standalone Flutter application. The user's exclusion of project persistence remains in effect.

Reference: [NodeQL](https://github.com/Kartoffelspalt/NodeQL), verified at master commit `bfbb70c5a6e4035041af05809d2f0f28f484aabd` (0.4.3+2). Visual source: [upstream dark-mode preview](https://github.com/Kartoffelspalt/NodeQL/blob/bfbb70c5a6e4035041af05809d2f0f28f484aabd/docs/screenshots/darkmode-preview.png); block geometry and sizing were also checked against `block_shape_painter.dart`, `block_syntax.dart`, `scratch_style.dart` and `workbench_page.dart`. The FQLite integration version is 5.1, separate from the upstream NodeQL version.

Implementation screenshot: [current editor](img/nodeql-current-editor.png). JavaFX scene viewport: 1440 × 860 logical pixels, snapshot 1440 × 860 pixels. Reference gallery asset: 3164 × 1906 pixels, including the native window frame and outer padding. The gallery and implementation were opened together; comparison used the app content and individual blocks rather than treating these different frames/densities as a pixel-for-pixel overlay.

State: dark editor, connected EXECUTE QUERY → SELECT → ORDER BY, a column reporter inserted into SELECT, descending order, film sample database. The implementation additionally shows the three executed result rows. The gallery uses `film_id` and an empty database preview; the current reference code exposes SELECT mode and alias controls that are absent from that older gallery state.

Screenshots refreshed on 2026-10-30 with the `NodeQL for FQLite` heading, table
and column suggestion buttons and the
shared FQLite theme switch: [Dark](img/nodeql-current-editor.png) and
[Light](img/nodeql-light-editor.png). The build and all 199 smoke-test checks
passed during capture. Screenshots use only the test's sample data.

## Comparison and corrections

- Initial P1: SELECT keywords were elided and inputs extended past the painted block. Fixed by measuring styled content, keeping keyword widths and recalculating geometry on scene attachment. The final screenshot shows SELECT, FROM and AS inside the blue boundary; layout assertions pass.
- Initial P2: palette previews did not expand with their cards. Fixed by binding their painted width to the available card width; final previews align within the palette.
- Initial P2: programmatic direction changes were not reflected by the displayed selector. Added synchronization between input values and choices. Final SQL, selector and results all show descending order.
- Final full-view comparison: category rail, searchable block cards, dark central workspace, SQL output and inline results are visible without overlapping editor controls. FQLite-specific toolbar actions and existing categories are intentional integration differences.
- Focused comparison: trigger, statement, expression and ordering blocks were inspected together with the reference. Top notches and bottom tabs join at the body boundary; reporter shapes are oval. Join, set and terminal geometry additionally follows the current reference paths and dimensions.

## Fidelity surfaces

- Typography: block keywords use 15 px bold text, matching the current painter's 15 px / weight 700. JavaFX system text is retained for native FQLite integration; Flutter/JavaFX antialiasing differs.
- Spacing: body heights follow the reference kinds (44 px expressions, 50 px clauses/trigger, 56 px statements, 86/66 px joins, 54 px sets, 48 px terminals); connector depth is 8 px. Width follows input content, and the three panels are resizable.
- Colors: surrounding panels, SQL, results and canvas use FQLite's central light/dark palette. Query/source/filter/join/set/expression/mutation/schema/transaction blocks retain their NodeQL color families, white text and reference yellow selection outline. The app palette around the blocks is an intentional integration difference.
- Assets: official Material Icons font provides the rail symbols and play icon; its Apache 2.0 license is included in resources. Blocks are interactive JavaFX vector controls, corresponding to the original custom-painted UI components.
- Copy: English labels follow the existing FQLite UI. Palette descriptions explain statements and reporter placement; the original integration attribution is retained.

## Functional evidence

### Workspace and table-input regressions

The panel-resize camera regression was run against the unchanged `eeedb33`
baseline and failed with `resizing panels reset horizontal camera`; it passes
with the fix. The revised canvas uses transformed visual bounds for scrolling,
does not recenter on every viewport layout, and disables built-in primary-button
panning in favor of the existing explicit empty-canvas gesture. Drag offsets use
workspace coordinates, including detached nested reporters. Tests exercise actual
JavaFX mouse handlers for repeated detach/reattach at 70/100/140 % zoom and verify
unchanged camera positions, working right-button panning and a stable zoom center.

Schema suggestions are tested in every block containing a table input. They read
tables/views from the active main and temporary SQLite schemas, exclude internal
SQLite objects, refresh on opening/focus, filter case-insensitively while typing,
quote selected identifiers, and preserve custom SQL expressions. Tests cover
spaces/embedded quotes in identifiers, execution of the resulting queries, new
tables, live editor focus, Escape dismissal and popup scene cleanup. Native OS
pointer automation remains outside this smoke test.

Column suggestions use SQLite column metadata and resolve simple table references,
including quoted/schema-qualified names and aliases; arbitrary custom SQL is never
executed just to discover suggestions. Tests execute selected columns containing
spaces/quotes, qualified JOIN columns and wildcards. They also cover ambiguity,
generated columns, temp/view sources, explicit FROM overrides, live filtering,
schema refresh, multi-projection preservation (including commas in functions and
strings), custom expressions and nested reporter selection/undo/detachment.
Complex subqueries remain free-form inputs rather than being parsed for metadata.

The [column multi-picker](img/nodeql-column-multiselect.png) is an actual JavaFX
dialog, not a mockup. Its screenshot shows `id` and `title` checked before Apply.
Checkbox tests cover preselected quoted/unquoted identifiers, retained choice order
and selections during search, wildcard exclusivity, preserved/removable custom SQL,
Cancel without mutation, Apply as one undo action, aliases/JOINs/GROUP BY, embedded
COLUMN reporters, >20 available columns and shared theme/scene lifecycle cleanup.

The JavaFX harness [NodeQlSmokeTest.java](tests/NodeQlSmokeTest.java) passed 199 checks against a temporary in-memory SQLite database. Run it on a graphical desktop with JDK 21 using `./gradlew nodeQlSmokeTest`; it writes screenshots to `build/reports/nodeql/editor.png` and `build/reports/nodeql/editor-light.png`. Coverage includes per-kind heights, chain alignment, reporter drop-target recognition, insertion/detachment, nested reporter undo, WHERE and ORDER BY generation, table alias, DISTINCT, LIMIT/OFFSET, text escaping, nested SQLite function/alias execution, inline result columns/rows/order, and content bounds. Theme checks exercise the actual GUI callback and toggle logic on isolated controls without starting the full main application or reading user settings: initial light-mode inheritance, two-way switching, shared palette/button styling, multiple builders, expanded results, unchanged query/result state and cleanup. Snapshot pixels verify that the trigger and SELECT text still render after repeated switches. Java compilation and resource processing passed without CSS errors; standard JavaFX unnamed-module and SLF4J no-provider warnings remain.

Native pointer automation, every pre-existing SQL template, and other display densities were not exhaustively tested. Plugin management, standalone application settings, workshop mode and standalone project persistence are outside this integration change.

final result: passed

# NodeQL integration: reference and verification

Scope: the existing FQLite NodeQL editor, aligned with the current public NodeQL block presentation and core query workflow. This is a JavaFX integration, not a replacement with the standalone Flutter application. The user's exclusion of project persistence remains in effect.

Reference: [NodeQL](https://github.com/Kartoffelspalt/NodeQL), verified at master commit `bfbb70c5a6e4035041af05809d2f0f28f484aabd` (0.4.3+2). Visual source: [upstream dark-mode preview](https://github.com/Kartoffelspalt/NodeQL/blob/bfbb70c5a6e4035041af05809d2f0f28f484aabd/docs/screenshots/darkmode-preview.png); block geometry and sizing were also checked against `block_shape_painter.dart`, `block_syntax.dart`, `scratch_style.dart` and `workbench_page.dart`. The FQLite integration version is 5.1, separate from the upstream NodeQL version.

Implementation screenshot: [current editor](img/nodeql-current-editor.png). JavaFX scene viewport: 1440 × 860 logical pixels, snapshot 1440 × 860 pixels. Reference gallery asset: 3164 × 1906 pixels, including the native window frame and outer padding. The gallery and implementation were opened together; comparison used the app content and individual blocks rather than treating these different frames/densities as a pixel-for-pixel overlay.

State: dark editor, connected EXECUTE QUERY → SELECT → ORDER BY, a column reporter inserted into SELECT, descending order, film sample database. The implementation additionally shows the three executed result rows. The gallery uses `film_id` and an empty database preview; the current reference code exposes SELECT mode and alias controls that are absent from that older gallery state.

## Comparison and corrections

- Initial P1: SELECT keywords were elided and inputs extended past the painted block. Fixed by measuring styled content, keeping keyword widths and recalculating geometry on scene attachment. The final screenshot shows SELECT, FROM and AS inside the blue boundary; layout assertions pass.
- Initial P2: palette previews did not expand with their cards. Fixed by binding their painted width to the available card width; final previews align within the palette.
- Initial P2: programmatic direction changes were not reflected by the displayed selector. Added synchronization between input values and choices. Final SQL, selector and results all show descending order.
- Final full-view comparison: category rail, searchable block cards, dark central workspace, SQL output and inline results are visible without overlapping editor controls. FQLite-specific toolbar actions and existing categories are intentional integration differences.
- Focused comparison: trigger, statement, expression and ordering blocks were inspected together with the reference. Top notches and bottom tabs join at the body boundary; reporter shapes are oval. Join, set and terminal geometry additionally follows the current reference paths and dimensions.

## Fidelity surfaces

- Typography: block keywords use 15 px bold text, matching the current painter's 15 px / weight 700. JavaFX system text is retained for native FQLite integration; Flutter/JavaFX antialiasing differs.
- Spacing: body heights follow the reference kinds (44 px expressions, 50 px clauses/trigger, 56 px statements, 86/66 px joins, 54 px sets, 48 px terminals); connector depth is 8 px. Width follows input content, and the three panels are resizable.
- Colors: current NodeQL dark background and query/source/filter/join/set/expression/mutation/schema/transaction color families are applied. Selection uses the reference yellow outline.
- Assets: official Material Icons font provides the rail symbols and play icon; its Apache 2.0 license is included in resources. Blocks are interactive JavaFX vector controls, corresponding to the original custom-painted UI components.
- Copy: English labels follow the existing FQLite UI. Palette descriptions explain statements and reporter placement; the original integration attribution is retained.

## Functional evidence

The JavaFX harness [NodeQlSmokeTest.java](tests/NodeQlSmokeTest.java) passed 25 checks against a temporary in-memory SQLite database. Run it on a graphical desktop with JDK 21 using `./gradlew nodeQlSmokeTest`; it writes a new screenshot to `build/reports/nodeql/editor.png`. Coverage includes per-kind heights, chain alignment, reporter drop-target recognition, insertion/detachment, nested reporter undo, WHERE and ORDER BY generation, table alias, DISTINCT, LIMIT/OFFSET, text escaping, nested SQLite function/alias execution, inline result columns/rows/order, and content bounds. Java compilation and resource processing passed. No application exception or CSS parsing error occurred; JavaFX emitted its standard unnamed-module warning for the harness.

Native pointer automation, every pre-existing SQL template, and other display densities were not exhaustively tested. Plugin management, standalone application settings, workshop mode and standalone project persistence are outside this integration change.

final result: passed

package fqlite.base;

import fqlite.ui.FileInfo;
import javafx.application.Platform;
import javafx.scene.Scene;
import javafx.scene.paint.Color;
import javafx.scene.text.Text;
import javafx.scene.text.TextFlow;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Central theme manager for FQLite.
 *
 * All windows that want to participate in the global Light/Dark theme
 * call {@link #register(Scene)} when their Scene is ready, and
 * {@link #unregister(Scene)} when their window closes (optional but good practice).
 *
 * GUI calls {@link #setDark(boolean)} whenever the user clicks the theme toggle.
 * Every registered Scene is updated immediately.
 *
 * Usage from any dialog / window:
 * <pre>
 *   Scene scene = new Scene(root, 800, 600);
 *   ThemeManager.register(scene);
 *   stage.setScene(scene);
 * </pre>
 *
 * The CSS is written to a temporary file once per theme change so that
 * JavaFX's stylesheet URL mechanism can pick it up.
 */
public final class ThemeManager {

    public static List<TextFlow> flows = new ArrayList<>();
    public static List<FileInfo> fileinfos = new ArrayList<>();


    // ── Singleton state ──────────────────────────────────────────────────
    private static boolean dark = true;   // default: dark theme
    private static File    activeStyleFile;

    /** All scenes that should receive theme updates. */
    private static final List<Scene> scenes = new ArrayList<>();

    private ThemeManager() {}

    // ── Public API ───────────────────────────────────────────────────────

    /** Returns {@code true} when the current theme is Dark. */
    public static boolean isDark() { return dark; }

    /**
     * Switch to dark ({@code true}) or light ({@code false}) theme and
     * immediately repaint every registered scene.
     */
    public static void setDark(boolean useDark) {
        dark = useDark;
        applyToAll();
        for (TextFlow tf : flows){
            applyToTextFlow(tf);
        }
        for (FileInfo fi : fileinfos){
            fi.setTheme(isDark());
        }
    }

    /**
     * Register a scene so it receives all future theme updates.
     * Also applies the current theme immediately.
     * Safe to call from any thread.
     */
    public static void register(Scene scene) {
        if (scene == null) return;
        Platform.runLater(() -> {
            if (!scenes.contains(scene)) scenes.add(scene);
            applyTo(scene);
        });
    }

    /**
     * Unregister a scene (call when the owning window closes).
     */
    public static void unregister(Scene scene) {
        if (scene == null) return;
        Platform.runLater(() -> scenes.remove(scene));
    }

    /**
     * Re-apply the current theme to all registered scenes.
     * Called automatically by {@link #setDark}; can also be called manually.
     */
    public static void applyToAll() {
        // Rebuild the CSS file once, then hand it to every scene
        String cssUri = buildCssFile();
        Platform.runLater(() -> {
            for (Scene s : List.copyOf(scenes)) applyTo(s, cssUri);
        });
    }

    // ── Internal helpers ─────────────────────────────────────────────────

    /** Apply the current theme to a single scene (generates CSS if needed). */
    private static void applyTo(Scene scene) {
        applyTo(scene, buildCssFile());
    }

    private static void applyTo(Scene scene, String cssUri) {
        if (scene == null || scene.getRoot() == null) return;

        ThemeColors c = dark ? DARK : LIGHT;

        // Inline JavaFX skin variables (picked up by all controls)
        scene.getRoot().setStyle(
                "-fx-font-family: 'Segoe UI', 'Helvetica Neue', sans-serif;" +
                "-fx-font-size: 13px;" +
                "-fx-base: "                   + c.bgPane()    + ";" +
                "-fx-background: "             + c.bgRoot()    + ";" +
                "-fx-control-inner-background: "+ c.bgControl() + ";" +
                "-fx-text-base-color: "        + c.textMain()  + ";" +
                "-fx-text-background-color: "  + c.textMain()  + ";"
        );

        // Remove old stylesheet URL (if any)
        scene.getStylesheets().removeIf(url ->
                activeStyleFile != null && url.equals(activeStyleFile.toURI().toString()));

        if (cssUri != null) scene.getStylesheets().add(cssUri);

        for (TextFlow tf : flows){
            applyToTextFlow(tf);
        }

        for (FileInfo fi : fileinfos){
            fi.setTheme(isDark());
        }


    }

    /** Writes the CSS to a temp file and returns its URI string. */
    private static String buildCssFile() {
        ThemeColors c = dark ? DARK : LIGHT;
        String css = buildCss(c);
        try {
            if (activeStyleFile == null) {
                activeStyleFile = File.createTempFile("fqlite_theme_", ".css");
                activeStyleFile.deleteOnExit();
            }
            Files.writeString(activeStyleFile.toPath(), css);
            return activeStyleFile.toURI().toString();
        } catch (IOException e) {
            System.err.println("ThemeManager: could not write CSS file – " + e.getMessage());
            return null;
        }
    }

    // ── Colour tokens ────────────────────────────────────────────────────

    private record ThemeColors(
            String bgRoot, String bgPane, String bgControl, String bgToolbar,
            String bgSelected, String bgHover, String divider, String border,
            String textMain, String textMuted, String textTab, String textTabSel,
            String textArea, String textDropHint,
            String toggleBg, String toggleText, String toggleLabel
    ) {}

    static final ThemeColors DARK = new ThemeColors(
            "#1e2330", "#181d2a", "#181d2a", "#1a1f2e",
            "#3a4a7a", "#2d3a5e", "#2d3550", "#2d3550",
            "#c8d0e8", "#7080a8", "#8898c0", "#ffffff",
            "#a8c8a0", "#3a4a7a",
            "#2d3a5e", "#c8d0e8", "☀  Light"
    );

    static final ThemeColors LIGHT = new ThemeColors(
            // bgRoot         bgPane     bgControl  bgToolbar
            "#f5f6fa",      "#ffffff",  "#ffffff",  "#eef0f7",
            // bgSelected     bgHover    divider     border
            "#c8d8f8",      "#e8edf8",  "#c0c8dc",  "#b8c0d4",
            // textMain       textMuted  textTab     textTabSel
            "#18203a",      "#4a5580",  "#2a3560",  "#0a1228",
            // textArea       textDropHint
            "#18203a",      "#8090b8",
            // toggleBg       toggleText  toggleLabel
            "#ffffff",      "#18203a",   "🌙  Dark"
    );

    /** @return the toggle button label for the current theme */
    public static String toggleLabel() {
        return dark ? DARK.toggleLabel() : LIGHT.toggleLabel();
    }

    /** @return the toggle button style string for the current theme */
    public static String toggleBtnStyle() {
        ThemeColors c = dark ? DARK : LIGHT;
        return "-fx-background-color: " + c.toggleBg()   + ";"
               + "-fx-text-fill: "         + c.toggleText() + ";"
               + "-fx-border-color: "      + c.border()     + ";"
               + "-fx-border-width: 1;"
               + "-fx-border-radius: 4;"
               + "-fx-background-radius: 4;"
               + "-fx-font-size: 12px;"
               + "-fx-padding: 4 10 4 10;";
    }

    /**
     * Applies the current theme colour directly to all {@link Text} children of
     * the given {@link TextFlow}.
     *
     * <p>JavaFX does not propagate {@code -fx-fill} from a {@code TextFlow} CSS
     * rule down to its {@code Text} nodes, so CSS alone is not sufficient.
     * Call this method whenever you create or update a {@code TextFlow}, and
     * register it in your scene's theme-change listener so it is re-applied on
     * every theme switch.</p>
     *
     * <p>Example usage:</p>
     * <pre>
     *   ThemeManager.register(scene);
     *   ThemeManager.applyToTextFlow(myTextFlow);
     * </pre>
     *
     * @param textFlow the {@code TextFlow} whose {@code Text} children should be coloured
     */
    public static void applyToTextFlow(TextFlow textFlow) {
        if (textFlow == null) return;
        ThemeColors c = dark ? DARK : LIGHT;
        Color fill = Color.web(c.textMain());
        textFlow.getChildren().stream()
                .filter(node -> node instanceof Text)
                .map(node -> (Text) node)
                .forEach(t -> t.setFill(fill));
    }

    public static void subscribe(TextFlow textFlow) {
        flows.add(textFlow);
    }

    public static void subscribe(FileInfo fileInfo) {
        fileinfos.add(fileInfo);
    }

    // ── CSS builder ──────────────────────────────────────────────────────

    /** Light-theme-only colour constants (not part of ThemeColors record). */
    private static final String L_BTN_BG        = "#f0f4ff"; // very light blue-white for buttons
    private static final String L_BTN_BG_HOVER  = "#dde6f8"; // slightly deeper on hover
    private static final String L_BTN_BG_PRESS  = "#c8d4f0"; // pressed state
    private static final String L_BTN_BORDER    = "#a8b8d8"; // subtle border so buttons stand out
    private static final String L_TBL_GRID      = "#d0d8e8"; // table grid line colour
    private static final String L_TBL_HEADER_BG = "#e8ecf6"; // column header background
    private static final String L_TBL_ALT_ROW   = "#f8f9fd"; // even-row alternating tint
    private static final String L_INPUT_BORDER  = "#a8b4cc"; // text-field / combo border

    private static String buildCss(ThemeColors c) {
        // Pick light-only extras when in light mode, otherwise use dark-neutral values
        boolean lt = !dark;
        String btnBg       = lt ? L_BTN_BG        : c.bgSelected();
        String btnBgHover  = lt ? L_BTN_BG_HOVER  : c.bgHover();
        String btnBgPress  = lt ? L_BTN_BG_PRESS  : c.divider();
        String btnBorder   = lt ? L_BTN_BORDER     : c.border();
        String tblGrid     = lt ? L_TBL_GRID       : c.divider();
        String tblHdrBg    = lt ? L_TBL_HEADER_BG  : c.bgToolbar();
        String tblAltRow   = lt ? L_TBL_ALT_ROW    : c.bgControl();
        String inputBorder = lt ? L_INPUT_BORDER   : c.border();

        return """
            /* ── Root & containers ───────────────────────────────────── */
            .root { -fx-background-color: %s; }
            .split-pane { -fx-background-color: %s; -fx-padding: 0; }
            .split-pane-divider { -fx-background-color: %s; -fx-pref-width: 3px; -fx-pref-height: 3px; }

            /* ── Tree ────────────────────────────────────────────────── */
            .tree-view { -fx-background-color: %s; -fx-border-color: %s; -fx-border-width: 0 1 0 0; }
            .tree-cell { -fx-background-color: transparent; -fx-text-fill: %s; -fx-padding: 3 6; }
            .tree-cell:selected { -fx-background-color: %s; -fx-text-fill: %s; }
            .tree-cell:hover    { -fx-background-color: %s; }

            /* ── Text area ───────────────────────────────────────────── */
            .text-area { -fx-control-inner-background: %s; -fx-text-fill: %s;
                         -fx-border-color: %s; -fx-border-width: 1; }
            .text-area .content { -fx-background-color: %s; }

            /* ── Tabs ────────────────────────────────────────────────── */
            .tab-pane .tab-header-area { -fx-background-color: %s; }
            .tab-pane .tab-header-background { -fx-background-color: %s; }
            .tab { -fx-background-color: %s; -fx-text-fill: %s; }
            .tab:selected { -fx-background-color: %s; -fx-text-fill: %s; }

            /* ── Toolbar & menu ──────────────────────────────────────── */
            .tool-bar { -fx-background-color: %s; -fx-border-color: %s; -fx-border-width: 0 0 1 0; }
            .menu-bar { -fx-background-color: %s; }
            .menu-bar .menu .label { -fx-text-fill: %s; }
            .menu-item .label { -fx-text-fill: %s; }
            .context-menu { -fx-background-color: %s; -fx-border-color: %s; -fx-border-width: 1; }

            /* ── Status bar ──────────────────────────────────────────── */
            .status-bar { -fx-background-color: %s; -fx-border-color: %s; -fx-border-width: 1 0 0 0; }
            .status-bar .label { -fx-text-fill: %s; -fx-font-size: 11px; }

            /* ── Misc labels ─────────────────────────────────────────── */
            .drop-hint { -fx-text-fill: %s; -fx-font-size: 15px; -fx-alignment: center; -fx-text-alignment: center; }
            .hex-dump-title { -fx-text-fill: %s; }

            /* ── List ────────────────────────────────────────────────── */
            .list-view { -fx-background-color: %s; -fx-border-color: %s; -fx-border-width: 1; }
            .list-cell { -fx-background-color: transparent; -fx-text-fill: %s;
                         -fx-padding: 3 6; -fx-font-family: monospace; -fx-font-size: 12px; }
            .list-cell:selected { -fx-background-color: %s; -fx-text-fill: %s; }

            /* ── Scrollbar ───────────────────────────────────────────── */
            .scroll-bar { -fx-background-color: %s; }
            .scroll-bar .thumb { -fx-background-color: %s; -fx-background-radius: 3; }
            .scroll-bar .track { -fx-background-color: %s; }

            /* ── Dialog pane ─────────────────────────────────────────── */
            .dialog-pane { -fx-background-color: %s; }
            .dialog-pane .header-panel { -fx-background-color: %s; }
            .dialog-pane .header-panel .label { -fx-text-fill: %s; }
            .dialog-pane .content { -fx-background-color: %s; }

            /* ── Labels ──────────────────────────────────────────────── */
            .label { -fx-text-fill: %s; }

            /* ── Buttons – light-coloured so blue icons stay visible ─── */
            .button {
                -fx-background-color: %s;
                -fx-text-fill: %s;
                -fx-border-color: %s;
                -fx-border-width: 1;
                -fx-border-radius: 4;
                -fx-background-radius: 4;
            }
            .button:hover   { -fx-background-color: %s; }
            .button:pressed { -fx-background-color: %s; }

            /* ── Check box & radio ───────────────────────────────────── */
            .check-box .text   { -fx-text-fill: %s; }
            .radio-button .text { -fx-text-fill: %s; }

            /* ── Choice box ──────────────────────────────────────────── */
            .choice-box { -fx-background-color: %s; -fx-border-color: %s;
                          -fx-border-width: 1; -fx-border-radius: 4; -fx-background-radius: 4; }
            .choice-box .label { -fx-text-fill: %s; }

            /* ── Spinner ─────────────────────────────────────────────── */
            .spinner { -fx-background-color: %s; -fx-border-color: %s;
                       -fx-border-width: 1; -fx-border-radius: 4; }
            .spinner .text-field { -fx-background-color: %s; -fx-text-fill: %s; }

            /* ── Text field ──────────────────────────────────────────── */
            .text-field { -fx-background-color: %s; -fx-text-fill: %s;
                          -fx-border-color: %s; -fx-border-width: 1; -fx-border-radius: 4; }

            .text-area {
                          -fx-prompt-text-fill: darkgray;
            }
            

            /* ── Combo box ───────────────────────────────────────────── */
            .combo-box { -fx-background-color: %s; -fx-border-color: %s;
                         -fx-border-width: 1; -fx-border-radius: 4; -fx-background-radius: 4; }
            .combo-box .list-cell { -fx-text-fill: %s; -fx-background-color: transparent; }

            /* ── Table – with visible grid lines ─────────────────────── */
            .table-view {
                -fx-background-color: %s;
                -fx-border-color: %s;
                -fx-border-width: 1;
                -fx-table-cell-border-color: %s;
            }
            .table-view .column-header-background { -fx-background-color: %s; }
            .table-view .column-header {
                -fx-background-color: %s;
                -fx-border-color: %s;
                -fx-border-width: 0 1 1 0;
                -fx-size: 30px;
            }
            .table-view .column-header .label { -fx-text-fill: %s; -fx-font-weight: bold; }
            .table-view .filler { -fx-background-color: %s; }
            .table-row-cell {
                -fx-background-color: %s;
                -fx-text-fill: %s;
                -fx-border-color: %s;
                -fx-border-width: 0 0 1 0;
                -fx-cell-size: 26px;
            }
            .table-row-cell:odd  { -fx-background-color: %s; }
            .table-row-cell:selected { -fx-background-color: %s; -fx-text-fill: %s; }
            .table-cell {
                -fx-border-color: %s;
                -fx-border-width: 0 1 0 0;
                -fx-padding: 2 6 2 6;
            }
            .table-cell:selected { -fx-text-fill: %s; }

            /* ── Scroll pane ─────────────────────────────────────────── */
            .scroll-pane { -fx-background-color: %s; }
            .scroll-pane .viewport { -fx-background-color: %s; }

            /* ── TextFlow ────────────────────────────────────────────── */
            .text-flow { -fx-background-color: %s; -fx-border-color: %s;
                         -fx-border-width: 1; -fx-border-radius: 4;
                         -fx-background-radius: 4; -fx-padding: 6 8; }
            .text-flow .text { -fx-fill: %s; }
            """.formatted(
                // [1-3] root, split, divider
                c.bgRoot(), c.bgPane(), c.divider(),
                // [4-9] tree-view, tree-cell, tree-cell:selected, tree-cell:hover
                c.bgRoot(), c.border(), c.textMain(),
                c.bgSelected(), c.textTabSel(), c.bgHover(),
                // [10-13] text-area
                c.bgControl(), c.textArea(), inputBorder, c.bgControl(),
                // [14-19] tabs
                c.bgRoot(), c.bgToolbar(),
                c.bgPane(), c.textTab(), c.bgSelected(), c.textTabSel(),
                // [20-26] toolbar, menu-bar, menu label, menu-item, context-menu border
                c.bgToolbar(), c.border(),
                c.bgToolbar(), c.textMain(), c.textMain(),
                c.bgPane(), c.border(),
                // [27-29] status-bar
                c.bgToolbar(), c.border(), c.textMuted(),
                // [30-31] drop-hint, hex-dump-title
                c.textDropHint(), c.textMuted(),
                // [32-36] list
                c.bgControl(), c.border(), c.textMain(),
                c.bgSelected(), c.textTabSel(),
                // [37-39] scrollbar
                c.bgPane(), c.divider(), c.bgControl(),
                // [40-43] dialog-pane
                c.bgPane(), c.bgToolbar(), c.textMain(), c.bgPane(),
                // [44] label
                c.textMain(),
                // [45-49] button (bg, text, border, hover, pressed)
                btnBg, c.textMain(), btnBorder,
                btnBgHover, btnBgPress,
                // [50-51] check-box, radio-button
                c.textMain(), c.textMain(),
                // [52-54] choice-box (bg, border, label)
                c.bgControl(), inputBorder, c.textMain(),
                // [55-58] spinner (bg, border, textfield-bg, textfield-text)
                c.bgControl(), inputBorder, c.bgControl(), c.textMain(),
                // [59-61] text-field (bg, text, border)
                c.bgControl(), c.textMain(), inputBorder,
                // [62-64] combo-box (bg, border, list-cell text)
                c.bgControl(), inputBorder, c.textMain(),
                // [65-68] table-view (bg, border, cell-border, column-header-background)
                c.bgControl(), c.border(), tblGrid, tblHdrBg,
                // [69-72] column-header (bg, border, label, filler)
                tblHdrBg, c.border(), c.textMain(), tblHdrBg,
                // [73-76] table-row-cell (bg, text, border, odd-row)
                c.bgControl(), c.textMain(), tblGrid, tblAltRow,
                // [77-78] table-row-cell:selected
                c.bgSelected(), c.textTabSel(),
                // [79-80] table-cell (border, selected text)
                tblGrid, c.textTabSel(),
                // [81-82] scroll-pane
                c.bgRoot(), c.bgRoot(),
                // [83-85] text-flow (bg, border, text fill)
                c.bgControl(), inputBorder, c.textMain()
        );
    }



}